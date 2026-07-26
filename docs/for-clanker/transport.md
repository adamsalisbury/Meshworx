# Transports — abstractions, TCP, in-memory

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [protocol.md](protocol.md) · [known-issues.md](known-issues.md)

The transport layer is the swap point. Hub and client depend only on `ITransport` /
`ITransportListener`; three concrete implementations ship (`Tcp*`, `WebSocket*` — PR #78, issue #18 —
`InMemory*`), and you can add your own. A transport is a **dumb, message-oriented pipe** — it owns
framing but knows nothing about opcodes. The TCP and WebSocket pairs are both optionally
**TLS-secured**: pass TLS options to the listener and to `TcpTransport.ConnectAsync` /
`WebSocketTransport.ConnectAsync` and the framing is unchanged, only the byte stream differs
([Turning TLS on, TCP](#turning-tls-on-both-ends); [Turning TLS on, WebSocket](#turning-tls-on-websocket-both-ends)).

Namespace `AdamSalisbury.Meshworx.Transport` (+ `.Tcp`, `.WebSocket`, `.InMemory`).

---

## The contracts

### `ITransport` — `Transport/ITransport.cs:16`

```csharp
Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken = default);
Task<byte[]?> ReceiveAsync(CancellationToken = default);   // null == connection closed
```

**Contract (must honour when implementing):**
- Framing is **your** responsibility. Callers send/receive **complete messages**; each `ReceiveAsync`
  result is exactly one message the sender passed to `SendAsync`.
- **`SendAsync` must be safe to call concurrently** from multiple threads.
- **`ReceiveAsync` is single-reader** — never called concurrently. You may reuse a read buffer.
- `ReceiveAsync` returns **`null`** to signal the connection has closed (clean or mid-frame EOF).
- `IAsyncDisposable`. Disposing should end the peer's `ReceiveAsync`.

### `ITransportListener` — `Transport/ITransportListener.cs:23`

```csharp
Task StartAsync(CancellationToken = default);
Task<ITransport> AcceptAsync(CancellationToken = default);
```

Cancel pending `AcceptAsync` via the token before disposing. Disposing stops the listener.

**Disposal contract — spelled out in the `<remarks>` (`ITransportListener.cs:6-22`) and binding on every
implementation:**

- **An implementation may not rely on the caller cancelling first.** A listener disposed with an accept
  still pending must end that accept with **`ObjectDisposedException`** — not leave the caller waiting,
  and not report a transport-level error.
- **Every accept attempted afterwards throws the same**, whether or not the listener ever started.
- **Disposal is idempotent and safe to call concurrently.** Only the first call tears the listener down,
  and *every* call — first or not — returns only once that teardown is complete.

The exception *type* is load-bearing. `MeshHub.AcceptLoopAsync` uses it to tell **"this listener is
finished"** (break the loop) from **"that one connection failed"** (log and `continue`), and the retry
has no delay (`MeshHub.cs:691-699`) — so a listener that is never coming back but reports anything else
spins the hub's accept loop hot. Both shipped listeners translate accordingly; see
[known-issues.md](known-issues.md) KI-22.

### `IBatchSendTransport` (internal) — `Transport/IBatchSendTransport.cs:14`

```csharp
Task SendAsync(IReadOnlyList<ReadOnlyMemory<byte>> messages, CancellationToken = default);
```

An **optional capability**. The hub's send loop coalesces a burst of queued frames into one underlying
write when the connection's transport implements it (`MeshHub.SendLoopAsync`, `MeshHub.cs:1471-1473`);
transports that don't implement it just receive frames one at a time. It is deliberately **`internal`**:
only the bundled `TcpTransport`/`WebSocketTransport` benefit and only the in-assembly hub consumes it, so
it stays off the public `ITransport` surface. Each element is delivered as its own message. **External
transports cannot and need not implement it.**

`WebSocketTransport` implements it too, but gets a narrower win than `TcpTransport`: WebSocket has no
equivalent of TCP's single-write coalescing, so a batch still costs one WebSocket message per queued
frame — what it saves is acquiring the write lock **once** for the whole batch rather than once per
message, which still matters for a fan-out burst (a broadcast or group send). See
[`WebSocketTransport`](#websockettransport--transportwebsocketwebsockettransportcs23) below.

### `IRemoteEndPointTransport` (public) — `Transport/IRemoteEndPointTransport.cs:16`

```csharp
EndPoint? RemoteEndPoint { get; }
```

Added by PR #68 (issue #16). Another **optional capability**, following the same pattern as
`IBatchSendTransport` above but **public** rather than internal, since a custom network transport
outside this assembly needs to be able to implement it too. `MeshHub.AcceptLoopAsync` uses it,
immediately after `AcceptAsync` and before any handshake, to cap how many connections it admits from a
single remote address at once (`ExtractRemoteAddress`, `MeshHub.cs:743-748`) — see
[hub.md](hub.md#per-remote-endpoint-connection-cap) and [known-issues.md](known-issues.md) KI-29.
- Return `null` if the transport has no meaningful network address (e.g. it isn't network-backed at
  all, or the address isn't known yet). A transport that doesn't implement this interface, or that
  returns `null`, or that returns something other than an `IPEndPoint`, is simply **never capped** by
  the hub's per-remote-endpoint limit — `InMemoryTransport` falls into this bucket and always has.
- The hub only recognises an `IPEndPoint`; it discards any other `EndPoint` subtype the same way as
  `null`.
- **`TcpTransport` and `WebSocketTransport` both implement it** in this codebase (below). For
  `WebSocketTransport` it is `null` on the client side (`ClientWebSocket` exposes no underlying socket to
  report an address from) and the accepted socket's remote address on the listener side. If you write a
  custom TCP-like transport and want it subject to `maxConnectionsPerRemoteEndpoint`, implement this
  interface and report the genuine peer address — do not fabricate one, since the hub uses it as the
  cap's dictionary key.

---

## `TcpTransport` — `Transport/Tcp/TcpTransport.cs:26`

`public sealed class TcpTransport : ITransport, IBatchSendTransport, IRemoteEndPointTransport`.
Length-prefixed framing over a
`Stream` — a `NetworkStream` from a `TcpClient`, an `SslStream` wrapping one when TLS is in use, or an
arbitrary `Stream` via an internal ctor used by loopback tests.

### Framing

Every message: **4-byte big-endian length prefix** (`HeaderSize=4`, `TcpTransport.cs:28`) followed by
the payload. `MaxPayloadSize = 1 MiB` (`:29`). See [protocol.md](protocol.md) for the byte layout.
**TLS does not change the framing** — the identical frames simply travel inside the TLS record layer,
so every send/receive path below behaves the same either way.

### Behaviour

- **`RemoteEndPoint`** (`:70`) — `public EndPoint?`; the `IRemoteEndPointTransport` implementation.
  `null` only for the internal `Stream`-only constructor tests use; every socket-backed instance reports
  `TcpClient.Client.RemoteEndPoint`. See [IRemoteEndPointTransport](#iremoteendpointtransport-public--transportiremoteendpointtransportcs16)
  above.
- **`ConnectAsync(host, port, ct)`** (`:86`) — static factory; sets `NoDelay = true`, connects, returns
  a ready **cleartext** transport. Disposes the socket if connect throws.
- **`ConnectAsync(host, port, SslClientAuthenticationOptions, ct)`** (`:146`) — the TLS factory.
  Connects, wraps the `NetworkStream` in an `SslStream` (`leaveInnerStreamOpen: false`), runs
  `AuthenticateAsClientAsync`, and returns a transport over the authenticated stream. `tlsOptions` is
  **required** and non-null (`ArgumentNullException` otherwise) — use the cleartext overload if you do
  not want TLS. A failed handshake surfaces as `AuthenticationException`; on any throw the `SslStream`
  is disposed **before** the `TcpClient` (`:171-182`) so the partially negotiated session unwinds before
  the socket goes away.
  - **The handshake is bounded only by your `cancellationToken`.** There is no built-in client-side
    handshake timeout (unlike the listener's). Pass a token that expires if a hostile or dead peer must
    not be able to stall the caller indefinitely.
- **`IsEncrypted`** (`:62`) — `public bool`; true only when the underlying stream is an `SslStream` with
  `IsEncrypted` set. Cheap; intended for a start-up/health assertion that a deployment really is
  encrypted.
- **`SendAsync(single)`** (`:230`) — rejects payloads over 1 MiB with `ArgumentException` **before**
  writing (also guards the size addition against overflow). Rents the frame buffer from
  `ArrayPool<byte>.Shared`, writes header+payload, then **writes and flushes under an internal
  `SemaphoreSlim` write lock** — this is what makes concurrent `SendAsync` safe.
- **`SendAsync(batch)`** (`:273`) — frames the whole batch into one rented buffer, one `WriteAsync` +
  one `FlushAsync` under the write lock. Subtlety: if a payload in the batch is oversize, it frames and
  writes the **valid prefix up to** the first oversize frame, **then throws** — preserving the
  single-send "deliver-then-fault" behaviour so coalesced frames ahead of the bad one still go out
  (`:290-347`). Empty batch is a no-op; single-element batch delegates to the scalar path.
- **`ReceiveAsync`** (`:351`) — reads the 4-byte prefix into a **reused** `_headerBuffer` (safe because
  single-reader), then allocates a fresh `byte[payloadLength]` for the body and returns it. A length
  `< 0` or `> 1 MiB` throws `IOException` ("Invalid payload length") — framing is no longer trustworthy,
  so receive loops treat it as a transport failure and close cleanly. Length `0` returns `[]`. A clean
  or mid-frame EOF (`EndOfStreamException` in `ReadExactlyAsync`, `:400`) returns `null`.
- **`DisposeAsync`** (`:386`) — disposes the stream (the `SslStream` when TLS is in use, which closes
  the `NetworkStream` it owns), the `TcpClient` (if owned), and the write lock.

### `CloneClientOptions` (internal) — `:194`

`ConnectAsync` never uses the caller's `SslClientAuthenticationOptions` instance directly; it copies it
first. Two reasons, both load-bearing:

1. A later mutation of the caller's object cannot retroactively change how a live connection was
   authenticated.
2. Defaulting `TargetHost` is not a visible side effect on an object the caller may reuse.

`TargetHost` falls back to the dialled `host` when unset (`:202`) — that is the name the server
certificate is then validated against. The RSA padding switches (`AllowRsaPkcs1Padding`,
`AllowRsaPssPadding`) exist **only on Linux and Windows**; they are copied inside an
`OperatingSystem.IsLinux() || IsWindows()` guard (`:220-224`) because reading them elsewhere throws.

> **If you add a property to this clone, or the framework type gains one, the copy must handle it.** A
> missed property silently discards the caller's intent, which for a security setting means quietly
> weakening the connection. `TlsOptionsCloneTests` enumerates the framework type's settable properties
> by reflection and **fails when one is unhandled** — that test failing is the design working, not a
> flaky test. The listener's `CloneServerOptions` has the identical contract.

The copy is **shallow**. Mutating an object you passed in — the `ClientCertificates` collection, the
`CertificateChainPolicy` — still affects the connection. Treat those as immutable once handed over.

### Gotchas

- **Every received frame is a fresh allocation** (`new byte[payloadLength]`). Delivery is not pooled on
  the read path. The `ReadOnlyMemory<byte>` handed to event handlers is a view over this per-frame array
  — safe to retain today, but copy if you want to be robust against a future pooling change.
- **1 MiB payload cap is enforced on both send and receive.** Oversize send → `ArgumentException` to
  the caller; oversize received length → `IOException` → connection dropped. Keep the two peers'
  `MaxPayloadSize` in agreement if you ever fork the transport.
- **Cleartext is the default.** `ConnectAsync(host, port, ct)` gives no confidentiality, integrity or
  peer authentication. Nothing warns you. See [known-issues.md](known-issues.md) KI-2.
- **A `RemoteCertificateValidationCallback` that always returns `true`** accepts any certificate from
  anyone and reduces TLS to obfuscation — an on-path attacker can then impersonate the hub. Validate or
  pin. `TestCertificates.PinnedTo` (`Transport/Tcp/TestCertificates.cs:59`) is the suite's example of
  doing it properly.
- **Leave `EnabledSslProtocols` at its default** (`SslProtocols.None`) so the platform negotiates its
  best available version rather than a pinned, ageing one. `CertificateRevocationCheckMode` is passed
  through untouched, so revocation is **not** checked unless you ask for it.
- Internal ctors (`TcpTransport(TcpClient)` `:39`, `TcpTransport(Stream)` `:44`,
  `TcpTransport(TcpClient, Stream)` `:49` — the last used by both TLS paths to pair the socket with its
  `SslStream`) are `internal` and reached by the listener and by `InternalsVisibleTo` tests; not part of
  the public API. The `Stream`-only constructor is also the one case where `RemoteEndPoint` returns
  `null` — see [Behaviour](#behaviour) above.

---

## `TcpTransportListener` — `Transport/Tcp/TcpTransportListener.cs:19`

`public sealed class TcpTransportListener : ITransportListener`. It has **two distinct internal modes**
— cleartext and TLS — selected by whether you pass `tlsOptions`. The `ITransportListener` surface is
identical in both; the machinery behind `AcceptAsync` is not.

### State and threading — read before touching any field

**Every mutable field is guarded by one `Lock _stateLock` (`:44`).** `StartAsync`, `AcceptAsync`,
`DisposeAsync` and `LocalEndPoint` each take the state they need under it **once**, then work only from
locals; nothing blocks or awaits while the lock is held. Two fields exist purely to make that work:
`_disposeTask` (`:57`, the elected single teardown, read and written only under the lock) and the
`volatile bool _disposed` (`:61`, set the instant disposal takes ownership, readable by an in-flight
accept without taking the lock).

Re-reading a field mid-operation is precisely the defect this shape replaced: a concurrent dispose could
null `_listener` between `AcceptAsync`'s null-check and its dereference, producing a
`NullReferenceException` instead of a clean `ObjectDisposedException` (issue #11, fixed by PR #63 —
[known-issues.md](known-issues.md) KI-22). **If you add state here, put it under the lock and keep the
capture-once shape.**

### Constructors

Both take the same optional TLS parameters:

```csharp
TcpTransportListener(
    IPEndPoint endPoint,                                  // :110
    SslServerAuthenticationOptions? tlsOptions = null,
    TimeSpan? tlsHandshakeTimeout = null,                 // default 10s  (:21)
    int? maxConcurrentTlsHandshakes = null)               // default 64   (:22)

TcpTransportListener(int port, ...)                       // :174 — delegates to the above
```

> **`(int port)` binds `IPAddress.Loopback`, not `IPAddress.Any`.** A hub created with the
> convenience constructor is reachable **only from the same host** — it will not accept connections
> from other machines, and a "why can't my remote client connect?" bug usually ends here. This is
> deliberate: the hub performs no authentication unless a `ClientAuthenticator` is supplied
> ([hub.md](hub.md#authentication)), so exposure is opt-in. To listen on another interface, pass an
> explicit `IPEndPoint` — `new TcpTransportListener(new IPEndPoint(IPAddress.Any, port))` — and do it
> knowingly, with TLS options unless the segment is already trusted.

**Constructor guards** (all fail fast at construction, not per connection):

| Condition | Throws | Where |
|---|---|---|
| `endPoint` null | `ArgumentNullException` | `:116` |
| `tlsHandshakeTimeout <= TimeSpan.Zero` | `ArgumentOutOfRangeException` | `:118-122` |
| `maxConcurrentTlsHandshakes <= 0` | `ArgumentOutOfRangeException` | `:124-128` |
| `tlsOptions` with no `ServerCertificate`, no `ServerCertificateContext` and no `ServerCertificateSelectionCallback` | `ArgumentException` | `:130-142` |

The certificate guard is deliberate: without one *every* handshake fails, and a listener that silently
accepts nothing is far harder to diagnose at run time than a failed construction.

The options are copied by `CloneServerOptions` (`:418`) under the same contract as the client-side
clone — same reflection test, same shallow-copy caveat, same OS-guarded RSA padding switches
(`:440-444`).

### Lifecycle

- **`StartAsync`** (`:185`) is synchronous under the hood — creates and `Start()`s a `TcpListener`, all
  under `_stateLock`. Throws **`ObjectDisposedException` on a disposed listener** (`:194`; previously it
  would silently bind a fresh socket onto an object mid-teardown, and the teardown would then leave
  behind a running listener that nothing owned), `InvalidOperationException` if already running, and
  honours an already-cancelled token. **The
  socket is bound before the field is published** (`:201-206`), so a failed bind leaves the listener
  startable again rather than permanently claiming to be running. **When TLS is configured it
  additionally** creates a bounded `Channel<TcpTransport>` of capacity `maxConcurrentTlsHandshakes`
  (`SingleReader = true`) and launches the background handshake pump (`:208-223`). The pump
  `Task.Yield()`s first (`:472`), so `StartAsync` stays non-blocking and the lock is never held across
  I/O.
- **`AcceptAsync`** (`:234`) — takes the listener **and** the handshake channel under the lock in one go
  and then works only from those locals (`:242-248`). Throws `ObjectDisposedException` if already
  disposed, `InvalidOperationException` if never started.
  - *TLS mode* (`:250-268`): reads an already-authenticated transport from the channel. It never runs a
    handshake itself. A `ChannelClosedException` (pump stopped — disposed, or accept failed outright) is
    **translated to `ObjectDisposedException`, carrying the original cause as `InnerException`**.
  - *Cleartext mode* (`:270-297`): awaits `AcceptTcpClientAsync`, sets `NoDelay`, wraps in a
    `TcpTransport`. **A failure from an accept that a concurrent disposal stopped is translated to
    `ObjectDisposedException` here too** (`:275-283`) — the exception filter is
    `when (_disposed && IsStoppedListenerFailure(ex))`, and the original is carried as `InnerException`.
    If setting `NoDelay`/getting the stream throws (peer reset immediately after accept), it **disposes
    the socket and rethrows** rather than leaking it — the hub's accept loop then logs and continues.
  - **The translation is not a TLS-mode property.** Both branches now end a disposal-interrupted accept
    the same way, which is what the `ITransportListener` contract requires. Before PR #63 the cleartext
    branch was raw: a listener disposed under a pending accept surfaced a bare
    `SocketException`/`InvalidOperationException`, which `MeshHub.AcceptLoopAsync` logged and retried
    **with no delay** against a listener that was never coming back — a hot spin.
    [known-issues.md](known-issues.md) KI-22.
- **`IsStoppedListenerFailure(Exception)`** (`:403`, `internal static`) — the predicate behind that
  filter. A stopped `TcpListener` reports itself in **three** ways, and which one you get turns on timing
  rather than anything the caller controls: an accept *already pending* when the socket closes gets a
  `SocketException` (operation cancelled) or an `ObjectDisposedException`; an accept issued in the
  instant *after* the stop gets a plain `InvalidOperationException` ("Not listening"), because to the
  `TcpListener` it was simply never started. All three mean the same thing, so the predicate is
  `exception is SocketException or InvalidOperationException` — `ObjectDisposedException` derives from
  the latter and is covered by it. It is `internal` rather than `private` **so a test can assert against
  the framework directly** that what the platform throws is still something this recognises; the third
  case is not reliably reachable through the listener, so nothing else would notice a framework change.
- **`DisposeAsync`** (`:307`) is **not** `async` — it elects a single teardown. Under the lock, the first
  caller sets `_disposed`, hands the four pieces of state to `DisposeCoreAsync` and clears the fields
  **in the same critical section**, storing the resulting `Task` in `_disposeTask`; every other caller,
  later or concurrent, awaits that same task. Disposal is therefore idempotent, safe to call
  concurrently, and **every** call returns only once teardown is complete — the contract
  `ITransportListener` now states. Only the synchronous head of the teardown runs under the lock, and
  `CancelAsync` hands its callbacks to the thread pool, so no cancellation callback can re-enter it.
- **`DisposeCoreAsync`** (`:345`, `private static`) performs the one and only teardown, working entirely
  from the values handed to it so it cannot race another caller over the fields. **The order is unchanged
  and still matters:** cancel the handshake CTS → stop the listener (unblocking the pump's pending
  accept) → **await the pump task** → dispose the CTS → drain the channel, disposing every
  handshaken-but-never-accepted transport so those sockets are not leaked. The pump never faults (it
  completes the channel with the error instead) and it waits for every handshake it started, so awaiting
  it here cannot throw and nothing is still negotiating against the CTS disposed just after.
- `internal EndPoint? LocalEndPoint` (`:63`) exposes the bound endpoint to tests (e.g. for ephemeral
  port 0). It now reads `_listener` under the lock, so it returns `null` once the listener is disposed.

### The TLS handshake pump — read this before touching it

`HandshakePumpAsync` (`:466`) and `HandshakeAsync` (`:541`) exist to keep one slow or hostile peer from
denying the listener to everyone else. The hub's accept loop consumes one connection at a time, so an
inline handshake would be head-of-line blocking on unauthenticated input. Four decisions are deliberate
and each has a counter-intuitive rationale — do not "simplify" any of them:

1. **The accept is never gated on a handshake bound** (`:458-463`). Waiting for a free handshake slot
   before accepting would hand an attacker the whole listener: a few dozen peers that connect and send
   nothing would hold every slot until their timeout, and the loop would stop accepting entirely.
2. **A connection only spends a handshake slot once its peer has actually sent something.**
   `HandshakeAsync` awaits a **zero-byte read** on the `NetworkStream` (`:567`) — which consumes nothing
   and completes only when data arrives — *before* acquiring from `handshakeSlots` (`:569`). A silent
   peer therefore waits out its own timeout without ever occupying part of the CPU budget.
   `maxConcurrentTlsHandshakes` bounds asymmetric crypto on unauthenticated input; it is not an
   admission limit.
3. **Admission is capped by a separate pending bound that is polled, never waited on.**
   `_maxPendingTlsHandshakes = maxConcurrentTlsHandshakes * 16` (`PendingHandshakeMultiplier`, `:28`);
   `pendingSlots.Wait(0, …)` (`:500-504`) sheds the connection immediately when full rather than parking
   the accept loop. Negotiating connections are mostly idle, so this bound caps memory and descriptors,
   not work — and it is sized off the one knob so there are not two.
4. **A transient `SocketException` on accept does not retire the listener** (`:487-496`) — it pauses for
   `AcceptRetryDelay` (50 ms, `:32`) and continues. The cleartext path recovers because the *hub's*
   accept loop logs and continues; the pump has no such caller and has to recover for itself. The pause
   stops a persistent condition (descriptor exhaustion) spinning the loop hot.

Every negotiation is bounded by `tlsHandshakeTimeout` via a linked CTS with `CancelAfter` (`:559-560`).
Outstanding handshakes are awaited in the pump's `finally` (`:537`) so the semaphores are not disposed
while slots are still held.

### Gotchas

- **A failed TLS handshake is completely invisible.** `HandshakeAsync`'s `catch` (`:583-597`) disposes
  the connection and swallows the cause — there is no logger at the transport layer, so an untrusted
  client certificate, a protocol mismatch, a timeout or a cleartext client all look identical to the
  hub: *a connection that never arrived*. Budget for this when debugging "the client cannot connect".
  [known-issues.md](known-issues.md) KI-18.
- **`ObjectDisposedException` from `AcceptAsync` does not always mean somebody disposed the listener.**
  The **TLS** path translates *any* pump stoppage into it, including an accept that failed outright
  (`:256-267`). The **cleartext** path's translation is gated on `_disposed` (`:275`), so there it really
  is disposal. Either way the original cause is the `InnerException` — read it before concluding a clean
  shutdown.
- **Do not widen the cleartext accept's exception filter beyond `IsStoppedListenerFailure`, and do not
  drop the `_disposed` conjunct.** Without the flag, an ordinary transient socket error on a healthy
  listener would be reported as disposal and would stop the hub's accept loop for good — the exact
  inverse of the hot spin the translation exists to prevent.
- **`maxConcurrentTlsHandshakes` is not a connection limit.** Sixteen times that many can be mid-flight,
  and the accept loop itself is unbounded. If you need an admission limit, that is a different control.
- **TLS is off unless you pass options.** No warning is emitted for a cleartext listener.

---

## Turning TLS on (both ends)

Real mutual-TLS setup, lifted from `MeshIntegrationTests.EndToEnd_MessageRoutedBetweenTwoClientsOverMutualTls`
(`MeshIntegrationTests.cs:432`):

```csharp
var listener = new TcpTransportListener(
    new IPEndPoint(IPAddress.Loopback, 0),
    new SslServerAuthenticationOptions
    {
        ServerCertificate = hubCertificate,
        ClientCertificateRequired = true,                                   // mutual TLS
        RemoteCertificateValidationCallback = TestCertificates.PinnedTo(clientCertificate),
    });

await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

var clientTlsOptions = new SslClientAuthenticationOptions
{
    ClientCertificates = [clientCertificate],
    RemoteCertificateValidationCallback = TestCertificates.PinnedTo(hubCertificate),
};

TcpTransport transport = await TcpTransport.ConnectAsync("localhost", port, clientTlsOptions);
Assert.True(transport.IsEncrypted);
await alice.ConnectAsync(transport, "Alice");
```

Notes for anyone wiring this up:

- **Nothing above the transport changes.** `MeshHub`, `MeshClient` and the wire protocol are untouched;
  TLS is entirely a property of the byte stream. This is why the whole feature is contained in two
  files.
- **Transport authentication composes with, but is independent of, the hub's `ClientAuthenticator`.**
  mTLS proves who holds a certificate at the connection level; the `ClientAuthenticator` decides who may
  register under a name ([hub.md](hub.md#authentication)). Neither implies the other.
- **`MeshClientReconnector` needs no change** — its `transportFactory` simply calls the TLS overload, so
  every reconnect renegotiates:
  `async ct => (ITransport)await TcpTransport.ConnectAsync("hub.example.com", 9000, tlsOptions, ct)`.
  Covered by `MeshClientReconnectorTests.StartAsync_TlsTransportFactoryFromDocumentation_ConnectsOverEncryptedTransport`.
  See [client.md](client.md).
- **The listener owns no certificate lifetime.** It holds the `X509Certificate2` you passed (via the
  shallow options copy); disposing that certificate while the listener is running breaks every
  subsequent handshake.

---

## `WebSocketTransport` / `WebSocketTransportListener` — `Transport/WebSocket/`

Added by PR #78 (issue #18). Reaches a hub over `ws://`/`wss://` — the only way to connect from a
browser, and one that traverses proxies and firewalls that block arbitrary TCP ports. **The wire
protocol, `MeshHub` and `MeshClient` are all completely untouched by this transport** — nothing above
`ITransport` changed to support it, exactly as nothing changed for the TCP TLS work. See
[protocol.md](protocol.md#two-layers-framing-vs-message) for how this transport's framing fits the
two-layer model.

### Framing

One WebSocket **binary** message carries exactly one Meshworx frame — **no separate length prefix**,
unlike `TcpTransport`, because the WebSocket protocol already delimits messages
(`WebSocketTransport.cs:13-17`). The 1 MiB payload cap is shared in *value* with `TcpTransport`
(`MaxPayloadSize`, `WebSocketTransport.cs:25`) but is its own constant, checked independently on both
send and receive — there is no shared code path with the TCP transport's cap enforcement, so if you ever
change one you must change the other by hand to keep them in agreement.

### `WebSocketTransport` — `Transport/WebSocket/WebSocketTransport.cs:23`

`public sealed class WebSocketTransport : ITransport, IBatchSendTransport, IRemoteEndPointTransport`.
Wraps a `System.Net.WebSockets.WebSocket`. The constructor is `internal` (`:31`) — reached only via
`ConnectAsync` below (client side) or `WebSocketTransportListener.NegotiateAsync` (server side); there is
no public way to wrap an arbitrary `WebSocket` yourself.

- **`IsEncrypted`** (`:46`) — `public bool`, but derived **differently on each side**, unlike
  `TcpTransport.IsEncrypted` which always introspects the live `SslStream`:
  - **Client side:** set once at connect time from the URI scheme
    (`string.Equals(uri.Scheme, "wss", StringComparison.OrdinalIgnoreCase)`, `:88`) — **not** by
    inspecting the socket, because `ClientWebSocket` exposes nothing equivalent to `SslStream.IsEncrypted`
    to inspect. Reliable in practice, since a `wss://` `ConnectAsync` cannot succeed without completing a
    TLS handshake, but it is an inference from the URI, not a read of the live connection's state.
  - **Server side:** set once at construction from whether the *listener* was configured with
    `tlsOptions` (`isEncrypted: _tlsOptions is not null`, `WebSocketTransportListener.cs:445`) — a
    property of the listener, not of the individual connection. Every connection off a TLS-configured
    listener has already been through `AuthenticateAsServerAsync` by the time this constructor runs, so
    it is accurate, just derived per-listener rather than per-socket.
- **`RemoteEndPoint`** (`:54`) — the `IRemoteEndPointTransport` implementation. `null` for every
  client-side connection (`ConnectAsync` always passes `remoteEndPoint: null`, `:89`); the accepted
  socket's `TcpClient.Client.RemoteEndPoint` for a server-side one, captured once at negotiation
  (`WebSocketTransportListener.cs:445`) before the raw socket is wrapped in a `WebSocket`.
- **`ConnectAsync(uri, configureOptions, ct)`** (`:75-96`) — static factory. Accepts `ws://` or `wss://`;
  `configureOptions` is your hook onto the underlying `ClientWebSocketOptions` — certificate validation
  callback, client certificates for mutual TLS, anything else `ClientWebSocketOptions` exposes. Disposes
  the `ClientWebSocket` if `ConnectAsync`/the callback throws (`:91-95`), mirroring `TcpTransport`'s
  dispose-on-throw-during-connect behaviour.
- **`SendAsync(single)`** (`:99-120`) — rejects payloads over 1 MiB with `ArgumentException` up front
  (matching `TcpTransport`); otherwise takes the internal `SemaphoreSlim` write lock and sends one
  `WebSocketMessageType.Binary` message with `endOfMessage: true`.
- **`SendAsync(batch)`** (`:130-182`, `IBatchSendTransport`) — takes the write lock **once** for the
  whole batch rather than once per message (the concurrency win — see
  [`IBatchSendTransport`](#ibatchsendtransport-internal--transportibatchsendtransportcs14) above for why
  it cannot also coalesce into one wire write the way TCP does). Same deliver-then-fault semantics as
  `TcpTransport`'s batch path: if an element partway through the batch is oversize, every valid element
  ahead of it is still sent before the batch throws `ArgumentException` (`:175-181`). Empty batch is a
  no-op; single-element batch delegates to the scalar path.
- **`ReceiveAsync`** (`:191-236`) — rents an 8 KiB chunk (`ReceiveChunkSize`, `:26`) from
  `ArrayPool<byte>.Shared`. **Fast path:** a message that fits in one WebSocket frame (every message up to
  8 KiB) costs one copy — `chunk.AsSpan(0, count).ToArray()` (`:206`) — matching `TcpTransport`'s single
  exact-size read. **Slow path:** a message spanning several frames accumulates in a `MemoryStream`,
  checking the running total against the 1 MiB cap on every chunk and throwing `IOException` if it would
  be exceeded (`:221-225`) — a second copy on the final `ToArray()`. This receive path is a genuinely
  separate implementation from `TcpTransport.ReceiveAsync`, not shared code, so a change to one's framing
  or cap behaviour does not propagate to the other.
  - **`ReceiveChunkAsync`** (`:243-272`) — the single-frame primitive underneath both paths above.
    Translates `WebSocketException` with `WebSocketError.ConnectionClosedPrematurely` into a synthetic
    `Close` result (`:250-256`), so a peer that drops without completing the WebSocket close handshake is
    treated as a clean EOF, exactly like TCP's mid-frame-EOF-returns-`null` case, rather than propagating
    as an exception from `ReceiveAsync`. A non-`Binary`, non-`Close` frame (i.e. a `Text` frame — Meshworx
    never sends one) throws `IOException` (`:263-269`), the same "framing can no longer be trusted, treat
    it as a transport fault" response `TcpTransport` gives an invalid length prefix.
- **`DisposeAsync`** (`:275-299`) — best-effort graceful close: if the socket is `Open` or
  `CloseReceived`, sends a normal-closure `CloseAsync` bounded by a 2-second internal timeout, swallowing
  `OperationCanceledException`/`WebSocketException` from that attempt (the peer may already be gone).
  Always disposes the `WebSocket` and the write-lock semaphore in `finally`, so the close attempt failing
  never leaks either.

### `WebSocketTransportListener` — `Transport/WebSocket/WebSocketTransportListener.cs:29`

`public sealed class WebSocketTransportListener : ITransportListener`. Built on a `TcpListener` plus an
optional `SslServerAuthenticationOptions`, exactly like `TcpTransportListener`, but negotiation here means
**two** things in sequence rather than one: the TLS handshake where configured, **then** parsing the RFC
6455 HTTP upgrade request by hand (not `HttpListener`/ASP.NET Core). Both happen off the accept path for
the identical reason `TcpTransportListener`'s TLS handshake does — the hub's accept loop consumes one
connection at a time, so negotiating inline would let one slow or hostile peer head-of-line block every
other client waiting to connect.

**The state/threading discipline is identical to `TcpTransportListener`'s** — one `Lock _stateLock`
(`:56`) guards every mutable field, every entry point captures what it needs under the lock **once** and
then works from locals, and nothing that blocks or awaits runs while the lock is held. `internal
LocalEndPoint` (`:65-74`) exists for the same reason as `TcpTransportListener`'s — so tests can read back
an ephemeral (`0`) port.

#### Constructors

```csharp
WebSocketTransportListener(
    IPEndPoint endPoint,                                  // :109
    string path = "/",                                    // the HTTP upgrade path clients must hit
    SslServerAuthenticationOptions? tlsOptions = null,
    TimeSpan? handshakeTimeout = null,                    // default 10s
    int? maxConcurrentHandshakes = null)                  // default 64

WebSocketTransportListener(int port, ...)                 // :168 — binds IPAddress.Loopback, same caveat as TcpTransportListener(int port)
```

Guards mirror `TcpTransportListener`'s exactly: `ArgumentNullException` for a null `endPoint`,
`ArgumentException` for an empty `path` or `tlsOptions` with none of `ServerCertificate` /
`ServerCertificateContext` / `ServerCertificateSelectionCallback`, `ArgumentOutOfRangeException` for a
non-positive `handshakeTimeout`/`maxConcurrentHandshakes` (`:116-140`). `path` is normalised to start with
`/` if you omit it (`:143`) but is otherwise matched **ordinally** against the request line — case
matters. TLS options are copied via the **same** `TcpTransportListener.CloneServerOptions` (`:144`) —
the same shallow-copy caveat and the same `TlsOptionsCloneTests` reflection tripwire cover this listener
too, since it is literally the same clone method.

#### Lifecycle

- **`StartAsync`** (`:180-211`) creates and starts a `TcpListener`, creates a bounded
  `Channel<WebSocketTransport>` of capacity `maxConcurrentHandshakes` (`SingleReader = true`, `:199-204`),
  and launches `NegotiationPumpAsync` — **unconditionally**, not gated on `tlsOptions` the way
  `TcpTransportListener` only launches its pump for TLS. See [Gotchas](#gotchas-2) below for why that
  matters.
- **`AcceptAsync`** (`:217-242`) reads one negotiated transport off the channel. A `ChannelClosedException`
  (pump stopped, for any reason — disposal or an outright accept failure) is translated to
  `ObjectDisposedException` carrying the original as `InnerException` (`:233-241`) — the same
  unconditional translation shape as `TcpTransportListener`'s **TLS-mode** `AcceptAsync`, applied here to
  every connection since there is only the one accept path.
- **`DisposeAsync`** (`:250-277`) / **`DisposeCoreAsync`** (`:279-310`) — the same elected-single-teardown
  shape as `TcpTransportListener`: the first caller takes ownership under the lock, clears the fields and
  stores the resulting `Task`; every caller, first or not, awaits that same task. Order: cancel the
  negotiation `CancellationTokenSource` → stop the `TcpListener` → **await the pump task** → dispose the
  CTS → drain the channel, disposing every negotiated-but-never-accepted transport so those sockets are
  not leaked.

### The negotiation pump — read this before touching it

`NegotiationPumpAsync` (`:317-373`) and `NegotiateAsync` (`:375-482`) are the WebSocket equivalent of
`TcpTransportListener`'s `HandshakePumpAsync`/`HandshakeAsync`, and reuse the **same** hardening shape
almost line for line:

1. **The accept is never gated on a negotiation slot** — a flood of connect-then-idle peers must not be
   able to hold the listener by occupying every slot before they are even asked to negotiate.
2. **A connection only spends a negotiation slot once its peer has sent something.** `NegotiateAsync`
   awaits a **zero-byte read** on the raw `NetworkStream` (`:407`) — completes only once data arrives,
   consumes nothing — **before** acquiring from `negotiationSlots` (`:409`), exactly mirroring
   `TcpTransportListener.HandshakeAsync`'s identical zero-byte-read-before-slot-acquisition (comment at
   `:400-406` says so explicitly). A silent peer waits out its own timeout without ever occupying part of
   the negotiation budget.
3. **A separate, much larger pending bound is polled, never waited on.** `_maxPendingHandshakes =
   maxConcurrentHandshakes * 16` (`PendingHandshakeMultiplier`, `:40`); `pendingSlots.Wait(0, …)`
   (`:344-348`) sheds a connection immediately once full rather than parking the accept loop — the same
   `* 16` multiplier and the same reasoning as `TcpTransportListener.PendingHandshakeMultiplier`.
4. **A transient `SocketException` on accept does not retire the listener** (`:338-342`) — pauses for
   `AcceptRetryDelay` (50 ms, `:44`) and continues, identically to `TcpTransportListener`'s pump.

**One genuine difference, and it is the one to know before touching either listener:**
`TcpTransportListener` only runs this whole pump machinery **when TLS is configured** — a cleartext
`TcpTransportListener` accepts inline in `AcceptAsync` itself, with no background task at all.
`WebSocketTransportListener` runs the pump **always**, TLS or not, because the HTTP upgrade parse has to
happen off the accept path regardless of encryption — a hostile or slow peer that never finishes sending
its upgrade request would head-of-line block the hub's accept loop exactly as an unfinished TLS handshake
would. One consequence: `maxConcurrentHandshakes` bounds concurrent **plain HTTP header parsing** for a
cleartext deployment just as much as it bounds concurrent TLS handshakes for a secured one — it is not a
"TLS-only" knob here the way its TCP namesake effectively is.

Once inside a negotiation slot, `NegotiateAsync` optionally wraps the stream in an `SslStream` and
authenticates (`:413-417`), then reads and validates the HTTP upgrade request
(`ReadUpgradeRequestAsync`, `:420-421`), writes the `101 Switching Protocols` response
(`WriteUpgradeResponseAsync`, `:430`), and only then constructs the `WebSocketTransport` (`:444-445`) —
**unlike** `TcpTransportListener.HandshakeAsync`, which constructs its owning `TcpTransport` immediately
so any later failure can dispose it uniformly. `WebSocketTransport` cannot exist until the WebSocket
object does, which needs the completed HTTP upgrade, so the catch-all instead tracks whichever `Stream`
is currently negotiating (`:390` comment explains this explicitly) and disposes *that* on failure — same
intent as the TCP path, different mechanics forced by the different construction order. Every negotiation
is bounded by `_handshakeTimeout` via a linked CTS (`:395-396`).

### The HTTP upgrade handshake

`ReadUpgradeRequestAsync` (`:496-556`) validates the request line (`GET`, exact path match — ordinal),
`Upgrade: websocket`, a `Connection` header whose comma-separated tokens include `Upgrade`,
`Sec-WebSocket-Version: 13`, and a non-empty `Sec-WebSocket-Key` — returning the key on success or `null`
on **any** failure, including a path mismatch (see [Gotchas](#gotchas-2)). `WriteUpgradeResponseAsync`
computes `Sec-WebSocket-Accept` per RFC 6455 with `SHA1.HashData` (`:620-623`; `#pragma warning disable
CA5350` because the algorithm is a protocol requirement, not a security control here — it only proves the
server read the client's key).

`ReadHeaderLinesAsync` (`:575-613`) reads in **16 KiB-bounded, chunked** reads (`MaxRequestHeaderBytes`,
`:32`) rather than byte-at-a-time — deliberately, since a per-byte read would multiply badly across many
concurrently-negotiating connections, particularly under a reconnect storm. Because the terminating blank
line can be read past within the same chunk that contains it, whatever comes after it in that chunk is
not header data at all — **a peer is not required to wait for the `101` response before sending its first
WebSocket frame**, so those bytes can legitimately be the start of one. `ReadHeaderLinesAsync` returns
them as `leftover` rather than discarding them, and `NegotiateAsync` wraps the negotiated stream in a
private nested `LeftoverPrefixedStream` (`:658-739`) whenever `leftover.Length > 0` (`:436-439`) so
`SystemWebSocket.CreateFromStream` sees those bytes served back before anything further is read from the
underlying socket. *(Inference: no test in this repo specifically drives a client that writes a WebSocket
frame ahead of receiving the `101` response, so this path's correctness rests on code inspection and RFC
6455 conformance rather than a dedicated regression test — a coverage gap, not a known defect.)*

<a id="turning-tls-on-websocket-both-ends"></a>

### Turning TLS on (WebSocket, both ends)

```csharp
// Hub
var listener = new WebSocketTransportListener(
    new IPEndPoint(IPAddress.Any, 22002),
    tlsOptions: new SslServerAuthenticationOptions { ServerCertificate = hubCertificate });
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

// Client
await using WebSocketTransport transport = await WebSocketTransport.ConnectAsync(
    new Uri("wss://hub.example.com:22002/"),
    options => options.RemoteCertificateValidationCallback = MyValidationCallback);
Assert.True(transport.IsEncrypted);
await client.ConnectAsync(transport, "Alice");
```

Cleartext is `ws://` plus no `tlsOptions`; nothing else about the shape changes. As with the TCP pair,
**nothing above the transport changes** — `MeshHub`, `MeshClient` and the wire protocol are untouched, so
the whole feature lives in these two files. `WebSocketMeshIntegrationTests.cs`
(`EndToEnd_RegisterSendBroadcastAndGroupMessage_OverSecureWebSocket`) is the reference end-to-end example,
running registration, direct send, broadcast and a group message over a real `wss://` connection with the
actual `MeshHub`/`MeshClient`.

<a id="gotchas-2"></a>

### Gotchas

- **The negotiation pump always runs, cleartext or not — the opposite of `TcpTransportListener`, whose
  pump exists only for TLS.** Do not port an assumption from the TCP listener that a cleartext accept is
  inline with no background task, or that the handshake-concurrency knob "only matters with TLS" — here
  it bounds plain HTTP header parsing too. See [above](#the-negotiation-pump--read-this-before-touching-it)
  and [known-issues.md](known-issues.md) KI-35.
- **The leftover-bytes handling for a pipelined first frame has no dedicated test.** It looks correct on
  inspection, but nothing in the suite drives a client that writes its first WebSocket frame ahead of the
  `101` response. See [known-issues.md](known-issues.md) KI-37.
- **A wrong upgrade path and a malformed/missing upgrade request are indistinguishable to the caller.**
  `ReadUpgradeRequestAsync` returns `null` for *both* a path mismatch and a malformed request, and
  `NegotiateAsync` always responds `400 Bad Request` for a `null` key (`:422-428`) — there is no 404
  anywhere in the implementation, and the constructor's `path` XML doc now says so correctly (previously
  it wrongly claimed a mismatched path got a `404 Not Found`; fixed before merge — see
  [known-issues.md](known-issues.md) KI-36, now closed). Do not build anything — a proxy rule, a health
  probe — that expects a distinct status for "wrong path" versus "malformed request"; both produce `400`.
- **`IsEncrypted` is an inference on both sides, not a live read of the connection.** Client-side it comes
  from the connect URI's scheme; server-side from whether the *listener* was configured with `tlsOptions`.
  Reliable given how `ClientWebSocket`/`AuthenticateAsServerAsync` behave, but neither is the direct
  `SslStream.IsEncrypted` read `TcpTransport.IsEncrypted` does.
- **No wire-level batch coalescing.** `IBatchSendTransport` still sends one WebSocket message per queued
  frame; only the write-lock acquisition is shared across a batch.
- **`ReceiveChunkSize` (8 KiB) is unrelated to the 1 MiB payload cap** — it only decides how many messages
  take the single-copy fast path in `ReceiveAsync` versus the `MemoryStream` slow path; both are capped
  at `MaxPayloadSize` regardless.
- **The public server-side construction path is only through the listener.** `WebSocketTransport`'s
  constructor and `WebSocketTransportListener.NegotiateAsync` are both `internal`; there is no supported
  way to hand this transport an already-established `WebSocket` from outside the assembly.

---

## `InMemoryTransport` / `InMemoryTransportListener` — `Transport/InMemory/`

In-process transport backed by `System.Threading.Channels`. No sockets, no framing (channels preserve
message boundaries). For hosting hub + clients in one process and for fast, deterministic tests.

### `InMemoryTransport` — `InMemory/InMemoryTransport.cs:14`

- **`CreatePair()`** (`:32`) — returns two connected endpoints wired by two **unbounded** channels; a
  send on one is received on the other.
- `SendAsync` (`:46`) — throws `ObjectDisposedException` if disposed, honours cancellation, then
  **copies** the payload (`data.ToArray()`) and `TryWrite`s it. The copy means callers may reuse their
  buffers; it also means the transport never aliases your memory.
- `ReceiveAsync` (`:55`) — `ReadAsync`; a `ChannelClosedException` (peer disposed) returns `null`.
- `DisposeAsync` (`:69`) — completes the send channel, ending the peer's `ReceiveAsync` (mirrors a
  closed connection). Idempotent via `Interlocked.Exchange`.

### `InMemoryTransportListener` — `InMemory/InMemoryTransportListener.cs:11`

Brought up to the `ITransportListener` disposal contract by PR #63 (issue #11); it did not previously
meet it.

- `Connect()` (`:34`) — **the client-side entry point.** Creates a transport pair, queues the server
  endpoint for `AcceptAsync`, returns the client endpoint. Throws `InvalidOperationException` if not
  started, and again if the listener has been disposed — the write to the completed channel fails
  (`:43-46`).
- `AcceptAsync` (`:52`) — throws `ObjectDisposedException` **first of all** (`:57`), ahead of the
  started guard *and* ahead of the read. Both orderings are deliberate: a disposed listener reports
  itself as disposed whether or not it ever ran, and it must not hand out a connection queued before
  disposal, because completing a channel does not discard what is already buffered. Otherwise it reads
  the next queued server endpoint; a channel closed underneath a pending read also surfaces as
  `ObjectDisposedException` (`:68-71`).
- `DisposeAsync` (`:75`) — **idempotent** via `Interlocked.Exchange` on `_disposed`, and now
  asynchronous: it completes the writer, then **drains the pending-connection channel, disposing each
  queued `InMemoryTransport`** (`:86-89`). Without that drain a disposed listener left the client half
  of every established-but-never-accepted pair parked on a server end nobody would ever read, and those
  transports leaked.
- Usage (README pattern): `var l = new InMemoryTransportListener(); ... await hub.StartAsync(); await
  client.ConnectAsync(l.Connect(), "Alice");` — note you call `l.Connect()` instead of
  `TcpTransport.ConnectAsync`.

### Gotchas

- **Unbounded channels → no back-pressure.** A fast producer with a stalled consumer grows memory
  without bound. Fine for tests and controlled in-process use; do not treat it as production-grade for
  adversarial workloads. [known-issues.md](known-issues.md) KI-7.
- **A disposed listener closes what it never handed out.** `DisposeAsync` disposes every queued-but-
  unaccepted server endpoint, so the matching client endpoint's `ReceiveAsync` returns `null` instead of
  hanging. Do not write a test that expects a connection made via `Connect()` to survive the listener.
- `SendAsync` allocates a copy per message; acceptable for its intended niche.

---

## Implementing a custom transport

1. Implement `ITransport` (and `ITransportListener` if you need the hub to accept it). Own your framing;
   deliver each `SendAsync` payload as exactly one `ReceiveAsync` result.
2. Make `SendAsync` concurrency-safe; keep `ReceiveAsync` single-reader.
3. Return `null` from `ReceiveAsync` on close; dispose should unblock the peer's receive.
4. **If you implement `ITransportListener`, honour its disposal contract in full**
   ([above](#itransportlistener--transportitransportlistenercs23)): a pending accept must end in
   `ObjectDisposedException`, later accepts must throw the same, and `DisposeAsync` must be idempotent,
   concurrency-safe and return only once teardown is complete. Getting the exception type wrong spins
   `MeshHub.AcceptLoopAsync` hot rather than stopping it. Both shipped listeners are worked examples.
5. You **cannot** implement `IBatchSendTransport` (it's internal) — you don't need to; the hub falls
   back to one-frame-at-a-time sends automatically.
6. Test doubles: the suite mocks `ITransport`/`ITransportListener` directly with Moq. See the fixtures
   in [testing.md](testing.md).
