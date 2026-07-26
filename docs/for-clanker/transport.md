# Transports — abstractions, TCP, in-memory

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [protocol.md](protocol.md) · [known-issues.md](known-issues.md)

The transport layer is the swap point. Hub and client depend only on `ITransport` /
`ITransportListener`; two concrete implementations ship (`Tcp*`, `InMemory*`), and you can add your own.
A transport is a **dumb, message-oriented pipe** — it owns framing but knows nothing about opcodes.
The TCP pair is optionally **TLS-secured**: pass TLS options to the listener and to
`TcpTransport.ConnectAsync` and the framing is unchanged, only the byte stream differs
([Turning TLS on](#turning-tls-on-both-ends)).

Namespace `AdamSalisbury.Meshworx.Transport` (+ `.Tcp`, `.InMemory`).

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
has no delay (`MeshHub.cs:604-612`) — so a listener that is never coming back but reports anything else
spins the hub's accept loop hot. Both shipped listeners translate accordingly; see
[known-issues.md](known-issues.md) KI-22.

### `IBatchSendTransport` (internal) — `Transport/IBatchSendTransport.cs:14`

```csharp
Task SendAsync(IReadOnlyList<ReadOnlyMemory<byte>> messages, CancellationToken = default);
```

An **optional capability**. The hub's send loop coalesces a burst of queued frames into one underlying
write when the connection's transport implements it (`MeshHub.SendLoopAsync`, `MeshHub.cs:1277-1279`);
transports that don't implement it just receive frames one at a time. It is deliberately **`internal`**:
only the bundled `TcpTransport` benefits and only the in-assembly hub consumes it, so it stays off the
public `ITransport` surface. Each element is delivered as its own message. **External transports cannot
and need not implement it.**

### `IRemoteEndPointTransport` (public) — `Transport/IRemoteEndPointTransport.cs:16`

```csharp
EndPoint? RemoteEndPoint { get; }
```

Added by PR #68 (issue #16). Another **optional capability**, following the same pattern as
`IBatchSendTransport` above but **public** rather than internal, since a custom network transport
outside this assembly needs to be able to implement it too. `MeshHub.AcceptLoopAsync` uses it,
immediately after `AcceptAsync` and before any handshake, to cap how many connections it admits from a
single remote address at once (`ExtractRemoteAddress`, `MeshHub.cs:656-661`) — see
[hub.md](hub.md#per-remote-endpoint-connection-cap) and [known-issues.md](known-issues.md) KI-29.
- Return `null` if the transport has no meaningful network address (e.g. it isn't network-backed at
  all, or the address isn't known yet). A transport that doesn't implement this interface, or that
  returns `null`, or that returns something other than an `IPEndPoint`, is simply **never capped** by
  the hub's per-remote-endpoint limit — `InMemoryTransport` falls into this bucket and always has.
- The hub only recognises an `IPEndPoint`; it discards any other `EndPoint` subtype the same way as
  `null`.
- **Only `TcpTransport` implements it** in this codebase (below). If you write a custom TCP-like
  transport and want it subject to `maxConnectionsPerRemoteEndpoint`, implement this interface and
  report the genuine peer address — do not fabricate one, since the hub uses it as the cap's dictionary
  key.

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
