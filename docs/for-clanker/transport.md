# Transports — abstractions, TCP, WebSocket, Unix socket, named pipe, QUIC, in-memory

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [protocol.md](protocol.md) · [known-issues.md](known-issues.md)

The transport layer is the swap point. Hub and client depend only on `ITransport` /
`ITransportListener`; six concrete implementations ship (`Tcp*`, `WebSocket*` — PR #78, issue #18 —
`Unix*` and `NamedPipe*` — PR #81, issue #20 — `Quic*` — PR #82, issue #21, **not yet merged to
`main`** — `InMemory*`), and you can add your own. A transport is a **dumb, message-oriented pipe** —
it owns framing but knows nothing about opcodes. `TcpTransport`, `UnixSocketTransport`,
`NamedPipeTransport` and `QuicTransport` all frame identically over a plain `Stream` and now **share
one internal helper**, `StreamFramer`, rather than each reimplementing the length prefix
([Shared framing](#shared-framing-streamframer-internal--transportframingstreamframercs18) below).
TLS is a three-way split across the six: **optional** on TCP and WebSocket (pass TLS options to the
listener and to `TcpTransport.ConnectAsync` / `WebSocketTransport.ConnectAsync` and the framing is
unchanged, only the byte stream differs — [Turning TLS on, TCP](#turning-tls-on-both-ends); [Turning
TLS on, WebSocket](#turning-tls-on-websocket-both-ends)); **mandatory** on QUIC, which requires it at
the protocol level and so has no cleartext mode at all ([Turning TLS on,
QUIC](#turning-tls-on-quic-both-ends)); and **absent** on `UnixSocketTransport`/`NamedPipeTransport`,
which never leave the host and rely on the operating system's own filesystem/ACL access control
instead (see their own sections below).

Namespace `AdamSalisbury.Meshworx.Transport` (+ `.Tcp`, `.WebSocket`, `.Unix`, `.NamedPipes`, `.Quic`,
`.InMemory`, and the internal `.Framing`).

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
spins the hub's accept loop hot. All four socket/pipe-backed listeners (`Tcp`, `WebSocket`, `Unix`,
`NamedPipe`) plus `InMemoryTransportListener` translate accordingly; see
[known-issues.md](known-issues.md) KI-22.

### `IBatchSendTransport` (internal) — `Transport/IBatchSendTransport.cs:14`

```csharp
Task SendAsync(IReadOnlyList<ReadOnlyMemory<byte>> messages, CancellationToken = default);
```

An **optional capability**. The hub's send loop coalesces a burst of queued frames into one underlying
write when the connection's transport implements it (`MeshHub.SendLoopAsync`, `MeshHub.cs:1471-1473`);
transports that don't implement it just receive frames one at a time. It is deliberately **`internal`**:
only the bundled stream-oriented/WebSocket transports benefit and only the in-assembly hub consumes it,
so it stays off the public `ITransport` surface. Each element is delivered as its own message.
**External transports cannot and need not implement it.**

`WebSocketTransport` implements it too, but gets a narrower win than `TcpTransport`: WebSocket has no
equivalent of TCP's single-write coalescing, so a batch still costs one WebSocket message per queued
frame — what it saves is acquiring the write lock **once** for the whole batch rather than once per
message, which still matters for a fan-out burst (a broadcast or group send). See
[`WebSocketTransport`](#websockettransport--transportwebsocketwebsockettransportcs23) below.

`UnixSocketTransport`, `NamedPipeTransport` (PR #81, issue #20) and `QuicTransport` (PR #82, issue #21)
implement it too, and — because all three share `StreamFramer` with `TcpTransport` — get the **same**
single-rented-buffer, one-write-one-flush coalescing `TcpTransport` gets, not the narrower
WebSocket-style win. See their own sections below.

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
- **`TcpTransport`, `WebSocketTransport` and `QuicTransport` all implement it** in this codebase
  (below). For `WebSocketTransport` it is `null` on the client side (`ClientWebSocket` exposes no
  underlying socket to report an address from) and the accepted socket's remote address on the listener
  side. `QuicTransport` (PR #82, issue #21) reports the real `QuicConnection.RemoteEndPoint` on **both**
  sides — QUIC runs over UDP, so a genuine connection endpoint exists as soon as the connection does,
  unlike WebSocket's client-side gap — and so it participates in the hub's per-remote-endpoint cap
  exactly as TCP does; this was a deliberate correctness point confirmed during review, not an
  afterthought, and is the reason it is called out explicitly against the two local-IPC transports
  below. If you write a custom TCP-like transport and want it subject to
  `maxConnectionsPerRemoteEndpoint`, implement this interface and report the genuine peer address — do
  not fabricate one, since the hub uses it as the cap's dictionary key.
- **`UnixSocketTransport` and `NamedPipeTransport` do *not* implement it** (PR #81, issue #20). Both are
  local-only transports with no `IPEndPoint` to report in the first place, so this is not a bug in
  either transport considered alone — but it does mean **`maxConnectionsPerRemoteEndpoint` is silently
  inert for both**: a hub reached only over a Unix domain socket or a named pipe has no cap on
  connections from one source at all, short of `maxClients` itself. See
  [known-issues.md](known-issues.md) KI-38 and the two transports' own sections below. **`QuicTransport`
  is not in this bucket** — see the point above.

---

## Shared framing: `StreamFramer` (internal) — `Transport/Framing/StreamFramer.cs:18`

Added by PR #81 (issue #20), factored out of what was previously `TcpTransport`'s own private framing
code. `internal static class StreamFramer` holds the length-prefixed framing logic **every
stream-oriented transport in this codebase needs identically**: `TcpTransport`, `UnixSocketTransport`,
`NamedPipeTransport` and, since PR #82 (issue #21), `QuicTransport` all wrap a plain `.NET` `Stream` (a
`NetworkStream`/`SslStream`, a `NetworkStream` over a Unix domain socket, a `PipeStream`, or a
`QuicStream`) and all frame the same way, so this is the one place that logic lives rather than four
copies of it. `WebSocketTransport` does **not** use it — a WebSocket message already delimits one
frame, so it needs no length prefix at all (see
[protocol.md](protocol.md#two-layers-framing-vs-message)).

- **`HeaderSize = 4`** (`:23`), **`MaxPayloadSize = 1024 * 1024`** (`:28`) — the 4-byte big-endian
  length prefix and the 1 MiB payload cap, both `internal const`. Every transport that shares this
  helper shares these two numbers; there is now exactly one place to change either.
- **`SendAsync(stream, writeLock, data, ct)`** (`:38-77`) — the single-frame send: rejects an oversize
  payload with `ArgumentException` up front (also guards the frame-size addition against overflow),
  rents the frame buffer from `ArrayPool<byte>.Shared`, writes the header and payload, then writes and
  flushes **under the caller-supplied `writeLock`**. The lock and the reused header buffer are owned by
  the caller — this class is stateless — so each transport controls its own concurrency and allocation
  lifetime exactly as `TcpTransport` did before this helper existed.
- **`SendBatchAsync(stream, writeLock, messages, ct)`** (`:93-166`) — frames a whole batch into one
  rented buffer and issues one `WriteAsync` + one `FlushAsync` under the lock. Deliver-then-fault:
  frames the valid prefix up to the first oversize element, writes it, **then** throws
  `ArgumentException` for the offending element — so frames coalesced ahead of a bad one are still
  delivered. Empty batch is a no-op; single-element batch delegates to `SendAsync`.
- **`ReceiveAsync(stream, headerBuffer, ct)`** (`:179-211`) — reads the 4-byte prefix into the
  **caller-owned, reused** `headerBuffer` (safe only because every consuming transport is
  single-reader, per the `ITransport` contract), then allocates a fresh `byte[payloadLength]` for the
  body. A length `< 0` or `> MaxPayloadSize` throws `IOException` ("Invalid payload length") — the
  framing is no longer trustworthy, so a receive loop treats it as a transport failure. Length `0`
  returns `[]`. A clean or mid-frame EOF (`EndOfStreamException` from the private `ReadExactlyAsync`
  helper, `:213-225`) returns `null`.
- **Contract:** every method is stateless with respect to the class itself — all mutable state (the
  stream, the write lock, the header buffer) is passed in by the caller on every call. This is what lets
  three unrelated transport implementations share the code safely without sharing an instance.

**Verified behaviourally unchanged for `TcpTransport`:** the extraction was a pure refactor — the 48
existing `Transport/Tcp/*Tests.cs` tests pass unmodified against the delegating implementation, and the
byte-for-byte framing (header layout, cap, error types) is identical to what `TcpTransport` implemented
inline before this branch.

---

## `TcpTransport` — `Transport/Tcp/TcpTransport.cs:25`

`public sealed class TcpTransport : ITransport, IBatchSendTransport, IRemoteEndPointTransport`.
Length-prefixed framing over a
`Stream` — a `NetworkStream` from a `TcpClient`, an `SslStream` wrapping one when TLS is in use, or an
arbitrary `Stream` via an internal ctor used by loopback tests.

### Framing

Every message: **4-byte big-endian length prefix** followed by the payload, capped at **1 MiB**. Since
PR #81 (issue #20) this is no longer implemented inline here — `TcpTransport` delegates every send/
receive to the shared
[`StreamFramer`](#shared-framing-streamframer-internal--transportframingstreamframercs18) helper
(`StreamFramer.HeaderSize`/`StreamFramer.MaxPayloadSize`, `Transport/Framing/StreamFramer.cs:23`,
`:28`), which is also what `UnixSocketTransport` and `NamedPipeTransport` now use. See
[protocol.md](protocol.md) for the byte layout. **TLS does not change the framing** — the identical
frames simply travel inside the TLS record layer, so every send/receive path below behaves the same
either way.

### Behaviour

- **`RemoteEndPoint`** (`:66`) — `public EndPoint?`; the `IRemoteEndPointTransport` implementation.
  `null` only for the internal `Stream`-only constructor tests use; every socket-backed instance reports
  `TcpClient.Client.RemoteEndPoint`. See [IRemoteEndPointTransport](#iremoteendpointtransport-public--transportiremoteendpointtransportcs16)
  above.
- **`ConnectAsync(host, port, ct)`** (`:82`) — static factory; sets `NoDelay = true`, connects, returns
  a ready **cleartext** transport. Disposes the socket if connect throws.
- **`ConnectAsync(host, port, SslClientAuthenticationOptions, ct)`** (`:142`) — the TLS factory.
  Connects, wraps the `NetworkStream` in an `SslStream` (`leaveInnerStreamOpen: false`), runs
  `AuthenticateAsClientAsync`, and returns a transport over the authenticated stream. `tlsOptions` is
  **required** and non-null (`ArgumentNullException` otherwise) — use the cleartext overload if you do
  not want TLS. A failed handshake surfaces as `AuthenticationException`; on any throw the `SslStream`
  is disposed **before** the `TcpClient` (`:167-178`) so the partially negotiated session unwinds before
  the socket goes away.
  - **The handshake is bounded only by your `cancellationToken`.** There is no built-in client-side
    handshake timeout (unlike the listener's). Pass a token that expires if a hostile or dead peer must
    not be able to stall the caller indefinitely.
- **`IsEncrypted`** (`:58`) — `public bool`; true only when the underlying stream is an `SslStream` with
  `IsEncrypted` set. Cheap; intended for a start-up/health assertion that a deployment really is
  encrypted.
- **`SendAsync(single)`** (`:226-229`) — a one-line delegation to `StreamFramer.SendAsync(_stream,
  _writeLock, data, cancellationToken)`. Oversize rejection, the `ArrayPool` rental and the write-lock
  discipline all now live in the shared helper — see
  [Shared framing](#shared-framing-streamframer-internal--transportframingstreamframercs18) above for
  exactly what it does; the behaviour is unchanged from before the extraction.
- **`SendAsync(batch)`** (`:237-242`) — likewise delegates to `StreamFramer.SendBatchAsync`. Same
  deliver-then-fault semantics as before: coalesced frames ahead of an oversize element are still sent
  before the batch throws `ArgumentException`. Empty batch is a no-op; single-element batch delegates to
  the scalar path (inside `StreamFramer` now, not here).
- **`ReceiveAsync`** (`:245-250`) — delegates to `StreamFramer.ReceiveAsync(_stream, _headerBuffer,
  cancellationToken)`. `_headerBuffer` (`:33`) is still owned and reused by `TcpTransport` itself — only
  the read logic moved, not the buffer's lifetime — which is what keeps this transport's
  single-reader-safe reuse guarantee intact. A length `< 0` or `> 1 MiB` throws `IOException` ("Invalid
  payload length"); length `0` returns `[]`; a clean or mid-frame EOF returns `null`.
- **`DisposeAsync`** (`:253-258`) — disposes the stream (the `SslStream` when TLS is in use, which
  closes the `NetworkStream` it owns), the `TcpClient` (if owned), and the write lock. Unaffected by the
  framing extraction.

> **If you are reading this transport to learn the framing shape for a new stream-oriented transport,
> read [`StreamFramer`](#shared-framing-streamframer-internal--transportframingstreamframercs18)
> instead** — that is where the logic actually lives now; this section only covers what is still
> genuinely `TcpTransport`'s own (the socket/TLS plumbing, `IsEncrypted`, `RemoteEndPoint`).

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
- Internal ctors (`TcpTransport(TcpClient)` `:35`, `TcpTransport(Stream)` `:40`,
  `TcpTransport(TcpClient, Stream)` `:45` — the last used by both TLS paths to pair the socket with its
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

## `UnixSocketTransport` / `UnixSocketTransportListener` — `Transport/Unix/`

Added by PR #81 (issue #20), **not yet merged to `main`**. Fast, portless, same-host inter-process
communication on Linux and macOS — a sidecar process, or a multi-process desktop/daemon layout, where
opening a loopback TCP port is unnecessary overhead. Framing is the [shared `StreamFramer`
helper](#shared-framing-streamframer-internal--transportframingstreamframercs18) — identical to
`TcpTransport`'s. **There is no TLS option here at all** (unlike TCP/WebSocket): a Unix domain socket
never leaves the host, so the trust boundary is the operating system's filesystem permissions on the
socket path, not a cryptographic one.

### `UnixSocketTransport` — `Transport/Unix/UnixSocketTransport.cs:22`

`public sealed class UnixSocketTransport : ITransport, IBatchSendTransport` — **note, no
`IRemoteEndPointTransport`** (see [Gotchas](#gotchas-3) below).

- **`ConnectAsync(path, ct)`** (`:56-73`) — static factory. Creates a raw `Socket`
  (`AddressFamily.Unix`, `SocketType.Stream`), connects to a `UnixDomainSocketEndPoint(path)`, wraps it
  in a `NetworkStream(socket, ownsSocket: true)`. `ArgumentException` for a null/empty `path`. Disposes
  the socket if the connect throws.
- **`SendAsync(single)`** (`:76-79`), **`SendAsync(batch)`** (`:88-93`), **`ReceiveAsync`** (`:96-99`) —
  each a one-line delegation to `StreamFramer`, exactly like `TcpTransport`'s equivalents. Same 1 MiB
  cap, same deliver-then-fault batch semantics, same `IOException`-on-corrupt-length/`null`-on-EOF
  receive contract.
- **`DisposeAsync`** (`:102-107`) — disposes the stream (which owns and disposes the socket), then the
  write lock.
- Internal ctors: `UnixSocketTransport(Socket)` (`:32-35`, used by the listener's `AcceptAsync`) and
  `UnixSocketTransport(Stream)` (`:37-40`, used by `UnixSocketTransportTests.cs`'s in-memory framing
  tests to drive `StreamFramer`'s error paths against a plain `MemoryStream` without a real socket).

### `UnixSocketTransportListener` — `Transport/Unix/UnixSocketTransportListener.cs:15`

`public sealed class UnixSocketTransportListener : ITransportListener`. Binds a `Socket
(AddressFamily.Unix)` at a filesystem path. **The state/threading discipline is the same shape as
`TcpTransportListener`'s** — one `Lock _stateLock` (`:32`) guards every mutable field, every entry point
captures what it needs under the lock once and works from locals afterwards, and nothing that blocks or
awaits runs while the lock is held.

- **Constructor** (`:59-67`): `UnixSocketTransportListener(string path, bool
  deleteExistingSocketFile = true, UnixFileMode? socketFileMode = null)`. `ArgumentException` for a
  null/empty `path`.
- **`StartAsync`** (`:71-115`) — deletes a pre-existing file at `path` first if
  `deleteExistingSocketFile` is true (`:84-87`, the default — recovers from a previous instance that
  crashed without cleaning up its socket file, which would otherwise fail the bind with "address already
  in use" even though nothing is listening), binds, **hardens the socket file's permissions, then
  calls `Listen()`** (`:94-103` — see [Permission hardening](#permission-hardening) below), and only
  then publishes `_listenSocket`. `InvalidOperationException` if already running; `ObjectDisposedException`
  if disposed.
- **`AcceptAsync`** (`:121-148`) — takes `_listenSocket` under the lock, then awaits `AcceptAsync` on it
  outside the lock. A disposal-interrupted accept is translated to `ObjectDisposedException` via a
  `catch (Exception ex) when (_disposed)` filter (`:137-145`), the same shape
  `TcpTransportListener`'s cleartext path uses. `InvalidOperationException` if never started.
- **`DisposeAsync`** (`:156-175`) — idempotent, guarded entirely under `_stateLock` rather than via an
  elected async teardown task the way `TcpTransportListener`/`WebSocketTransportListener` do (there is
  no background pump here to await, so the simpler shape is sufficient): disposes the listen socket and,
  if `deleteExistingSocketFile` was true, deletes the socket file
  (`TryDeleteSocketFile`, `:177-195` — swallows `IOException`/`UnauthorizedAccessException`, best-effort
  only).

#### Permission hardening

`UnixSocketTransportListener`'s entire access-control model rests on the socket file's filesystem
permissions, so the listener does not leave that to the hosting process's ambient umask:

- **`File.SetUnixFileMode(_path, _socketFileMode)` runs immediately after `Bind` and before `Listen`**
  (`:94-103`) — added during a security-review pass on this PR, after an initial pass found the socket
  file had no explicit permission hardening at all. There is no window in which the file exists with
  only the umask's (commonly far looser) permissions applied.
- **Default is owner read/write only** (`UnixFileMode.UserRead | UnixFileMode.UserWrite`,
  `DefaultSocketFileMode`, `:23`). An optional `socketFileMode` constructor parameter widens this for a
  deployment that genuinely needs another local account to connect (a group-shared sidecar layout, for
  instance).
- **Skipped on Windows** (`if (!OperatingSystem.IsWindows())`, `:98-101`) — Windows' own `AF_UNIX`
  support uses NTFS ACLs rather than POSIX mode bits, so `File.SetUnixFileMode` is neither meaningful nor
  supported there. There is no equivalent hardening applied on Windows for this transport; if you need
  Unix-domain-socket IPC with an explicit access-control default on Windows, this is a gap, not something
  handled elsewhere.

<a id="gotchas-3"></a>

### Gotchas

- **`UnixSocketTransport` does not implement `IRemoteEndPointTransport`, so it is never subject to
  `maxConnectionsPerRemoteEndpoint`.** A single local peer with filesystem access to the socket path can
  open connections up to the hub's full `maxClients` budget, not a per-source ceiling. This was a
  deliberate scope decision for this PR (issue #20's design says "new transport only; hub/client
  untouched") — **do not treat it as fixed**, and do not extend `MeshHub.cs` to work around it without
  reading the discussion first. See [known-issues.md](known-issues.md) KI-38 for the full reasoning.
- **The `deleteExistingSocketFile` constructor parameter is dual-purpose, and only its "before bind"
  half is exercised by name in most callers' minds.** The same flag also controls whether `DisposeAsync`
  deletes the socket file on clean shutdown (`:167-170`) — pass `false` for "don't clean up a stale file
  before binding" and you also silently opt out of your *own* instance's file being deleted on
  disposal. Documented correctly on the constructor's own XML doc, but easy to miss; see
  [known-issues.md](known-issues.md) KI-39.
- **No TLS option, by design.** Encrypting traffic that never leaves the host adds nothing; the socket
  file's permissions are the entire access boundary. Do not add a `tlsOptions` parameter here on the
  reasoning that "the TCP/WebSocket pair both have one" — it would be dead weight.
- **The socket path has a platform length limit** (`sun_path`, typically 108 bytes on Linux). The test
  suite's `TempSocketPath` helper (`Transport/Unix/TempSocketPath.cs`) generates a short
  `{Guid:N}.sock` name under the temp directory for exactly this reason — a long, descriptive path can
  fail to bind with an unhelpful error.
- **Constructing a listener does not create the socket file — `StartAsync` does**, and only
  `StartAsync` running to completion applies the permission hardening. A listener that is constructed
  but never started leaves no file behind at all.

### Usage

```csharp
// Hub (Linux/macOS)
var listener = new UnixSocketTransportListener("/tmp/meshworx.sock");
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

// Client (Linux/macOS)
await using var client = new MeshClient(clientLogger);
await client.ConnectAsync(await UnixSocketTransport.ConnectAsync("/tmp/meshworx.sock"), "Alice");
```

---

## `NamedPipeTransport` / `NamedPipeTransportListener` — `Transport/NamedPipes/`

Added by PR #81 (issue #20), **not yet merged to `main`**. The Windows equivalent of
`UnixSocketTransport` — same-host inter-process communication with no open port — for the platform that
has no `AF_UNIX`-over-a-path convention Meshworx relies on elsewhere.  **Windows-only**: every entry
point throws `PlatformNotSupportedException` on any other operating system, checked **before** any
platform-specific API is touched. Framing is the same shared
[`StreamFramer`](#shared-framing-streamframer-internal--transportframingstreamframercs18) helper as
`TcpTransport` and `UnixSocketTransport`. No TLS option, for the same reason as the Unix socket
transport: traffic never leaves the host.

### `NamedPipeTransport` — `Transport/NamedPipes/NamedPipeTransport.cs:23`

`public sealed class NamedPipeTransport : ITransport, IBatchSendTransport` — **also no
`IRemoteEndPointTransport`**, same gap as `UnixSocketTransport`; see
[known-issues.md](known-issues.md) KI-38.

- **`ConnectAsync(pipeName, serverName = ".", ct)`** (`:49-75`) — static factory. `ArgumentException`
  for a null/empty `pipeName`; **`PlatformNotSupportedException` if `!OperatingSystem.IsWindows()`**
  (`:56-61`), checked before a `NamedPipeClientStream` is even constructed. Otherwise creates one
  (`PipeDirection.InOut`, `PipeOptions.Asynchronous`), connects, and disposes it if the connect throws.
- **`SendAsync(single)`** (`:78-81`), **`SendAsync(batch)`** (`:90-95`), **`ReceiveAsync`** (`:98-101`)
  — one-line delegations to `StreamFramer` over the underlying `PipeStream`, identical in shape and
  behaviour to `UnixSocketTransport`'s.
- **`DisposeAsync`** (`:104-108`) — disposes the `PipeStream`, then the write lock.

### `NamedPipeTransportListener` — `Transport/NamedPipes/NamedPipeTransportListener.cs:18`

`public sealed class NamedPipeTransportListener : ITransportListener`. Unlike the socket-based
listeners, there is no single long-lived listen handle here — the Win32 named-pipe API models "one
waiting connection slot" as **one `NamedPipeServerStream` instance**, so a fresh instance is created for
every `AcceptAsync` call rather than one instance being reused.

- **Constructor** (`:53-67`): `NamedPipeTransportListener(string pipeName, int? maxServerInstances =
  null, PipeSecurity? pipeSecurity = null)`. `ArgumentException` for a null/empty `pipeName`;
  `ArgumentOutOfRangeException` for a non-positive `maxServerInstances`. `maxServerInstances` defaults to
  `NamedPipeServerStream.MaxAllowedServerInstances`.
- **`StartAsync`** (`:72-96`) — **`PlatformNotSupportedException` if `!OperatingSystem.IsWindows()`**
  (`:85-90`), checked under the lock **before** `_acceptCts` is ever assigned — this ordering is what
  makes the CA1416 suppressions below sound (see [Windows-only API
  suppressions](#windows-only-api-suppressions)). Otherwise just creates the shared
  `CancellationTokenSource` that every `AcceptAsync` links against; there is no socket/handle to bind
  yet.
- **`AcceptAsync`** (`:107-157`) — creates a **new** `NamedPipeServerStream` via
  `NamedPipeServerStreamAcl.Create` (see [Permission hardening](#permission-hardening-1) below) and
  awaits `WaitForConnectionAsync` on it. A cancellation caused by disposal is translated to
  `ObjectDisposedException` (`:140-149`, disposing the half-created stream first); any other failure
  also disposes the stream before rethrowing (`:150-154`). `InvalidOperationException` if never started.
- **`DisposeAsync`** (`:165-194`) — the same elected-single-teardown shape as
  `TcpTransportListener`/`WebSocketTransportListener`: the first caller cancels the shared
  `CancellationTokenSource` and disposes it (`DisposeCoreAsync`, `:187-194`); every caller, first or not,
  awaits the same stored `Task`.

#### Permission hardening

Same underlying concern as `UnixSocketTransportListener`'s socket-file mode, different mechanism:

- **`NamedPipeServerStreamAcl.Create`** (`:125-134`), not the plain `NamedPipeServerStream` constructor
  — the ACL-aware factory `System.IO.Pipes.AccessControl` provides in place of the
  `PipeSecurity`-accepting constructor .NET Framework used to have.
- **Default is a `PipeSecurity` granting `PipeAccessRights.FullControl` to the current user only**
  (`CreateCurrentUserOnlyPipeSecurity`, `:196-219`) — added during the same security-review pass as the
  Unix socket file hardening above, because **Windows' own default for an unspecified `PipeSecurity` is
  considerably broader**: it also grants read access to the `Everyone` group and the anonymous account,
  alongside full control to `LocalSystem`, administrators and the creator owner. Left unset, that
  platform default would silently defeat the pipe-name access-control model this transport's whole
  security posture rests on.
  - **Cross-check against the code:** `CreateCurrentUserOnlyPipeSecurity` reads
    `WindowsIdentity.GetCurrent().User`, throwing `InvalidOperationException` if it cannot be resolved,
    and grants that one identity `FullControl`. It grants nothing to any group.
- An optional `pipeSecurity` constructor parameter overrides the default for a deployment that
  genuinely needs a different or wider set of principals.

<a id="windows-only-api-suppressions"></a>

**Windows-only API suppressions (`#pragma warning disable CA1416`):** `NamedPipeServerStreamAcl.Create`
in `AcceptAsync` (`:125-135`) and `CreateCurrentUserOnlyPipeSecurity` itself (`:207-219`, also marked
`[SupportedOSPlatform("windows")]`) are both Windows-only APIs the analyser would otherwise flag. Both
suppressions carry a comment pointing at the actual runtime guard: `StartAsync` throws
`PlatformNotSupportedException` on every non-Windows platform **before** `_acceptCts` is ever set
(`:85-92`), and `AcceptAsync` requires a non-null `_acceptCts` (`:115`) — so by construction, neither
Windows-only call can be reached from a non-Windows process. **Verified in this pass:** the ordering
inside `StartAsync`'s lock really does check the platform before assigning `_acceptCts` in every code
path, so the suppression's stated justification holds; this is not a case of a suppression comment
promising a guard that isn't actually there.

### Gotchas

- **Windows-only, and the two guard points are in different files.** `NamedPipeTransport.ConnectAsync`
  and `NamedPipeTransportListener.StartAsync` each independently check
  `OperatingSystem.IsWindows()` and throw `PlatformNotSupportedException` — there is no shared guard
  helper. If you add a third entry point (a hypothetical reconnect helper, say), it needs its own check;
  nothing enforces this for you.
- **No `IRemoteEndPointTransport`, same as `UnixSocketTransport`** — never capped by
  `maxConnectionsPerRemoteEndpoint`. See [known-issues.md](known-issues.md) KI-38.
- **The happy path (`AcceptAsync` actually accepting, a real round-trip) cannot run on this repo's CI at
  all.** `.github/workflows/ci.yml` runs `ubuntu-latest` only, so every named-pipe test that would
  exercise real pipe I/O is a documented no-op there — see [testing.md](testing.md) for exactly which
  tests fall into this bucket and why that is an accepted, not accidental, gap.
- **`maxServerInstances` bounds the operating system's own pipe-instance limit, not a Meshworx-level
  admission control** — it is a `NamedPipeServerStream` construction parameter, unrelated to
  `MeshHub.MaxClients` or `maxConnectionsPerRemoteEndpoint` (which, per the point above, cannot see this
  transport at all).
- **No TLS option, by design** — same reasoning as `UnixSocketTransport`: traffic never leaves the host,
  so the pipe's ACL is the entire access boundary.

### Usage

```csharp
// Hub (Windows)
var listener = new NamedPipeTransportListener("meshworx");
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

// Client (Windows)
await using var client = new MeshClient(clientLogger);
await client.ConnectAsync(await NamedPipeTransport.ConnectAsync("meshworx"), "Alice");
```

---

## `QuicTransport` / `QuicTransportListener` — `Transport/Quic/`

Added by PR #82 (issue #21), **not yet merged to `main`**. Reaches a hub over QUIC
(`System.Net.Quic`) using a single bidirectional `QuicStream` per connection — TLS 1.3 and faster
connection setup versus TCP, and (unlike TCP) resistance to head-of-line blocking at the transport
level, though Meshworx does not exploit multi-stream multiplexing today (see
[Gotchas](#gotchas-4) below). Framing is the [shared `StreamFramer`
helper](#shared-framing-streamframer-internal--transportframingstreamframercs18) — identical to
`TcpTransport`'s. **Unlike every other transport in this codebase, TLS is not optional here**: QUIC
mandates it at the protocol level, so both `QuicTransport.ConnectAsync` and the
`QuicTransportListener` constructor take TLS options as a required parameter rather than a nullable
one. **Requires `QuicListener.IsSupported`/`QuicConnection.IsSupported` to be `true`** — typically
meaning the native `msquic` library is installed (`apt install libmsquic` on Debian/Ubuntu) and the
platform's TLS stack supports TLS 1.3; both entry points throw `PlatformNotSupportedException` with
that guidance in the message if it is not. CI installs `libmsquic` explicitly in a dedicated step
(`.github/workflows/ci.yml`) rather than assuming the runner image already has it, precisely so the
`IsSupported` checks never silently turn the whole QUIC test suite into a no-op without anyone
noticing.

### `QuicTransport` — `Transport/Quic/QuicTransport.cs:33`

`public sealed class QuicTransport : ITransport, IBatchSendTransport, IRemoteEndPointTransport`.

- **`DefaultApplicationProtocol`** (`:43`, `internal static readonly`) — the ALPN protocol name
  (`"meshworx"`) both `ConnectAsync` and `QuicTransportListener` advertise when the caller's TLS
  options leave `ApplicationProtocols` unset. QUIC mandates ALPN negotiation as part of the TLS 1.3
  handshake, so — unlike TCP, where TLS itself is optional and ALPN doubly so — the two ends must agree
  on at least one protocol name or the handshake fails outright. If you set `ApplicationProtocols`
  yourself on either end, set it on **both**, matching.
- **`RemoteEndPoint`** (`:83-91`) — the `IRemoteEndPointTransport` implementation; reads
  `QuicConnection.RemoteEndPoint` directly. QUIC runs over UDP, so a real address is always available
  once the connection exists — this is what makes `QuicTransport` subject to `MeshHub`'s
  per-remote-endpoint cap on both the client and the listener side, unlike `WebSocketTransport` (`null`
  client-side) and unlike `UnixSocketTransport`/`NamedPipeTransport` (no `IRemoteEndPointTransport` at
  all). See [IRemoteEndPointTransport](#iremoteendpointtransport-public--transportiremoteendpointtransportcs16)
  above.
- **`ConnectAsync(host, port, tlsOptions, ct)`** (`:126-183`) — static factory, the **only** public way
  to construct one. `ArgumentException` for a null/empty `host`; `ArgumentNullException` for a null
  `tlsOptions` — there is no cleartext overload the way `TcpTransport` has one, because QUIC has no
  cleartext mode. `PlatformNotSupportedException` if `QuicConnection.IsSupported` is `false`. Clones
  `tlsOptions` via the same `TcpTransport.CloneClientOptions(tlsOptions, host)` the TLS `TcpTransport`
  factory uses (`:143` — same `TargetHost`-defaulting, same reflection-tested completeness guarantee,
  same shallow-copy caveat as [`TcpTransport`'s clone](#cloneclientoptions-internal--194)), then defaults
  `ApplicationProtocols` to `[DefaultApplicationProtocol]` if the clone left it empty (`:144-147`).
  Connects the `QuicConnection`, then opens **one** `QuicStreamType.Bidirectional` stream on it
  (`:164-166`) — that stream *is* the `ITransport`; nothing about Meshworx's framing or opcodes touches
  the connection object beyond that one stream. On any throw during either step, disposes whichever of
  the stream/connection was actually created, in that order, before rethrowing (`:169-182`) — the same
  dispose-on-throw-during-connect shape `TcpTransport.ConnectAsync` and `WebSocketTransport.ConnectAsync`
  both follow.
  - **The handshake and stream-open are both bounded only by your `cancellationToken`** — there is no
    built-in timeout on the client side, exactly like `TcpTransport`'s TLS `ConnectAsync`. Pass a token
    that expires if a hostile or dead peer must not be able to stall the caller indefinitely.
- **`SendAsync(single)`** (`:186-189`), **`SendAsync(batch)`** (`:198-203`, `IBatchSendTransport`),
  **`ReceiveAsync`** (`:206-209`) — one-line delegations to `StreamFramer`, identical in shape and
  behaviour to `TcpTransport`'s and `UnixSocketTransport`'s equivalents: same 1 MiB cap, same
  deliver-then-fault batch semantics, same `IOException`-on-corrupt-length/`null`-on-EOF receive
  contract.
- **`DisposeAsync`** (`:212-227`) — disposes the `QuicStream` first, then the `QuicConnection` if one
  exists (it does not for the internal `Stream`-only test constructor, see below), then the write-lock
  semaphore.
- Internal ctors: `QuicTransport(QuicConnection, QuicStream)` (`:53-56`, used by
  `ConnectAsync` above and by the listener's negotiation path) and `QuicTransport(Stream)` (`:64-67`,
  used by `QuicTransportTests.cs`'s in-memory framing tests to drive `StreamFramer`'s error paths
  against a plain `MemoryStream` — a real `QuicStream` cannot be constructed without a genuine
  connection). The `Stream`-only constructor is also the one case where `RemoteEndPoint` returns `null`
  (`_connection` is `null`), mirroring `TcpTransport`'s equivalent gap.

<a id="gotchas-4"></a>

### Gotchas

- **A QUIC stream is invisible to the peer until data actually arrives on it.** Opening a stream
  (`OpenOutboundStreamAsync`) is a purely local operation — QUIC does not notify the other end a stream
  exists until data (or a FIN) is sent on it. Concretely: `QuicTransportListener.AcceptAsync` **cannot
  complete** until the connecting `QuicTransport`'s `SendAsync` has been called at least once. This is
  the opposite test shape from every other transport in this codebase, where the listener's `AcceptAsync`
  completes as soon as the connection/handshake finishes and *then* the first message is sent — see
  [testing.md](testing.md) for how the test suite handles this. It is never an issue in normal Meshworx
  use, since `MeshClient.ConnectAsync` sends the registration frame immediately once handed a transport
  (confirmed against `MeshClient.cs` directly, not assumed) — but anyone driving `QuicTransport`/
  `QuicTransportListener` directly must call `SendAsync` before waiting on `AcceptAsync`, not after, or
  the two ends deadlock waiting on each other. Documented on `ConnectAsync`'s own XML remarks
  (`QuicTransport.cs:112-120`).
- **Meshworx uses exactly one stream per connection, not QUIC's multiplexing.** `ITransport` models one
  channel per client, so `QuicTransport` opens a single `QuicStreamType.Bidirectional` stream and never
  more — the several-concurrent-streams capability a QUIC connection can offer is unused. Don't assume a
  second stream exists to reach for; it doesn't, and adding one would be a design change to `ITransport`
  itself, not a `QuicTransport`-local tweak.
- **No cleartext mode, ever.** There is no `QuicTransport.ConnectAsync` overload that omits TLS options,
  unlike `TcpTransport`/`WebSocketTransport`. If you want an unauthenticated/self-signed connection for
  local testing, pass `SslClientAuthenticationOptions` with a validation callback that returns `true` (or
  better, `TestCertificates.PinnedTo`) rather than looking for a cleartext path — there isn't one.
- **`RemoteEndPoint`, `DisposeAsync`'s connection branch, and several call sites inside the listener
  carry `#pragma warning disable CA1416`** (Windows/Linux/macOS-only API). Every one of them is reachable
  only after a successful `QuicConnection.ConnectAsync`/`QuicListener.AcceptConnectionAsync`, both of
  which already require `IsSupported` to be `true` — the same "verified, not merely asserted" pattern
  the named-pipe listener's suppressions use (see [its own
  section](#namedpipetransportlistener--transportnamedpipesnamedpipetransportlistenercs18)).

### `QuicTransportListener` — `Transport/Quic/QuicTransportListener.cs:33`

`public sealed class QuicTransportListener : ITransportListener`. **The state/threading discipline is
the same shape as `TcpTransportListener`'s** — one `Lock _stateLock` (`:56`) guards every mutable
field, and every entry point captures what it needs under the lock once before working from locals —
**with one genuine structural difference from every other listener in this codebase**: `StartAsync`'s
bind step, `QuicListener.ListenAsync`, is itself asynchronous. Every other listener here (`Tcp`,
`WebSocket`, `Unix`, `NamedPipe`) binds synchronously, so its whole bind can run **inside** the lock,
which is what makes a concurrent `DisposeAsync` trivially safe to reason about for them — there is no
window in which the bind is in flight and the lock is not held. `QuicTransportListener` cannot do that:
it awaits `QuicListener.ListenAsync` **outside** the lock (`:262`), then re-takes the lock afterwards
and checks `_disposed` again before publishing state (`:265-284`) — the pattern the constructor's and
`StartAsync`'s own doc comments describe as "a concurrent `DisposeAsync` is handled by rechecking the
`_disposed` flag under lock once `ListenAsync`'s await completes" (`:239-243`). See
[Gotchas](#gotchas-5) below for what this shape does **not** cover.

#### Constructors

```csharp
QuicTransportListener(
    IPEndPoint endPoint,                                          // :130
    SslServerAuthenticationOptions tlsOptions,                    // required — QUIC mandates TLS
    TimeSpan? streamOpenTimeout = null,                           // default 10s   (:35)
    int? maxConcurrentNegotiations = null,                        // default 64    (:36)
    int? maxConcurrentNegotiationsPerSource = null)                // default maxConcurrentNegotiations / 8, min 1

QuicTransportListener(int port, ...)                              // :197 — binds IPAddress.Loopback, same caveat as the other `(int port)` constructors
```

**Constructor guards** (`:136-166`): `ArgumentNullException` for a null `endPoint` or `tlsOptions`;
`ArgumentOutOfRangeException` for a non-positive `streamOpenTimeout`, `maxConcurrentNegotiations` or
`maxConcurrentNegotiationsPerSource`; `ArgumentException` if `tlsOptions` supplies none of
`ServerCertificate`, `ServerCertificateContext` or `ServerCertificateSelectionCallback` — the identical
certificate guard `TcpTransportListener`/`WebSocketTransportListener` apply, necessarily unconditional
here since there is no cleartext mode to fall back to. `tlsOptions` is copied via the same
`TcpTransportListener.CloneServerOptions` (`:169`) every other listener's TLS options go through, and
`ApplicationProtocols` defaults to `[QuicTransport.DefaultApplicationProtocol]` if the (cloned) options
left it empty (`:170-173`).

#### Lifecycle

- **`StartAsync`** (`:217-291`) — checks `_disposed`/"already running" under the lock **before** the
  async bind (`:221-229`, see [Gotchas](#gotchas-5) for what this does and does not guarantee), then
  `QuicListener.IsSupported` (`:231-237`, `PlatformNotSupportedException` with install guidance if
  false), builds `QuicListenerOptions` with a `ConnectionOptionsCallback` that hands every accepted
  connection the same cloned `_tlsOptions` (`:245-260`), and awaits `QuicListener.ListenAsync` — the
  actual bind (`:262`). Once that completes, re-takes the lock, and if still not disposed creates the
  bounded `Channel<QuicTransport>` of capacity `maxConcurrentNegotiations` (`SingleReader = true`,
  `:269-274`) and a fresh negotiation `CancellationTokenSource`, publishes all four fields together, and
  launches `NegotiationPumpAsync` (`:265-283`). If disposed in the meantime, disposes the just-bound
  `QuicListener` (nothing else owns it) and throws `ObjectDisposedException` (`:286-290`).
- **`AcceptAsync`** (`:297-319`) — reads one negotiated `QuicTransport` off the channel under the same
  shape every other listener uses; a `ChannelClosedException` (pump stopped, for any reason) is
  translated to `ObjectDisposedException` carrying the original as `InnerException` (`:309-318`).
- **`DisposeAsync`** (`:326-353`) / **`DisposeCoreAsync`** (`:355-391`) — the same elected-single-teardown
  shape as `TcpTransportListener`'s and `WebSocketTransportListener`'s: the first caller takes ownership
  under the lock, clears the fields and stores the resulting `Task`; every caller, first or not, awaits
  that same task. Order: cancel the negotiation `CancellationTokenSource` → dispose the `QuicListener` →
  **await the negotiation pump task** → dispose the CTS → drain the channel, disposing every
  negotiated-but-never-accepted `QuicTransport` so those connections are not leaked.

### The negotiation pump — two-tier admission, read this before touching it

`NegotiationPumpAsync` (`:421-516`) and `NegotiateAsync` (`:518-565`) exist for the same reason every
other listener's pump does — the hub's accept loop consumes one connection at a time, so waiting for a
slow or hostile peer inline would head-of-line block every other client. The QUIC handshake itself
(TLS 1.3, msquic's own amplification-limiting/retry handling) completes **inside**
`QuicListener.AcceptConnectionAsync` before it ever returns a connection, so — unlike
`TcpTransportListener`'s TLS handshake pump or `WebSocketTransportListener`'s negotiation pump — **there
is no separate handshake step to run off the accept path and no CPU-expensive work left to bound**. What
this pump waits for instead is each accepted connection's **first stream** (`AcceptInboundStreamAsync`),
because a connection that never opens one is otherwise indistinguishable from one that eventually will.

**This is the one respect in which QUIC is structurally harder to defend than TCP/WebSocket:** those two
listeners gate a connection's admission into their negotiation pool on a **cheap pre-check** — a
zero-byte socket read that completes only once the peer has sent *something*, consuming no CPU and no
slot — before it ever occupies a handshake/negotiation slot (see
[`TcpTransportListener`'s pump](#the-tls-handshake-pump--read-this-before-touching-it) and
[`WebSocketTransportListener`'s pump](#the-negotiation-pump--read-this-before-touching-it) above). QUIC
has no equivalent: by the time `AcceptConnectionAsync` returns a `QuicConnection` at all, msquic has
already completed the full TLS 1.3 handshake internally, so there is nothing cheaper left to check
before waiting for the first stream. **This is why the admission design here is two separate,
independently-motivated caps layered in front of each other, not one**, and the shipped shape is the
*final* state after a design history worth knowing before you touch it:

1. **A global semaphore, `maxConcurrentNegotiations` (default 64)** (`:428`) — bounds how many
   connections may be concurrently waiting for their first stream at all. A connection that finds it
   full is **shed immediately** rather than queued (`negotiationSlots.Wait(0, …)`, `:481-490`).
2. **A per-source cap, `maxConcurrentNegotiationsPerSource` (default one eighth of
   `maxConcurrentNegotiations`, minimum 1 — 8 at the defaults)** (`:429`, `TryAdmitSource`/
   `ReleaseSource`/`NormaliseForSourceCap`, `:572-643`) — checked **first**, ahead of the global
   semaphore (`:475-479`), against a `ConcurrentDictionary<IPAddress, int>` keyed on the connection's
   source address, with an IPv6 source masked to its `/64` network prefix first — the identical
   normalisation and the identical reasoning as `MeshHub`'s own per-remote-endpoint cap (a single host
   is routinely handed a whole `/64`, so keying on the full address would let one attacker defeat the
   cap by rotating within it). Duplicated here rather than shared across the assembly boundary between
   the transport layer and `MeshHub`.

**Why both exist, in that order:** the global semaphore alone has no cheap pre-check to protect it —
without the per-source cap, a single source completing genuine (not spoofed) QUIC handshakes and never
opening a stream on any of them could occupy the *entire* `maxConcurrentNegotiations` pool by itself,
starving every other source. The per-source cap is what actually bounds that, checked before the global
pool is even consulted; a connection failing either check is shed immediately, and the shed itself runs
**off the accept loop** — `ShedInBackground` (`:432-444`) tracks the disposal via the same `inFlight`
`ConcurrentDictionary<Task, byte>` a genuine negotiation uses, rather than awaiting it inline, because
disposing a fully-established QUIC connection tears down its TLS 1.3 session and is measurably
expensive; awaiting it inline would serialise that cost onto the one loop that also has to keep
accepting everyone else.

**Design history — know this before "simplifying" it back:** an earlier version of this branch used a
two-semaphore design mirroring `TcpTransportListener`'s TLS pump almost exactly (a global bound plus a
much larger polled "pending" bound). A loopback test caught a head-of-line-queueing bug specific to
QUIC's lack of a cheap pre-check — the pending-bound trick works for TCP/WebSocket precisely because the
zero-byte-read pre-check keeps genuinely-idle peers out of the pool in the first place, and QUIC has no
equivalent signal to gate that pre-check on — so the design was simplified to the single global
semaphore above, then **hardened further during analyser review** by adding the per-source cap (found by
a security-review pass) and the non-blocking, off-loop shed (found by a performance-review pass). **The
two intermediate designs no longer exist in the shipped code — only the final combination (global
semaphore + per-source cap, both non-blocking) does.** Do not reintroduce a "pending" bound on the
reasoning that it worked for TCP; it would reintroduce exactly the queueing bug the loopback test caught,
because QUIC still has no cheap pre-check to make it safe.

Every negotiation is bounded by `streamOpenTimeout` (default 10 s) via a linked CTS with `CancelAfter`
(`NegotiateAsync`, `:529-530`). A transient `QuicException` on `AcceptConnectionAsync` — a malformed
initial packet, a peer that reset mid-handshake — pauses for `AcceptRetryDelay` (50 ms, `:38`) and
continues rather than retiring the listener (`:457-464`), the same transient-failure shape every other
pump uses.

**What the per-source cap does *not* do — mitigates, does not eliminate:** it bounds how much of the
global pool *one* source can hold, not how much a flood spread across *many distinct sources* can hold
between them. A distributed flood of genuine QUIC handshakes from `maxConcurrentNegotiations` different
source addresses, each opening no stream, can still exhaust the global pool exactly as it could before
the per-source cap existed — the cap changes the shape of the attack a single source can mount, not the
existence of the underlying "no cheap pre-check" gap. See [known-issues.md](known-issues.md) KI-40.

<a id="gotchas-5"></a>

### Gotchas

- **Concurrent `StartAsync` calls are now guarded — see [known-issues.md](known-issues.md) KI-41 (fixed)
  for the history.** Unlike every other listener here, `QuicListener.ListenAsync` is itself the
  asynchronous bind, so the "already running" check alone (taken before that await) is not enough: a
  second concurrent call could otherwise pass it too, before either had published anything. A `_starting`
  flag, claimed under `_stateLock` alongside that check and cleared once the bind either fails or
  publishes, closes the gap — mirroring `MeshHub.StartAsync`'s identical pattern.
  `StartAsync_CalledConcurrently_OnlyOneSucceeds` (`QuicTransportListenerTests.cs`) drives two overlapping
  calls and asserts exactly one succeeds and genuinely publishes usable state. Every other listener here
  cannot have this problem by construction, because their binds are synchronous and run entirely inside
  the lock.
- **A QUIC stream is invisible to the peer until data arrives on it — see the identical gotcha on
  [`QuicTransport`](#gotchas-4) above.** `AcceptAsync` will not return a connection until its client has
  sent something.
- **`maxConcurrentNegotiations` is not a connection-rate limiter, and its per-source sibling is a
  mitigation, not a fix — see [above](#the-negotiation-pump--two-tier-admission-read-this-before-touching-it)
  and [known-issues.md](known-issues.md) KI-40.** Size both, and `streamOpenTimeout`, for the deployment's
  real expected concurrent-connection count with this in mind.
- **No cleartext mode, ever** — same point as on `QuicTransport` above; the certificate guard in the
  constructor is therefore unconditional, unlike the optional-TLS listeners.

<a id="turning-tls-on-quic-both-ends"></a>

### Turning TLS on (QUIC, both ends)

TLS is not optional here, so this is simply "how to use the transport" rather than an opt-in step:

```csharp
// Hub
var listener = new QuicTransportListener(
    new IPEndPoint(IPAddress.Any, 22003),
    new SslServerAuthenticationOptions { ServerCertificate = hubCertificate });
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

// Client
await using var client = new MeshClient(clientLogger);
QuicTransport transport = await QuicTransport.ConnectAsync(
    "hub.example.com", 22003, new SslClientAuthenticationOptions());
await client.ConnectAsync(transport, "Alice");
```

As with the TCP and WebSocket pairs, **nothing above the transport changes** — `MeshHub`, `MeshClient`
and the wire protocol are untouched (confirmed: this branch does not modify `MeshHub.cs`, `MeshClient.cs`
or `IMeshClient.cs` at all). `QuicMeshIntegrationTests.EndToEnd_RegisterSendBroadcastAndGroupMessage_OverQuic`
is the reference end-to-end example.

### Usage

```csharp
// Hub
var listener = new QuicTransportListener(22003, new SslServerAuthenticationOptions { ServerCertificate = hubCertificate });
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

// Client
await using var client = new MeshClient(clientLogger);
QuicTransport transport = await QuicTransport.ConnectAsync(
    "localhost", 22003, new SslClientAuthenticationOptions());
await client.ConnectAsync(transport, "Alice");
```

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
   `MeshHub.AcceptLoopAsync` hot rather than stopping it. All four shipped listeners are worked examples.
5. You **cannot** implement `IBatchSendTransport` (it's internal) — you don't need to; the hub falls
   back to one-frame-at-a-time sends automatically.
6. **If your transport wraps a plain `Stream`** (a socket, a pipe, anything duplex), reuse the internal
   [`StreamFramer`](#shared-framing-streamframer-internal--transportframingstreamframercs18) helper
   rather than reimplementing the length-prefix framing — `TcpTransport`, `UnixSocketTransport`,
   `NamedPipeTransport` and `QuicTransport` all do (a `QuicStream` is a plain `Stream` for this purpose
   too). It is `internal`, so this only helps a transport added inside this assembly; an external
   transport still needs its own framing (or its own length-prefix scheme entirely, since none of this
   is part of the public contract).
7. **If your transport is network-backed and has a meaningful remote address, implement the public
   `IRemoteEndPointTransport`** so `maxConnectionsPerRemoteEndpoint` can see it — `TcpTransport`,
   `WebSocketTransport` and `QuicTransport` do this, `UnixSocketTransport` and `NamedPipeTransport`
   deliberately do not (there is no `IPEndPoint` for a local IPC transport to report). See
   [known-issues.md](known-issues.md) KI-38 for what skipping this costs a hub reachable only over such
   a transport.
8. **If accepting a connection requires an asynchronous bind step** (as QUIC's `QuicListener.ListenAsync`
   does, unlike every socket-backed listener's synchronous bind), a `StartAsync`-vs-`DisposeAsync` race
   is not the only one to defend against — a concurrent `StartAsync`-vs-`StartAsync` race needs its own
   guard too, published *before* the await, the way `MeshHub.StartAsync` itself claims a `_starting` flag
   before its own async work begins. `QuicTransportListener` does exactly this (see
   [known-issues.md](known-issues.md) KI-41) — follow the same shape for a new async-bind transport rather
   than reintroducing the gap it once had.
9. Test doubles: the suite mocks `ITransport`/`ITransportListener` directly with Moq. See the fixtures
   in [testing.md](testing.md).
