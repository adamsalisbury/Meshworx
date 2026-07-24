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

### `ITransportListener` — `Transport/ITransportListener.cs:11`

```csharp
Task StartAsync(CancellationToken = default);
Task<ITransport> AcceptAsync(CancellationToken = default);
```

Cancel pending `AcceptAsync` via the token before disposing. Disposing stops the listener.

### `IBatchSendTransport` (internal) — `Transport/IBatchSendTransport.cs:14`

```csharp
Task SendAsync(IReadOnlyList<ReadOnlyMemory<byte>> messages, CancellationToken = default);
```

An **optional capability**. The hub's send loop coalesces a burst of queued frames into one underlying
write when the connection's transport implements it (`MeshHub.SendLoopAsync`, `MeshHub.cs:693-695`);
transports that don't implement it just receive frames one at a time. It is deliberately **`internal`**:
only the bundled `TcpTransport` benefits and only the in-assembly hub consumes it, so it stays off the
public `ITransport` surface. Each element is delivered as its own message. **External transports cannot
and need not implement it.**

---

## `TcpTransport` — `Transport/Tcp/TcpTransport.cs:25`

`public sealed class TcpTransport : ITransport, IBatchSendTransport`. Length-prefixed framing over a
`Stream` — a `NetworkStream` from a `TcpClient`, an `SslStream` wrapping one when TLS is in use, or an
arbitrary `Stream` via an internal ctor used by loopback tests.

### Framing

Every message: **4-byte big-endian length prefix** (`HeaderSize=4`, `TcpTransport.cs:27`) followed by
the payload. `MaxPayloadSize = 1 MiB` (`:28`). See [protocol.md](protocol.md) for the byte layout.
**TLS does not change the framing** — the identical frames simply travel inside the TLS record layer,
so every send/receive path below behaves the same either way.

### Behaviour

- **`ConnectAsync(host, port, ct)`** (`:77`) — static factory; sets `NoDelay = true`, connects, returns
  a ready **cleartext** transport. Disposes the socket if connect throws.
- **`ConnectAsync(host, port, SslClientAuthenticationOptions, ct)`** (`:137`) — the TLS factory.
  Connects, wraps the `NetworkStream` in an `SslStream` (`leaveInnerStreamOpen: false`), runs
  `AuthenticateAsClientAsync`, and returns a transport over the authenticated stream. `tlsOptions` is
  **required** and non-null (`ArgumentNullException` otherwise) — use the cleartext overload if you do
  not want TLS. A failed handshake surfaces as `AuthenticationException`; on any throw the `SslStream`
  is disposed **before** the `TcpClient` (`:162-173`) so the partially negotiated session unwinds before
  the socket goes away.
  - **The handshake is bounded only by your `cancellationToken`.** There is no built-in client-side
    handshake timeout (unlike the listener's). Pass a token that expires if a hostile or dead peer must
    not be able to stall the caller indefinitely.
- **`IsEncrypted`** (`:61`) — `public bool`; true only when the underlying stream is an `SslStream` with
  `IsEncrypted` set. Cheap; intended for a start-up/health assertion that a deployment really is
  encrypted.
- **`SendAsync(single)`** (`:221`) — rejects payloads over 1 MiB with `ArgumentException` **before**
  writing (also guards the size addition against overflow). Rents the frame buffer from
  `ArrayPool<byte>.Shared`, writes header+payload, then **writes and flushes under an internal
  `SemaphoreSlim` write lock** — this is what makes concurrent `SendAsync` safe.
- **`SendAsync(batch)`** (`:264`) — frames the whole batch into one rented buffer, one `WriteAsync` +
  one `FlushAsync` under the write lock. Subtlety: if a payload in the batch is oversize, it frames and
  writes the **valid prefix up to** the first oversize frame, **then throws** — preserving the
  single-send "deliver-then-fault" behaviour so coalesced frames ahead of the bad one still go out
  (`:281-338`). Empty batch is a no-op; single-element batch delegates to the scalar path.
- **`ReceiveAsync`** (`:342`) — reads the 4-byte prefix into a **reused** `_headerBuffer` (safe because
  single-reader), then allocates a fresh `byte[payloadLength]` for the body and returns it. A length
  `< 0` or `> 1 MiB` throws `IOException` ("Invalid payload length") — framing is no longer trustworthy,
  so receive loops treat it as a transport failure and close cleanly. Length `0` returns `[]`. A clean
  or mid-frame EOF (`EndOfStreamException` in `ReadExactlyAsync`, `:391`) returns `null`.
- **`DisposeAsync`** (`:377`) — disposes the stream (the `SslStream` when TLS is in use, which closes
  the `NetworkStream` it owns), the `TcpClient` (if owned), and the write lock.

### `CloneClientOptions` (internal) — `:185`

`ConnectAsync` never uses the caller's `SslClientAuthenticationOptions` instance directly; it copies it
first. Two reasons, both load-bearing:

1. A later mutation of the caller's object cannot retroactively change how a live connection was
   authenticated.
2. Defaulting `TargetHost` is not a visible side effect on an object the caller may reuse.

`TargetHost` falls back to the dialled `host` when unset (`:193`) — that is the name the server
certificate is then validated against. The RSA padding switches (`AllowRsaPkcs1Padding`,
`AllowRsaPssPadding`) exist **only on Linux and Windows**; they are copied inside an
`OperatingSystem.IsLinux() || IsWindows()` guard (`:211-215`) because reading them elsewhere throws.

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
- Internal ctors (`TcpTransport(TcpClient)` `:38`, `TcpTransport(Stream)` `:43`,
  `TcpTransport(TcpClient, Stream)` `:48` — the last used by both TLS paths to pair the socket with its
  `SslStream`) are `internal` and reached by the listener and by `InternalsVisibleTo` tests; not part of
  the public API.

---

## `TcpTransportListener` — `Transport/Tcp/TcpTransportListener.cs:19`

`public sealed class TcpTransportListener : ITransportListener`. It has **two distinct internal modes**
— cleartext and TLS — selected by whether you pass `tlsOptions`. The `ITransportListener` surface is
identical in both; the machinery behind `AcceptAsync` is not.

### Constructors

Both take the same optional TLS parameters:

```csharp
TcpTransportListener(
    IPEndPoint endPoint,                                  // :86
    SslServerAuthenticationOptions? tlsOptions = null,
    TimeSpan? tlsHandshakeTimeout = null,                 // default 10s  (:21)
    int? maxConcurrentTlsHandshakes = null)               // default 64   (:22)

TcpTransportListener(int port, ...)                       // :150 — delegates to the above
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
| `endPoint` null | `ArgumentNullException` | `:92` |
| `tlsHandshakeTimeout <= TimeSpan.Zero` | `ArgumentOutOfRangeException` | `:94-98` |
| `maxConcurrentTlsHandshakes <= 0` | `ArgumentOutOfRangeException` | `:100-104` |
| `tlsOptions` with no `ServerCertificate`, no `ServerCertificateContext` and no `ServerCertificateSelectionCallback` | `ArgumentException` | `:106-118` |

The certificate guard is deliberate: without one *every* handshake fails, and a listener that silently
accepts nothing is far harder to diagnose at run time than a failed construction.

The options are copied by `CloneServerOptions` (`:281`) under the same contract as the client-side
clone — same reflection test, same shallow-copy caveat, same OS-guarded RSA padding switches
(`:303-307`).

### Lifecycle

- **`StartAsync`** (`:160`) is synchronous under the hood — creates and `Start()`s a `TcpListener`;
  throws `InvalidOperationException` if already running, and honours an already-cancelled token. **When
  TLS is configured it additionally** creates a bounded `Channel<TcpTransport>` of capacity
  `maxConcurrentTlsHandshakes` (`SingleReader = true`) and launches the background handshake pump
  (`:172-185`). The pump `Task.Yield()`s first (`:335`), so `StartAsync` stays non-blocking.
- **`AcceptAsync`** (`:191`) —
  - *Cleartext mode* (`:218-231`): awaits `AcceptTcpClientAsync`, sets `NoDelay`, wraps in a
    `TcpTransport`. If setting `NoDelay`/getting the stream throws (peer reset immediately after
    accept), it **disposes the socket and rethrows** rather than leaking it — the hub's accept loop then
    logs and continues.
  - *TLS mode* (`:198-216`): reads an already-authenticated transport from the channel. It never runs a
    handshake itself. A `ChannelClosedException` (pump stopped — disposed, or accept failed outright) is
    **translated to `ObjectDisposedException`, carrying the original cause as `InnerException`**. That
    translation is load-bearing: the hub's accept loop treats `ObjectDisposedException` as a reason to
    stop, whereas rethrowing the underlying error would be logged and retried forever against a listener
    that is never coming back.
- **`DisposeAsync`** (`:235`) is now genuinely asynchronous. Order matters and is commented as such:
  cancel the handshake CTS → stop the listener (unblocking the pump's pending accept) → **await the pump
  task** → dispose the CTS → drain the channel, disposing every handshaken-but-never-accepted transport
  so those sockets are not leaked. The pump never faults (it completes the channel with the error
  instead), so awaiting it here cannot throw.
- `internal EndPoint? LocalEndPoint` (`:48`) exposes the bound endpoint to tests (e.g. for ephemeral
  port 0).

### The TLS handshake pump — read this before touching it

`HandshakePumpAsync` (`:329`) and `HandshakeAsync` (`:404`) exist to keep one slow or hostile peer from
denying the listener to everyone else. The hub's accept loop consumes one connection at a time, so an
inline handshake would be head-of-line blocking on unauthenticated input. Four decisions are deliberate
and each has a counter-intuitive rationale — do not "simplify" any of them:

1. **The accept is never gated on a handshake bound** (`:321-326`). Waiting for a free handshake slot
   before accepting would hand an attacker the whole listener: a few dozen peers that connect and send
   nothing would hold every slot until their timeout, and the loop would stop accepting entirely.
2. **A connection only spends a handshake slot once its peer has actually sent something.**
   `HandshakeAsync` awaits a **zero-byte read** on the `NetworkStream` (`:430`) — which consumes nothing
   and completes only when data arrives — *before* acquiring from `handshakeSlots` (`:432`). A silent
   peer therefore waits out its own timeout without ever occupying part of the CPU budget.
   `maxConcurrentTlsHandshakes` bounds asymmetric crypto on unauthenticated input; it is not an
   admission limit.
3. **Admission is capped by a separate pending bound that is polled, never waited on.**
   `_maxPendingTlsHandshakes = maxConcurrentTlsHandshakes * 16` (`PendingHandshakeMultiplier`, `:28`);
   `pendingSlots.Wait(0, …)` (`:363-367`) sheds the connection immediately when full rather than parking
   the accept loop. Negotiating connections are mostly idle, so this bound caps memory and descriptors,
   not work — and it is sized off the one knob so there are not two.
4. **A transient `SocketException` on accept does not retire the listener** (`:350-359`) — it pauses for
   `AcceptRetryDelay` (50 ms, `:32`) and continues. The cleartext path recovers because the *hub's*
   accept loop logs and continues; the pump has no such caller and has to recover for itself. The pause
   stops a persistent condition (descriptor exhaustion) spinning the loop hot.

Every negotiation is bounded by `tlsHandshakeTimeout` via a linked CTS with `CancelAfter` (`:422-423`).
Outstanding handshakes are awaited in the pump's `finally` (`:400`) so the semaphores are not disposed
while slots are still held.

### Gotchas

- **A failed TLS handshake is completely invisible.** `HandshakeAsync`'s `catch` (`:446-460`) disposes
  the connection and swallows the cause — there is no logger at the transport layer, so an untrusted
  client certificate, a protocol mismatch, a timeout or a cleartext client all look identical to the
  hub: *a connection that never arrived*. Budget for this when debugging "the client cannot connect".
  [known-issues.md](known-issues.md) KI-18.
- **`AcceptAsync` in TLS mode can throw `ObjectDisposedException` for a reason other than disposal** —
  check `InnerException` for the real cause before assuming shutdown.
- **`maxConcurrentTlsHandshakes` is not a connection limit.** Sixteen times that many can be mid-flight,
  and the accept loop itself is unbounded. If you need an admission limit, that is a different control.
- **TLS is off unless you pass options.** No warning is emitted for a cleartext listener.

---

## Turning TLS on (both ends)

Real mutual-TLS setup, lifted from `MeshIntegrationTests.EndToEnd_MessageRoutedBetweenTwoClientsOverMutualTls`
(`MeshIntegrationTests.cs:336`):

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

- `Connect()` (`:34`) — **the client-side entry point.** Creates a transport pair, queues the server
  endpoint for `AcceptAsync`, returns the client endpoint. Throws if not started or stopped.
- `AcceptAsync` (`:54`) — reads the next queued server endpoint; a closed channel surfaces as
  `ObjectDisposedException`.
- Usage (README pattern): `var l = new InMemoryTransportListener(); ... await hub.StartAsync(); await
  client.ConnectAsync(l.Connect(), "Alice");` — note you call `l.Connect()` instead of
  `TcpTransport.ConnectAsync`.

### Gotchas

- **Unbounded channels → no back-pressure.** A fast producer with a stalled consumer grows memory
  without bound. Fine for tests and controlled in-process use; do not treat it as production-grade for
  adversarial workloads. [known-issues.md](known-issues.md) KI-7.
- `SendAsync` allocates a copy per message; acceptable for its intended niche.

---

## Implementing a custom transport

1. Implement `ITransport` (and `ITransportListener` if you need the hub to accept it). Own your framing;
   deliver each `SendAsync` payload as exactly one `ReceiveAsync` result.
2. Make `SendAsync` concurrency-safe; keep `ReceiveAsync` single-reader.
3. Return `null` from `ReceiveAsync` on close; dispose should unblock the peer's receive.
4. You **cannot** implement `IBatchSendTransport` (it's internal) — you don't need to; the hub falls
   back to one-frame-at-a-time sends automatically.
5. Test doubles: the suite mocks `ITransport`/`ITransportListener` directly with Moq. See the fixtures
   in [testing.md](testing.md).
