# Transports — abstractions, TCP, in-memory

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [protocol.md](protocol.md) · [known-issues.md](known-issues.md)

The transport layer is the swap point. Hub and client depend only on `ITransport` /
`ITransportListener`; two concrete implementations ship (`Tcp*`, `InMemory*`), and you can add your own.
A transport is a **dumb, message-oriented pipe** — it owns framing but knows nothing about opcodes.

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
write when the connection's transport implements it (`MeshHub.SendLoopAsync`, `MeshHub.cs:527-529`);
transports that don't implement it just receive frames one at a time. It is deliberately **`internal`**:
only the bundled `TcpTransport` benefits and only the in-assembly hub consumes it, so it stays off the
public `ITransport` surface. Each element is delivered as its own message. **External transports cannot
and need not implement it.**

---

## `TcpTransport` — `Transport/Tcp/TcpTransport.cs:15`

`public sealed class TcpTransport : ITransport, IBatchSendTransport`. Length-prefixed framing over a
`Stream` (from a `TcpClient`, or an arbitrary `Stream` via an internal ctor used by loopback tests).

### Framing

Every message: **4-byte big-endian length prefix** (`HeaderSize=4`, `TcpTransport.cs:17`) followed by
the payload. `MaxPayloadSize = 1 MiB` (`:18`). See [protocol.md](protocol.md) for the byte layout.

### Behaviour

- **`ConnectAsync(host, port, ct)`** (`:46`) — static factory; sets `NoDelay = true`, connects, returns
  a ready transport. Disposes the socket if connect throws.
- **`SendAsync(single)`** (`:66`) — rejects payloads over 1 MiB with `ArgumentException` **before**
  writing (also guards the size addition against overflow). Rents the frame buffer from
  `ArrayPool<byte>.Shared`, writes header+payload, then **writes and flushes under an internal
  `SemaphoreSlim` write lock** — this is what makes concurrent `SendAsync` safe.
- **`SendAsync(batch)`** (`:109`) — frames the whole batch into one rented buffer, one `WriteAsync` +
  one `FlushAsync` under the write lock. Subtlety: if a payload in the batch is oversize, it frames and
  writes the **valid prefix up to** the first oversize frame, **then throws** — preserving the
  single-send "deliver-then-fault" behaviour so coalesced frames ahead of the bad one still go out
  (`:126-183`). Empty batch is a no-op; single-element batch delegates to the scalar path.
- **`ReceiveAsync`** (`:187`) — reads the 4-byte prefix into a **reused** `_headerBuffer` (safe because
  single-reader), then allocates a fresh `byte[payloadLength]` for the body and returns it. A length
  `< 0` or `> 1 MiB` throws `IOException` ("Invalid payload length") — framing is no longer trustworthy,
  so receive loops treat it as a transport failure and close cleanly. Length `0` returns `[]`. A clean
  or mid-frame EOF (`EndOfStreamException` in `ReadExactlyAsync`, `:229`) returns `null`.
- **`DisposeAsync`** (`:222`) — disposes the stream, the `TcpClient` (if owned), and the write lock.

### Gotchas

- **Every received frame is a fresh allocation** (`new byte[payloadLength]`). Delivery is not pooled on
  the read path. The `ReadOnlyMemory<byte>` handed to event handlers is a view over this per-frame array
  — safe to retain today, but copy if you want to be robust against a future pooling change.
- **1 MiB payload cap is enforced on both send and receive.** Oversize send → `ArgumentException` to
  the caller; oversize received length → `IOException` → connection dropped. Keep the two peers'
  `MaxPayloadSize` in agreement if you ever fork the transport.
- Internal ctors (`TcpTransport(TcpClient)`, `TcpTransport(Stream)`) are `internal` and reached by the
  listener and by `InternalsVisibleTo` tests; not part of the public API.

---

## `TcpTransportListener` — `Transport/Tcp/TcpTransportListener.cs:9`

`public sealed class TcpTransportListener : ITransportListener`.

- Ctors: `(IPEndPoint)` or `(int port)` (binds `IPAddress.Any`, `:24`).
- `StartAsync` (`:29`) is synchronous under the hood — creates and `Start()`s a `TcpListener`; throws
  if already running or if the token is already cancelled.
- `AcceptAsync` (`:44`) awaits `AcceptTcpClientAsync`, sets `NoDelay`, wraps in a `TcpTransport`. If
  setting `NoDelay`/getting the stream throws (peer reset immediately after accept), it **disposes the
  socket and rethrows** rather than leaking it — the hub's accept loop then logs and continues.
- `DisposeAsync` (`:67`) stops the listener.
- `internal EndPoint? LocalEndPoint` (`:13`) exposes the bound endpoint to tests (e.g. for ephemeral
  port 0).

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
