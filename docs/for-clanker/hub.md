# Hub — `MeshHub` / `IMeshHub`

[← back to index](../for-clanker.md) · related: [client.md](client.md) · [transport.md](transport.md) · [protocol.md](protocol.md) · [known-issues.md](known-issues.md)

The server side. `MeshHub` accepts connections from an `ITransportListener`, runs the registration
handshake, tracks registered clients by id and by name, and routes direct / broadcast / group messages
between them. It never interprets payloads — it reads the one-byte opcode and forwards the body.

- **Type:** `public sealed class MeshHub : IMeshHub, IAsyncDisposable` — `src/AdamSalisbury.Meshworx/MeshHub.cs:11`
- **Interface:** `IMeshHub` — `src/AdamSalisbury.Meshworx/IMeshHub.cs:3`

---

## Public surface

| Member | Signature | Source |
|---|---|---|
| ctor | `MeshHub(ILogger<MeshHub>, ITransportListener, TimeSpan? registrationTimeout=null, int? maxClients=null, TimeSpan? heartbeatInterval=null, int maxMissedHeartbeats=2)` | `MeshHub.cs:53` |
| `StartAsync` | `Task StartAsync(CancellationToken=default)` — binds listener, starts accept loop | `MeshHub.cs:97` |
| `StopAsync` | `Task StopAsync(CancellationToken=default)` — best-effort disconnect all, drain, reset | `MeshHub.cs:110` |
| `DisposeAsync` | `ValueTask` — `StopAsync` then disposes the listener | `MeshHub.cs:182` |
| `ConnectedClientCount` | `int` — snapshot of registered client count | `MeshHub.cs:173` |
| `IsClientRegistered` | `bool IsClientRegistered(Guid)` | `MeshHub.cs:176` |
| `ClientConnected` | `event EventHandler<ClientConnectionEventArgs>` — after registration completes | `MeshHub.cs:167` |
| `ClientDisconnected` | `event EventHandler<ClientConnectionEventArgs>` — after a client is removed | `MeshHub.cs:170` |

### Using it efficiently

- Construct with a started-or-not listener; `StartAsync` calls `listener.StartAsync` for you. Calling
  `StartAsync` twice throws `InvalidOperationException` ("already running", `MeshHub.cs:99-102`).
- **The hub owns the listener** — `DisposeAsync` disposes it. Do not dispose the listener yourself.
- `ClientConnected` / `ClientDisconnected` fire **from the per-client handler task**, so they run
  **concurrently for different clients**. Handlers must be thread-safe. A throwing handler is caught and
  logged (`RaiseClientEvent`, `MeshHub.cs:474-494`) — it will not fault the hub.
- `ConnectedClientCount` and `IsClientRegistered` are point-in-time snapshots over a
  `ConcurrentDictionary`; treat them as advisory.
- Shut down with `StopAsync` or `await using`. `StopAsync` sends a best-effort `Disconnect` frame to
  every client, cancels the accept loop, waits for all handler tasks, then clears all state. It is
  idempotent (returns early if not running, `MeshHub.cs:112-115`).

---

## Internal architecture

### State

- `ConcurrentDictionary<Guid, ClientConnection> _clients` — registered connections by id.
- `ConcurrentDictionary<string, Guid> _clientNames` — name → id, the uniqueness gate. `TryAdd` here is
  the atomic "claim this name" operation (`MeshHub.cs:293`).
- `ConcurrentDictionary<string, Group> _groups` (`StringComparer.Ordinal`) — group name → membership.
- `ConcurrentDictionary<Task, byte> _handlerTasks` — live per-client handler tasks, awaited on shutdown.
- One `CancellationTokenSource _cts` + `Task _acceptLoopTask` for the accept loop lifecycle.

`ClientConnection` (nested, `MeshHub.cs:803`) holds the id, name, transport, a bounded outbound
`Channel<byte[]>` (capacity **1024**, single-reader/multi-writer), an `ActivitySequence` counter
(`Interlocked`-incremented per received frame), and the `HashSet<string> Groups` it has joined.

### The three per-connection tasks

Every accepted connection is handled by `HandleClientAsync` (`MeshHub.cs:230`), which after a successful
handshake spins up:

1. **Receive loop** — the body of `HandleClientAsync` itself: `transport.ReceiveAsync` → dispatch by
   opcode → route. Reads against one long-lived `clientCts` token (no per-frame CTS).
2. **Send loop** — `SendLoopAsync` (`MeshHub.cs:500`): drains the outbound `Channel`, **coalescing**
   already-queued frames up to a 64 KiB byte budget (`SendCoalesceByteBudget`, `MeshHub.cs:498`) into a
   single batched write when the transport implements `IBatchSendTransport`; otherwise sends them one at
   a time. A lone frame is sent immediately (no latency added).
3. **Heartbeat monitor** — `MonitorHeartbeatAsync` (`MeshHub.cs:557`), **only if `heartbeatInterval`
   is set**. One `PeriodicTimer`. Compares `ActivitySequence` between ticks; on an idle interval it
   increments a miss counter and enqueues a `Ping`; when misses exceed `maxMissedHeartbeats` it cancels
   the client's CTS to evict.

All three share `clientCts` (linked to the hub's token). Teardown in the `finally` block
(`MeshHub.cs:423-471`) completes the outbound queue, cancels the CTS, awaits the send + heartbeat tasks,
removes the client from all groups and both dictionaries, disposes the connection (which disposes the
transport), and raises `ClientDisconnected`.

> **Heartbeat off-by-one (know this before tuning):** eviction fires when `missedHeartbeats >
> _maxMissedHeartbeats` (`MeshHub.cs:584`). With the default `maxMissedHeartbeats = 2`, a fully silent
> client is pinged on the 1st and 2nd idle intervals and **evicted on the 3rd**. So "max missed = N"
> means eviction after N+1 consecutive idle intervals. Grounded, not a bug — but size `idleTimeout` and
> monitoring around it.

### The accept loop

`AcceptLoopAsync` (`MeshHub.cs:188`) loops `listener.AcceptAsync`. `OperationCanceledException` /
`ObjectDisposedException` break the loop (shutdown); **any other exception is logged and swallowed** so
one bad connection cannot kill the hub (`MeshHub.cs:205-213`) — this is the intentional broad catch at a
background-service boundary. Each accepted transport is handed to `HandleClientAsync`; the handler task
is tracked in `_handlerTasks` and a `ContinueWith` removes it and logs faults.

### Registration handshake (hub side)

Inside `HandleClientAsync` (`MeshHub.cs:238-308`), in order:

1. Receive one frame under a **registration-timeout** linked CTS. Timeout → drop silently.
2. Validate: frame must be ≥ 3 bytes and opcode `RegistrationRequest` (`0x04`) — else drop.
3. Byte 1 must equal `Protocol.Version` (2) — else send `Error(UnsupportedProtocolVersion)` and drop.
4. Decode UTF-8 name; if `name.Length > 256` (chars) send `Error(ClientNameTooLong)` and drop.
5. If `_clients.Count >= maxClients` send `Error(HubAtCapacity)` and drop.
6. `_clientNames.TryAdd(name, id)` — if it fails, name is taken → `Error(DuplicateClientName)` and drop.
7. Create `ClientConnection`, add to `_clients`, send `RegistrationComplete` + assigned 16-byte id,
   raise `ClientConnected`, start the send loop (+ heartbeat monitor), enter the receive loop.

The assigned id is a fresh `Guid.NewGuid()` generated at handler start (`MeshHub.cs:232`).

### Routing helpers

| Method | Opcode in | Behaviour | Source |
|---|---|---|---|
| `RouteMessage` | `SendMessage` | Look up recipient; build `DeliverMessage`; `TryWrite` to its queue. Unknown recipient → logged `Debug`, **dropped**. Full queue → logged `Warning`, **dropped**. | `MeshHub.cs:605` |
| `BroadcastMessage` | `BroadcastMessage` | Build one shared `DeliverMessage` frame; `TryWrite` to every client **except the sender**. | `MeshHub.cs:633` |
| `JoinGroup` | `JoinGroup` | `GetOrAdd` the `Group`, add member under its lock; empty name ignored. Retries if the group was concurrently removed (`Removed` flag). | `MeshHub.cs:660` |
| `LeaveGroup` | `LeaveGroup` | Remove member; if the group is now empty, mark `Removed` and drop it from `_groups`. | `MeshHub.cs:688` |
| `SendToGroup` | `GroupMessage` | Snapshot member ids under the group lock, then build one shared `DeliverGroupMessage` frame (carrying the group name) and `TryWrite` to each member **except the sender**. | `MeshHub.cs:731` |

**Shared delivery frames:** broadcast and group sends allocate the delivery buffer **once** and hand the
same `byte[]` to every recipient's queue. Send loops only read it, so concurrent reads of the
never-mutated buffer are safe. Do not mutate a delivery frame after it is built.

### Group locking model

Each `Group` (`MeshHub.cs:796`) has its own `Lock`, a `HashSet<Guid> Members`, and a `bool Removed`.
Distinct groups route in parallel; only same-group mutation contends. The `Removed` flag closes a
join/remove race: when the last member leaves, the group is marked removed **under its lock** and taken
out of `_groups` only if that exact instance is still mapped (`TryRemove(KeyValuePair)`,
`MeshHub.cs:726`). A concurrent `JoinGroup` that fetched the dying instance sees `Removed` and retries
against a fresh one (`MeshHub.cs:667-677`). A connection's own `Groups` set is only touched by its
receive loop and teardown, so it needs no lock.

---

## Contracts & gotchas

- **Ownership:** the hub owns every accepted transport and the listener; all are disposed on teardown.
- **Delivery is best-effort.** See the routing table — unknown recipients and full queues drop frames.
  This is the single most important behavioural fact about the hub. [known-issues.md](known-issues.md) KI-1, KI-4.
- **`StopAsync` writes the `Disconnect` frame directly to each transport** (`MeshHub.cs:118-128`),
  bypassing the send loop, concurrently with any in-flight send-loop write. This is only safe because
  `ITransport.SendAsync` is required to be concurrency-safe. A custom transport that violates that
  contract will corrupt framing during shutdown. [known-issues.md](known-issues.md) KI-6.
- **Name check is in chars, not bytes** (`clientName.Length`, `MeshHub.cs:276`), matching the client.
  A 256-char name can exceed 256 UTF-8 bytes. [known-issues.md](known-issues.md) KI-3.
- **Group names in `Join`/`Leave` frames are unbounded** (whole remainder of the frame), but
  `SendToGroup` frames cap the name at `ushort.MaxValue`. Minor asymmetry — a name longer than 65 535
  bytes could be joined but never targeted by a group send. [known-issues.md](known-issues.md) KI-8.
- **Lookup responses are handled inline in the receive loop** and awaited on `clientCts.Token`
  (`MeshHub.cs:402`) — a slow/blocked transport write here stalls that client's inbound processing.
- **Malformed frames are silently ignored** — the dispatch chain is a series of length-guarded
  `else if`s with no terminal `else`. [known-issues.md](known-issues.md) KI-9.

## Idiomatic usage (from tests)

The hub is tested against a mocked `ITransportListener`/`ITransport` (Moq) rather than real sockets. See
`MeshHubFixture` (`src/Tests/AdamSalisbury.Meshworx.UnitTests/Fixtures/MeshHubFixture.cs`): it queues
mock transports for `AcceptAsync`, scripts `ReceiveAsync` sequences, and captures sent frames via a
`SendAsync` callback. `RegisterClientAsync` performs a full handshake and waits until
`IsClientRegistered` is true. Copy this pattern for new hub tests — do not stand up TCP. Details in
[testing.md](testing.md).
