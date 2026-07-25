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
| ctor | `MeshHub(ILogger<MeshHub>, ITransportListener, TimeSpan? registrationTimeout=null, int? maxClients=null, TimeSpan? heartbeatInterval=null, int maxMissedHeartbeats=2, ClientAuthenticator? authenticator=null, int? maxConcurrentAuthentications=null)` | `MeshHub.cs:81` |
| `StartAsync` | `Task StartAsync(CancellationToken=default)` — binds listener, starts accept loop | `MeshHub.cs:154` |
| `StopAsync` | `Task StopAsync(CancellationToken=default)` — best-effort disconnect all, drain, reset | `MeshHub.cs:167` |
| `DisposeAsync` | `ValueTask` — `StopAsync`, disposes the listener, then the authentication semaphore | `MeshHub.cs:239` |
| `ConnectedClientCount` | `int` — snapshot of registered client count | `MeshHub.cs:230` |
| `IsClientRegistered` | `bool IsClientRegistered(Guid)` | `MeshHub.cs:233` |
| `ClientConnected` | `event EventHandler<ClientConnectionEventArgs>` — after registration completes | `MeshHub.cs:224` |
| `ClientDisconnected` | `event EventHandler<ClientConnectionEventArgs>` — after a client is removed | `MeshHub.cs:227` |

The last two constructor parameters are the **authentication seam** added in protocol version 3; see
[Authentication](#authentication) below. Both are optional and default to "no authentication", which
preserves the pre-v3 open-admission behaviour.

### Using it efficiently

- Construct with a started-or-not listener; `StartAsync` calls `listener.StartAsync` for you. Calling
  `StartAsync` twice throws `InvalidOperationException` ("already running", `MeshHub.cs:156-159`).
- **The hub owns the listener** — `DisposeAsync` disposes it. Do not dispose the listener yourself.
- `ClientConnected` / `ClientDisconnected` fire **from the per-client handler task**, so they run
  **concurrently for different clients**. Handlers must be thread-safe. A throwing handler is caught and
  logged (`RaiseClientEvent`, `MeshHub.cs:659-679`) — it will not fault the hub.
- `ConnectedClientCount` and `IsClientRegistered` are point-in-time snapshots over a
  `ConcurrentDictionary`; treat them as advisory.
- Shut down with `StopAsync` or `await using`. `StopAsync` sends a best-effort `Disconnect` frame to
  every client, cancels the accept loop, waits for all handler tasks, then clears all state. It is
  idempotent (returns early if not running, `MeshHub.cs:169-172`).
- **The constructor validates and then warns.** Non-positive timeouts/counts throw
  `ArgumentOutOfRangeException`; `maxMissedHeartbeats < 1` is rejected outright (`MeshHub.cs:112-116`).
  Beyond that, constructing with `heartbeatInterval` set **and** `maxMissedHeartbeats: 1` logs a
  `Warning` once at construction (`MeshHub.cs:139-150`), because that combination evicts on the first
  idle interval and never probes — see [the heartbeat schedule](#heartbeat-schedule) below. It is a
  warning, not a throw: the configuration is legal if your clients send continuously. If you are
  asserting on hub logs in a test, expect that line.

---

## Internal architecture

### State

- `ConcurrentDictionary<Guid, ClientConnection> _clients` — registered connections by id.
- `ConcurrentDictionary<string, Guid> _clientNames` — name → id, the uniqueness gate. `TryAdd` here is
  the atomic "claim this name" operation (`MeshHub.cs:398`).
- `ConcurrentDictionary<string, Group> _groups` (`StringComparer.Ordinal`) — group name → membership.
- `ConcurrentDictionary<Task, byte> _handlerTasks` — live per-client handler tasks, awaited on shutdown.
- One `CancellationTokenSource _cts` + `Task _acceptLoopTask` for the accept loop lifecycle.
- `ClientAuthenticator? _authenticator` + `SemaphoreSlim? _authenticationSlots` — the authentication
  seam. The semaphore is **only allocated when an authenticator was supplied** (`MeshHub.cs:133-137`),
  so an unauthenticated hub does no extra work and allocates nothing.

`ClientConnection` (nested, `MeshHub.cs:991`) holds the id, name, transport, a bounded outbound
`Channel<byte[]>` (capacity **1024**, single-reader/multi-writer), an `ActivitySequence` counter
(`Interlocked`-incremented per received frame), and the `HashSet<string> Groups` it has joined.

### The three per-connection tasks

Every accepted connection is handled by `HandleClientAsync` (`MeshHub.cs:288`), which after a successful
handshake spins up:

1. **Receive loop** — the body of `HandleClientAsync` itself: `transport.ReceiveAsync` → dispatch by
   opcode → route. Reads against one long-lived `clientCts` token (no per-frame CTS).
2. **Send loop** — `SendLoopAsync` (`MeshHub.cs:685`): drains the outbound `Channel`, **coalescing**
   already-queued frames up to a 64 KiB byte budget (`SendCoalesceByteBudget`, `MeshHub.cs:683`) into a
   single batched write when the transport implements `IBatchSendTransport`; otherwise sends them one at
   a time. A lone frame is sent immediately (no latency added).
3. **Heartbeat monitor** — `MonitorHeartbeatAsync` (`MeshHub.cs:742`), **only if `heartbeatInterval`
   is set**. One `PeriodicTimer`. Compares `ActivitySequence` between ticks; an interval in which the
   sequence did not move is a **silent interval** and increments a miss counter. The counter is
   **checked before the probe**: on reaching `maxMissedHeartbeats` it cancels the client's CTS to evict
   and returns (`MeshHub.cs:772-780`); otherwise it enqueues a `Ping` and loops
   (`MeshHub.cs:784`). Any frame from the client resets the counter to zero.

All three share `clientCts` (linked to the hub's token). Teardown in the `finally` block
(`MeshHub.cs:528-576`) completes the outbound queue, cancels the CTS, awaits the send + heartbeat tasks,
removes the client from all groups and both dictionaries, disposes the connection (which disposes the
transport), and raises `ClientDisconnected`.

<a id="heartbeat-schedule"></a>

> **Heartbeat schedule (know this before tuning):** eviction fires when `missedHeartbeats >=
> _maxMissedHeartbeats` (`MeshHub.cs:772`), and the check sits **above** the `TryWrite` of the `Ping`
> (`MeshHub.cs:784`). So "max missed = N" means exactly what it says — a client that sends nothing is
> **evicted on the Nth consecutive silent interval**, and is probed **N − 1 times** on its way there,
> because no ping is sent on the interval that evicts.
>
> | `maxMissedHeartbeats` | Pings sent to a fully silent client | Evicted on |
> |---|---|---|
> | 1 | **none** | 1st silent interval |
> | 2 (default) | 1 (at the end of interval 1) | 2nd silent interval |
> | N | N − 1 | Nth silent interval |
>
> With the default 2 and a 30 s interval, a silent client is pinged at 30 s and dropped at 60 s.
>
> **`maxMissedHeartbeats: 1` is the sharp edge.** There is no interval left in which a ping could be
> answered, so the hub never probes; a client that only *receives* — and therefore sends no frames of
> its own — is evicted every single interval. The constructor logs a `Warning` when it sees this
> combination (`MeshHub.cs:139-150`) rather than throwing, because it is legal if your clients are
> known to send continuously. Values below 1 throw `ArgumentOutOfRangeException` (`MeshHub.cs:112-116`).
>
> This was previously off by one — eviction on the (N+1)th interval — and was corrected in PR #61
> (issue #9). See [known-issues.md](known-issues.md) KI-11. If you are reading older notes, or a hub
> built before that change, the schedule was one interval longer and the client was probed N times.

### The accept loop

`AcceptLoopAsync` (`MeshHub.cs:246`) loops `listener.AcceptAsync`. `OperationCanceledException` /
`ObjectDisposedException` break the loop (shutdown); **any other exception is logged and swallowed** so
one bad connection cannot kill the hub (`MeshHub.cs:263-271`) — this is the intentional broad catch at a
background-service boundary. Each accepted transport is handed to `HandleClientAsync`; the handler task
is tracked in `_handlerTasks` and a `ContinueWith` removes it and logs faults.

### Registration handshake (hub side)

Inside `HandleClientAsync` (`MeshHub.cs:296-414`), in order. The frame layout is
`[type][version][name length (2, big-endian)][name][credential]` — see [protocol.md](protocol.md#registration-handshake).

1. Receive one frame under a **registration-timeout** linked CTS. Timeout → drop silently (`:299-314`).
2. Validate: frame must be ≥ **2** bytes and opcode `RegistrationRequest` (`0x04`) — else drop (`:318-323`).
3. Byte 1 must equal `Protocol.Version` (**3**) — else send `Error(UnsupportedProtocolVersion)` and drop
   (`:325-331`).
4. Frame must be ≥ 4 bytes, then read the `ushort` name length at offset 2. A **zero** length, or one
   that runs past the payload, is malformed → **drop silently, no error frame** (`:333-346`). Decode the
   name from `[4, 4+len)` (`:348`).
5. If `name.Length > 256` (chars) send `Error(ClientNameTooLong)` and drop (`:350-356`).
6. If `_clients.Count >= maxClients` send `Error(HubAtCapacity)` and drop (`:361-368`).
7. **If an authenticator is configured**, run it (`:370-379`, see [Authentication](#authentication)).
   Anything other than `true` → `Error(AuthenticationFailed)` and drop. Then **re-check capacity**
   (`:385-395`) because the await gave concurrent registrations a chance to fill the hub →
   `Error(HubAtCapacity)`.
8. `_clientNames.TryAdd(name, id)` — if it fails, name is taken → `Error(DuplicateClientName)` and drop
   (`:398-403`).
9. Create `ClientConnection`, add to `_clients`, send `RegistrationComplete` + assigned 16-byte id,
   raise `ClientConnected`, start the send loop (+ heartbeat monitor), enter the receive loop (`:405-425`).

The assigned id is a fresh `Guid.NewGuid()` generated at handler start (`MeshHub.cs:290`).

> **The ordering of steps 6–8 is deliberate and load-bearing.** Authentication sits *after* the capacity
> check (so a full hub never pays for a credential check, and a connection flood cannot drive
> authentication work) and *before* the name reservation (so a rejected client never claims a name).
> Preserve that ordering if you touch this method.

### Authentication

Added with protocol version 3. The hub does not implement any authentication scheme itself — it provides
a **seam** and leaves the policy to the integrator.

- Supply a `ClientAuthenticator` (see [types.md](types.md#authentication-types)) to the constructor. It
  receives a `RegistrationContext` — the client's name and the opaque credential bytes from the
  registration frame — and returns `true` to admit.
- **With no authenticator (the default) the hub admits any peer that completes the handshake.** That is
  the pre-v3 behaviour and it is still the default; it is only safe on a trusted network. See
  [known-issues.md](known-issues.md) KI-2.
- The library never interprets the credential bytes. Format, comparison and rotation are entirely the
  integrator's problem.

`AuthenticateAsync` (`MeshHub.cs:579-657`) wraps the callback with four protections, all of which exist
because **the callback runs on unauthenticated input, once per accepted connection**:

| Protection | Mechanism | Source |
|---|---|---|
| Concurrency cap | `SemaphoreSlim` of `maxConcurrentAuthentications` (default **64**) permits; a connection that cannot get a slot within `registrationTimeout` is refused | `MeshHub.cs:591-599` |
| Time bound | the callback's `ValueTask` is `WaitAsync(_registrationTimeout)`-ed, so a hanging callback cannot pin the handler task or its connection | `MeshHub.cs:614-626` |
| Throw isolation | any exception is logged and becomes a refusal rather than faulting the handler (callback boundary) | `MeshHub.cs:637-643` |
| Cancellation isolation | an `OperationCanceledException` raised *inside* the callback (e.g. an identity-provider call timing out) — as opposed to hub shutdown — becomes a logged refusal, not a silent drop | `MeshHub.cs:627-636` |

Every one of those paths results in the client receiving `Error(AuthenticationFailed)` and the connection
being dropped. **The client cannot distinguish a bad credential from a slow, throwing or overloaded
authenticator** — that is deliberate (it leaks nothing) but it makes hub-side logs the only diagnostic.

The credential is **copied out** of the inbound registration buffer before the context is built
(`MeshHub.cs:605`), so `RegistrationContext.Credential` does not alias the larger frame. The XML doc on
the delegate still tells callers to copy it if it must outlive the call — treat that as the contract,
not the current implementation.

```csharp
ClientAuthenticator authenticator = (context, _) =>
{
    bool ok = CredentialStore.IsValid(context.ClientName, context.Credential.Span);
    return ValueTask.FromResult(ok);
};

await using var hub = new MeshHub(logger, listener, authenticator: authenticator);
```

Gotchas when writing an authenticator:

- **Compare credentials in constant time.** The hub cannot do this for you; a naive `SequenceEqual` on a
  secret is a timing oracle reachable by any peer that can open the port.
- **Keep it cheap, or accept the cap.** An expensive check (a network round trip, a KDF) is bounded by
  `maxConcurrentAuthentications`; beyond that, connections queue and then fail at `registrationTimeout`.
  Raising the cap raises the work an unauthenticated peer can force the hub to do.
- **Do not throw to signal refusal.** It works — a throw is a refusal — but it logs at `Error` and costs
  an exception per rejected peer. Return `false`.
- `maxConcurrentAuthentications` is **ignored when no authenticator is supplied**; a non-positive value
  throws `ArgumentOutOfRangeException` (`MeshHub.cs:118-123`).

### Routing helpers

| Method | Opcode in | Behaviour | Source |
|---|---|---|---|
| `RouteMessage` | `SendMessage` | Look up recipient; build `DeliverMessage`; `TryWrite` to its queue. Unknown recipient → logged `Debug`, **dropped**. Full queue → logged `Warning`, **dropped**. | `MeshHub.cs:793` |
| `BroadcastMessage` | `BroadcastMessage` | Build one shared `DeliverMessage` frame; `TryWrite` to every client **except the sender**. | `MeshHub.cs:821` |
| `JoinGroup` | `JoinGroup` | `GetOrAdd` the `Group`, add member under its lock; empty name ignored. Retries if the group was concurrently removed (`Removed` flag). | `MeshHub.cs:848` |
| `LeaveGroup` | `LeaveGroup` | Remove member; if the group is now empty, mark `Removed` and drop it from `_groups`. | `MeshHub.cs:876` |
| `SendToGroup` | `GroupMessage` | Snapshot member ids under the group lock, then build one shared `DeliverGroupMessage` frame (carrying the group name) and `TryWrite` to each member **except the sender**. | `MeshHub.cs:919` |

**Shared delivery frames:** broadcast and group sends allocate the delivery buffer **once** and hand the
same `byte[]` to every recipient's queue. Send loops only read it, so concurrent reads of the
never-mutated buffer are safe. Do not mutate a delivery frame after it is built.

### Group locking model

Each `Group` (`MeshHub.cs:984`) has its own `Lock`, a `HashSet<Guid> Members`, and a `bool Removed`.
Distinct groups route in parallel; only same-group mutation contends. The `Removed` flag closes a
join/remove race: when the last member leaves, the group is marked removed **under its lock** and taken
out of `_groups` only if that exact instance is still mapped (`TryRemove(KeyValuePair)`,
`MeshHub.cs:914`). A concurrent `JoinGroup` that fetched the dying instance sees `Removed` and retries
against a fresh one (`MeshHub.cs:855-865`). A connection's own `Groups` set is only touched by its
receive loop and teardown, so it needs no lock.

---

## Contracts & gotchas

- **Ownership:** the hub owns every accepted transport and the listener; all are disposed on teardown.
- **Delivery is best-effort.** See the routing table — unknown recipients and full queues drop frames.
  This is the single most important behavioural fact about the hub. [known-issues.md](known-issues.md) KI-1, KI-4.
- **`StopAsync` writes the `Disconnect` frame directly to each transport** (`MeshHub.cs:175-185`),
  bypassing the send loop, concurrently with any in-flight send-loop write. This is only safe because
  `ITransport.SendAsync` is required to be concurrency-safe. A custom transport that violates that
  contract will corrupt framing during shutdown. [known-issues.md](known-issues.md) KI-6.
- **Name check is in chars, not bytes** (`clientName.Length`, `MeshHub.cs:350`), matching the client.
  A 256-char name can exceed 256 UTF-8 bytes. [known-issues.md](known-issues.md) KI-3.
- **Group names in `Join`/`Leave` frames are unbounded** (whole remainder of the frame), but
  `SendToGroup` frames cap the name at `ushort.MaxValue`. Minor asymmetry — a name longer than 65 535
  bytes could be joined but never targeted by a group send. [known-issues.md](known-issues.md) KI-8.
- **Lookup responses are handled inline in the receive loop** and awaited on `clientCts.Token`
  (`MeshHub.cs:507`) — a slow/blocked transport write here stalls that client's inbound processing.
- **Malformed frames are silently ignored** — the dispatch chain is a series of length-guarded
  `else if`s with no terminal `else`. [known-issues.md](known-issues.md) KI-9. A malformed *registration*
  frame is dropped the same way, without an error reply.
- **A hub with no authenticator admits anyone who can reach the listener.** The seam exists; using it is
  opt-in. [known-issues.md](known-issues.md) KI-2.

## Idiomatic usage (from tests)

The hub is tested against a mocked `ITransportListener`/`ITransport` (Moq) rather than real sockets. See
`MeshHubFixture` (`src/Tests/AdamSalisbury.Meshworx.UnitTests/Fixtures/MeshHubFixture.cs`): it queues
mock transports for `AcceptAsync`, scripts `ReceiveAsync` sequences, and captures sent frames via a
`SendAsync` callback. `RegisterClientAsync` performs a full handshake and waits until
`IsClientRegistered` is true. Copy this pattern for new hub tests — do not stand up TCP. Details in
[testing.md](testing.md).

The fixture takes `authenticator` and `maxConcurrentAuthentications` pass-throughs, and
`CreateRegistrationRequest(name, credential)` builds a v3 registration frame, so an authentication test
is a one-liner over the same harness — see the `HandleClient_Authenticator*` tests in `MeshHubTests.cs`.

The **heartbeat schedule above is pinned by tests**, and they are the reference if you change it: the
eviction interval is asserted indirectly by counting pings up to the moment of teardown
(`HandleClient_SilentClient_IsEvictedOnConfiguredIntervalNotTheOneAfter`, `MeshHubTests.cs:1453`), the
N=1 no-probe boundary by `HandleClient_SilentClientWithSingleMissedHeartbeat_IsEvictedWithoutPinging`
(`:1501`), and the no-false-eviction direction by
`HandleClient_ClientSendingFramesEveryInterval_IsNotEvicted` (`:1545`). A bare "was it evicted?"
assertion cannot tell the Nth interval from the (N+1)th — the ping count is what makes it a regression
test, so keep the counting shape if you extend these.
