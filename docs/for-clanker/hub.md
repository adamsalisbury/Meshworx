# Hub — `MeshHub` / `IMeshHub`

[← back to index](../for-clanker.md) · related: [client.md](client.md) · [transport.md](transport.md) · [protocol.md](protocol.md) · [known-issues.md](known-issues.md)

The server side. `MeshHub` accepts connections from an `ITransportListener`, runs the registration
handshake, tracks registered clients by id and by name, and routes direct / broadcast / group messages
between them. It never interprets payloads — it reads the one-byte opcode and forwards the body.

- **Type:** `public sealed class MeshHub : IMeshHub, IAsyncDisposable` — `src/AdamSalisbury.Meshworx/MeshHub.cs:11`
- **Interface:** `IMeshHub` — `src/AdamSalisbury.Meshworx/IMeshHub.cs:5`

---

## Public surface

| Member | Signature | Source |
|---|---|---|
| ctor | `MeshHub(ILogger<MeshHub>, ITransportListener, TimeSpan? registrationTimeout=null, int? maxClients=null, TimeSpan? heartbeatInterval=null, int maxMissedHeartbeats=2, ClientAuthenticator? authenticator=null, int? maxConcurrentAuthentications=null)` | `MeshHub.cs:108` |
| `StartAsync` | `Task StartAsync(CancellationToken=default)` — binds listener, starts accept loop. Refuses a second concurrent start, a start during shutdown, and a start after disposal | `MeshHub.cs:182` |
| `StopAsync` | `Task StopAsync(CancellationToken=default)` — best-effort disconnect all, drain, reset. **Not `async`** — returns the shared shutdown task | `MeshHub.cs:253` |
| `DisposeAsync` | `ValueTask` — `StopAsync`, disposes the listener, then the authentication semaphore. Memoised; disposal is terminal | `MeshHub.cs:396` |
| `ConnectedClientCount` | `int` — snapshot of registered client count | `MeshHub.cs:382` |
| `IsClientRegistered` | `bool IsClientRegistered(Guid)` | `MeshHub.cs:385` |
| `ClientConnected` | `event EventHandler<ClientConnectionEventArgs>` — after registration completes | `MeshHub.cs:376` |
| `ClientDisconnected` | `event EventHandler<ClientConnectionEventArgs>` — after a client is removed | `MeshHub.cs:379` |

The last two constructor parameters are the **authentication seam** added in protocol version 3; see
[Authentication](#authentication) below. Both are optional and default to "no authentication", which
preserves the pre-v3 open-admission behaviour.

### Using it efficiently

- Construct with a started-or-not listener; `StartAsync` calls `listener.StartAsync` for you. Calling
  `StartAsync` twice throws `InvalidOperationException` ("already running", `MeshHub.cs:191-194`), as
  does starting while a shutdown is still in flight. Starting a **disposed** hub throws
  `ObjectDisposedException` (`MeshHub.cs:189`). See [Lifecycle & concurrency](#lifecycle) below.
- **The hub owns the listener** — `DisposeAsync` disposes it. Do not dispose the listener yourself.
- `ClientConnected` / `ClientDisconnected` fire **from the per-client handler task**, so they run
  **concurrently for different clients**. Handlers must be thread-safe. A throwing handler is caught and
  logged (`RaiseClientEvent`, `MeshHub.cs:839-859`) — it will not fault the hub.
- `ConnectedClientCount` and `IsClientRegistered` are point-in-time snapshots over a
  `ConcurrentDictionary`; treat them as advisory.
- Shut down with `StopAsync` or `await using`. `StopAsync` sends a best-effort `Disconnect` frame to
  every client, cancels the accept loop, waits for all handler tasks, then clears all state. It is
  idempotent (a hub that is not running returns `Task.CompletedTask`, `MeshHub.cs:262-265`) and **safe
  under concurrent invocation** — overlapping callers share one shutdown. See
  [Lifecycle & concurrency](#lifecycle) below.
- **A stopped hub is not restartable in general.** `StopAsync` releases the hub's own state, but
  `ITransportListener` has no stop, so the endpoint stays bound and both shipped listeners throw on a
  second `StartAsync`. Treat a stopped hub as spent and dispose it. [known-issues.md](known-issues.md) KI-25.
- **The constructor validates and then warns.** Non-positive timeouts/counts throw
  `ArgumentOutOfRangeException`; `maxMissedHeartbeats < 1` is rejected outright (`MeshHub.cs:139-143`).
  Beyond that, constructing with `heartbeatInterval` set **and** `maxMissedHeartbeats: 1` logs a
  `Warning` once at construction (`MeshHub.cs:166-177`), because that combination evicts on the first
  idle interval and never probes — see [the heartbeat schedule](#heartbeat-schedule) below. It is a
  warning, not a throw: the configuration is legal if your clients send continuously. If you are
  asserting on hub logs in a test, expect that line.

---

## Internal architecture

### State

- `ConcurrentDictionary<Guid, ClientConnection> _clients` — registered connections by id.
- `ConcurrentDictionary<string, Guid> _clientNames` — name → id, the uniqueness gate. `TryAdd` here is
  the atomic "claim this name" operation (`MeshHub.cs:578`).
- `ConcurrentDictionary<string, Group> _groups` (`StringComparer.Ordinal`) — group name → membership.
- `ConcurrentDictionary<Task, byte> _handlerTasks` — live per-client handler tasks, awaited on shutdown.
- One `CancellationTokenSource? _cts` (`MeshHub.cs:46`) + `Task? _acceptLoopTask` (`:47`) for the accept
  loop lifecycle, plus `Task? _stopTask`, `Task? _disposeTask`, `bool _starting` and `bool _disposed`.
  **All six are guarded by `Lock _stateLock` (`MeshHub.cs:44`)** and must only be read or written inside
  it — see [Lifecycle & concurrency](#lifecycle).
- `ClientAuthenticator? _authenticator` + `SemaphoreSlim? _authenticationSlots` — the authentication
  seam. The semaphore is **only allocated when an authenticator was supplied** (`MeshHub.cs:160-164`),
  so an unauthenticated hub does no extra work and allocates nothing.

`ClientConnection` (nested, `MeshHub.cs:1171`) holds the id, name, transport, a bounded outbound
`Channel<byte[]>` (capacity **1024**, single-reader/multi-writer), an `ActivitySequence` counter
(`Interlocked`-incremented per received frame), and the `HashSet<string> Groups` it has joined.

<a id="lifecycle"></a>

### Lifecycle & concurrency

`StartAsync`, `StopAsync` and `DisposeAsync` can each be called from a different thread at the same
time. Since PR #64 (issue #12) all three are serialised behind a single `Lock _stateLock`
(`MeshHub.cs:44`) and the whole lifecycle obeys one rule:

> **Take the state you need in one critical section, copy it into locals, then work only from the
> locals — and never block or await while holding the lock.** Reading a lifecycle field twice is the
> bug this design exists to prevent: a concurrent stop could null `_cts` between another caller's null
> check and the dereference that followed it.

| Field | Meaning when set | Guarded by |
|---|---|---|
| `_cts` | the hub is running; this is the accept loop's token source | `_stateLock` |
| `_acceptLoopTask` | the running accept loop | `_stateLock` |
| `_stopTask` | a shutdown is in flight; concurrent stops await **this** task | `_stateLock` |
| `_disposeTask` | disposal has begun; concurrent disposals await **this** task | `_stateLock` |
| `_starting` | a start is between claiming the hub and publishing its accept loop | `_stateLock` |
| `_disposed` | disposal has begun — terminal, never cleared | `_stateLock` |

**`StartAsync` (`MeshHub.cs:182`)** claims the running slot before doing any I/O:

1. Under the lock: throw `ObjectDisposedException` if `_disposed` (`:189`); throw
   `InvalidOperationException` if `_cts`, `_stopTask` or `_starting` says the hub is spoken for
   (`:191-194`); otherwise set `_starting = true` (`:200`).
2. Outside the lock: `await _listener.StartAsync` (`:207`). On failure, release the claim and dispose
   the unused token source (`:209-221`) — a hub whose listener failed to start is startable again.
3. Under the lock again: clear `_starting`, **re-check `_disposed`** (`:230-234`, a disposal may have
   completed while the listener was starting), then publish `_cts` and `_acceptLoopTask`
   **together** (`:240-241`).

> **Why the `_starting` flag rather than publishing `_cts` early.** Publishing the token source before
> the accept loop exists would let a concurrent `StopAsync` take ownership of a hub that had just bound
> its listener and then report itself stopped — leaving the endpoint bound with nothing serving it and
> no way to recover, since the listener cannot be started a second time.

**`StopAsync` (`MeshHub.cs:253`) is not `async`** — it is a plain method returning a `Task`, so its
decision is taken **synchronously** under the lock before the caller gets a task back. Under the lock:
if `_stopTask` is already set, join it; otherwise read `_cts` and, if it is null, return
`Task.CompletedTask` (`:262-265`); otherwise take ownership — capture the token source and accept-loop
task into locals, null both fields, and publish the shutdown in `_stopTask` (`:267-273`). Every caller
then awaits that one task, so **clients are notified once, not once per caller**, and every caller
returns only once the hub has actually stopped.

- A caller's own `cancellationToken` is honoured via `WaitAsync` (`:281`), but **abandoning the wait
  does not cancel the shutdown** — that belongs to the caller which started it.
- The teardown is split in two. `StopCoreAsync` (`:288`) opens with `await Task.Yield()` (`:294`) so
  none of it runs on the caller's stack while the lock is held, then sends the best-effort `Disconnect`
  notification (`:298-309`). `ShutDownAsync` (`:325`) does the shutdown proper: cancel (`:330`), drain
  the accept loop (`:332-342`) and the handler tasks (`:346-354`), clear the four registries
  (`:356-359`), dispose the token source (`:361`).
- **`ShutDownAsync` runs from `StopCoreAsync`'s `finally` (`:311-318`).** That is load-bearing: the
  notification's exception filter covers only `IOException`/`ObjectDisposedException`/
  `OperationCanceledException`, so before this an unfiltered transport exception abandoned the shutdown
  half way — accept loop still running, token source undisposed, hub reporting itself stopped and no
  later call able to put it right.
- `ShutDownAsync`'s own `finally` clears `_stopTask` (`:363-372`), so a shutdown that failed part way
  leaves the hub *stopped* rather than wedged as permanently *stopping*.

**`DisposeAsync` (`MeshHub.cs:396`)** sets `_disposed = true` **first** (`:404`), before any teardown
begins, so a start racing a disposal is refused rather than racing the listener's teardown. It then
memoises its teardown in `_disposeTask` (`:405`); every later or concurrent call awaits that same task.
`DisposeCoreAsync` (`:415`) yields (`:419`) before awaiting `StopAsync` (`:421`), then disposes the
listener (`:422`) and the authentication semaphore (`:423`) — **exactly once**. Disposal is terminal.

> **If you change any of this, keep the shape.** Do not reintroduce a second read of a lifecycle field
> outside the lock; do not await inside the lock; do not move `ShutDownAsync` out of the `finally`; and
> do not make `StopAsync` `async` again — its synchronous decision is what makes the "join the existing
> shutdown" handover race-free, and it is what the tests pin. See
> [testing.md](testing.md#parking-a-caller-mid-lifecycle) for the seams that pin these interleavings.

### The three per-connection tasks

Every accepted connection is handled by `HandleClientAsync` (`MeshHub.cs:468`), which after a successful
handshake spins up:

1. **Receive loop** — the body of `HandleClientAsync` itself: `transport.ReceiveAsync` → dispatch by
   opcode → route. Reads against one long-lived `clientCts` token (no per-frame CTS).
2. **Send loop** — `SendLoopAsync` (`MeshHub.cs:865`): drains the outbound `Channel`, **coalescing**
   already-queued frames up to a 64 KiB byte budget (`SendCoalesceByteBudget`, `MeshHub.cs:863`) into a
   single batched write when the transport implements `IBatchSendTransport`; otherwise sends them one at
   a time. A lone frame is sent immediately (no latency added).
3. **Heartbeat monitor** — `MonitorHeartbeatAsync` (`MeshHub.cs:922`), **only if `heartbeatInterval`
   is set**. One `PeriodicTimer`. Compares `ActivitySequence` between ticks; an interval in which the
   sequence did not move is a **silent interval** and increments a miss counter. The counter is
   **checked before the probe**: on reaching `maxMissedHeartbeats` it cancels the client's CTS to evict
   and returns (`MeshHub.cs:952-960`); otherwise it enqueues a `Ping` and loops
   (`MeshHub.cs:964`). Any frame from the client resets the counter to zero.

All three share `clientCts` (linked to the hub's token). Teardown in the `finally` block
(`MeshHub.cs:708-756`) completes the outbound queue, cancels the CTS, awaits the send + heartbeat tasks,
removes the client from all groups and both dictionaries, disposes the connection (which disposes the
transport), and raises `ClientDisconnected`.

<a id="heartbeat-schedule"></a>

> **Heartbeat schedule (know this before tuning):** eviction fires when `missedHeartbeats >=
> _maxMissedHeartbeats` (`MeshHub.cs:952`), and the check sits **above** the `TryWrite` of the `Ping`
> (`MeshHub.cs:964`). So "max missed = N" means exactly what it says — a client that sends nothing is
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
> combination (`MeshHub.cs:166-177`) rather than throwing, because it is legal if your clients are
> known to send continuously. Values below 1 throw `ArgumentOutOfRangeException` (`MeshHub.cs:139-143`).
>
> This was previously off by one — eviction on the (N+1)th interval — and was corrected in PR #61
> (issue #9). See [known-issues.md](known-issues.md) KI-11. If you are reading older notes, or a hub
> built before that change, the schedule was one interval longer and the client was probed N times.

### The accept loop

`AcceptLoopAsync` (`MeshHub.cs:426`) loops `listener.AcceptAsync`. `OperationCanceledException` /
`ObjectDisposedException` break the loop (shutdown); **any other exception is logged and swallowed** so
one bad connection cannot kill the hub (`MeshHub.cs:443-451`) — this is the intentional broad catch at a
background-service boundary. Each accepted transport is handed to `HandleClientAsync`; the handler task
is tracked in `_handlerTasks` and a `ContinueWith` removes it and logs faults.

> **That two-way split is what makes `ITransportListener`'s disposal contract load-bearing.** The retry
> branch is `continue` with **no delay**, so a listener that is finished but reports itself with anything
> other than `ObjectDisposedException` puts this loop into an unbounded hot spin rather than stopping it.
> Both shipped listeners translate accordingly; a custom one must too. See
> [transport.md](transport.md#itransportlistener--transportitransportlistenercs23) and
> [known-issues.md](known-issues.md) KI-22.

### Registration handshake (hub side)

Inside `HandleClientAsync` (`MeshHub.cs:476-594`), in order. The frame layout is
`[type][version][name length (2, big-endian)][name][credential]` — see [protocol.md](protocol.md#registration-handshake).

1. Receive one frame under a **registration-timeout** linked CTS. Timeout → drop silently (`:479-494`).
2. Validate: frame must be ≥ **2** bytes and opcode `RegistrationRequest` (`0x04`) — else drop (`:498-503`).
3. Byte 1 must equal `Protocol.Version` (**3**) — else send `Error(UnsupportedProtocolVersion)` and drop
   (`:505-511`).
4. Frame must be ≥ 4 bytes, then read the `ushort` name length at offset 2. A **zero** length, or one
   that runs past the payload, is malformed → **drop silently, no error frame** (`:513-526`). Decode the
   name from `[4, 4+len)` (`:528`).
5. If `name.Length > 256` (chars) send `Error(ClientNameTooLong)` and drop (`:530-536`).
6. If `_clients.Count >= maxClients` send `Error(HubAtCapacity)` and drop (`:541-548`).
7. **If an authenticator is configured**, run it (`:550-559`, see [Authentication](#authentication)).
   Anything other than `true` → `Error(AuthenticationFailed)` and drop. Then **re-check capacity**
   (`:565-575`) because the await gave concurrent registrations a chance to fill the hub →
   `Error(HubAtCapacity)`.
8. `_clientNames.TryAdd(name, id)` — if it fails, name is taken → `Error(DuplicateClientName)` and drop
   (`:578-583`).
9. Create `ClientConnection`, add to `_clients`, send `RegistrationComplete` + assigned 16-byte id,
   raise `ClientConnected`, start the send loop (+ heartbeat monitor), enter the receive loop (`:585-605`).

The assigned id is a fresh `Guid.NewGuid()` generated at handler start (`MeshHub.cs:470`).

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

`AuthenticateAsync` (`MeshHub.cs:759-837`) wraps the callback with four protections, all of which exist
because **the callback runs on unauthenticated input, once per accepted connection**:

| Protection | Mechanism | Source |
|---|---|---|
| Concurrency cap | `SemaphoreSlim` of `maxConcurrentAuthentications` (default **64**) permits; a connection that cannot get a slot within `registrationTimeout` is refused | `MeshHub.cs:771-779` |
| Time bound | the callback's `ValueTask` is `WaitAsync(_registrationTimeout)`-ed, so a hanging callback cannot pin the handler task or its connection | `MeshHub.cs:794-806` |
| Throw isolation | any exception is logged and becomes a refusal rather than faulting the handler (callback boundary) | `MeshHub.cs:817-823` |
| Cancellation isolation | an `OperationCanceledException` raised *inside* the callback (e.g. an identity-provider call timing out) — as opposed to hub shutdown — becomes a logged refusal, not a silent drop | `MeshHub.cs:807-816` |

Every one of those paths results in the client receiving `Error(AuthenticationFailed)` and the connection
being dropped. **The client cannot distinguish a bad credential from a slow, throwing or overloaded
authenticator** — that is deliberate (it leaks nothing) but it makes hub-side logs the only diagnostic.

The credential is **copied out** of the inbound registration buffer before the context is built
(`MeshHub.cs:785`), so `RegistrationContext.Credential` does not alias the larger frame. The XML doc on
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
  throws `ArgumentOutOfRangeException` (`MeshHub.cs:145-150`).

### Routing helpers

| Method | Opcode in | Behaviour | Source |
|---|---|---|---|
| `RouteMessage` | `SendMessage` | Look up recipient; build `DeliverMessage`; `TryWrite` to its queue. Unknown recipient → logged `Debug`, **dropped**. Full queue → logged `Warning`, **dropped**. | `MeshHub.cs:973` |
| `BroadcastMessage` | `BroadcastMessage` | Build one shared `DeliverMessage` frame; `TryWrite` to every client **except the sender**. | `MeshHub.cs:1001` |
| `JoinGroup` | `JoinGroup` | `GetOrAdd` the `Group`, add member under its lock; empty name ignored. Retries if the group was concurrently removed (`Removed` flag). | `MeshHub.cs:1028` |
| `LeaveGroup` | `LeaveGroup` | Remove member; if the group is now empty, mark `Removed` and drop it from `_groups`. | `MeshHub.cs:1056` |
| `SendToGroup` | `GroupMessage` | Snapshot member ids under the group lock, then build one shared `DeliverGroupMessage` frame (carrying the group name) and `TryWrite` to each member **except the sender**. | `MeshHub.cs:1099` |

**Shared delivery frames:** broadcast and group sends allocate the delivery buffer **once** and hand the
same `byte[]` to every recipient's queue. Send loops only read it, so concurrent reads of the
never-mutated buffer are safe. Do not mutate a delivery frame after it is built.

### Group locking model

Each `Group` (`MeshHub.cs:1164`) has its own `Lock`, a `HashSet<Guid> Members`, and a `bool Removed`.
Distinct groups route in parallel; only same-group mutation contends. The `Removed` flag closes a
join/remove race: when the last member leaves, the group is marked removed **under its lock** and taken
out of `_groups` only if that exact instance is still mapped (`TryRemove(KeyValuePair)`,
`MeshHub.cs:1094`). A concurrent `JoinGroup` that fetched the dying instance sees `Removed` and retries
against a fresh one (`MeshHub.cs:1035-1045`). A connection's own `Groups` set is only touched by its
receive loop and teardown, so it needs no lock.

---

## Contracts & gotchas

- **Ownership:** the hub owns every accepted transport and the listener; all are disposed on teardown.
- **Delivery is best-effort.** See the routing table — unknown recipients and full queues drop frames.
  This is the single most important behavioural fact about the hub. [known-issues.md](known-issues.md) KI-1, KI-4.
- **The shutdown writes the `Disconnect` frame directly to each transport** (`StopCoreAsync`,
  `MeshHub.cs:299-309`), bypassing the send loop, concurrently with any in-flight send-loop write. This
  is only safe because `ITransport.SendAsync` is required to be concurrency-safe. A custom transport
  that violates that contract will corrupt framing during shutdown. [known-issues.md](known-issues.md) KI-6.
- **That notification is sequential and has no send timeout** — one registered peer that stops reading
  can hold a token-less shutdown open indefinitely, and the peers behind it are never notified.
  Pass a cancellable token to `StopAsync` if you need a bound. [known-issues.md](known-issues.md) KI-24.
- **Lifecycle calls are safe under concurrency but the hub is single-use.** Overlapping `StopAsync` /
  `DisposeAsync` calls share one teardown; a stopped hub cannot generally be started again.
  See [Lifecycle & concurrency](#lifecycle), [known-issues.md](known-issues.md) KI-25.
- **Name check is in chars, not bytes** (`clientName.Length`, `MeshHub.cs:530`), matching the client.
  A 256-char name can exceed 256 UTF-8 bytes. [known-issues.md](known-issues.md) KI-3.
- **Group names in `Join`/`Leave` frames are unbounded** (whole remainder of the frame), but
  `SendToGroup` frames cap the name at `ushort.MaxValue`. Minor asymmetry — a name longer than 65 535
  bytes could be joined but never targeted by a group send. [known-issues.md](known-issues.md) KI-8.
- **Lookup responses are handled inline in the receive loop** and awaited on `clientCts.Token`
  (`MeshHub.cs:687`) — a slow/blocked transport write here stalls that client's inbound processing.
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
(`HandleClient_SilentClient_IsEvictedOnConfiguredIntervalNotTheOneAfter`, `MeshHubTests.cs:1695`), the
N=1 no-probe boundary by `HandleClient_SilentClientWithSingleMissedHeartbeat_IsEvictedWithoutPinging`
(`:1743`), and the no-false-eviction direction by
`HandleClient_ClientSendingFramesEveryInterval_IsNotEvicted` (`:1787`). A bare "was it evicted?"
assertion cannot tell the Nth interval from the (N+1)th — the ping count is what makes it a regression
test, so keep the counting shape if you extend these.

The **lifecycle concurrency contract is pinned by eight tests** added in PR #64, grouped under the
`// StopAsync / DisposeAsync under concurrent invocation` banner at `MeshHubTests.cs:116`:

| Test | Pins | Source |
|---|---|---|
| `StopAsync_CalledWhileAShutdownIsInFlight_NotifiesEachClientOnce` | clients notified once, not once per caller | `MeshHubTests.cs:131` |
| `StopAsync_CalledWhileAShutdownIsInFlight_ReturnsOnlyOnceTheShutdownCompletes` | a joining caller does not return early | `:163` |
| `StopAsync_CalledConcurrently_AllCallersCompleteWithoutError` | reproduces the original `NullReferenceException` | `:203` |
| `DisposeAsync_CalledConcurrently_TearsTheHubDownOnce` | teardown memoised, listener disposed once | `:220` |
| `StartAsync_AfterDispose_ThrowsObjectDisposedException` | disposal is terminal | `:241` |
| `StartAsync_ListenerFailsToStart_LeavesTheHubStartable` | the `_starting` claim is released on failure | `:254` |
| `StopAsync_AfterCompleting_ReleasesTheHubsRunningClaim` | `_stopTask` cleared — hub not wedged as *stopping* | `:280` |
| `StopAsync_WhileAStartIsInProgress_LeavesTheStartedHubIntact` | a stop cannot abandon a just-bound listener | `:304` |

Two of these pin the interleaving **deterministically** rather than hoping for it, and the seams they
use are reusable — see [testing.md](testing.md#parking-a-caller-mid-lifecycle).
`StopAsync_CalledConcurrently_AllCallersCompleteWithoutError` is deliberately a genuine thread race and
is documented in its own `<remarks>` as the weaker of the pair; the deterministic test beside it is the
guard. Keep that pairing if you extend these.
