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
| ctor | `MeshHub(ILogger<MeshHub>, ITransportListener, TimeSpan? registrationTimeout=null, int? maxClients=null, TimeSpan? heartbeatInterval=null, int maxMissedHeartbeats=2, ClientAuthenticator? authenticator=null, int? maxConcurrentAuthentications=null)` | `MeshHub.cs:119` |
| `StartAsync` | `Task StartAsync(CancellationToken=default)` — binds listener, starts accept loop. Refuses a second concurrent start, a start during shutdown, and a start after disposal | `MeshHub.cs:193` |
| `StopAsync` | `Task StopAsync(CancellationToken=default)` — best-effort disconnect all, drain, reset. **Not `async`** — returns the shared shutdown task | `MeshHub.cs:264` |
| `DisposeAsync` | `ValueTask` — `StopAsync`, disposes the listener, then the authentication semaphore. Memoised; disposal is terminal | `MeshHub.cs:407` |
| `ConnectedClientCount` | `int` — snapshot of `_clients.Count`. **Not** the value `maxClients` is enforced against, and it can transiently read *below* the number of claimed slots | `MeshHub.cs:393` |
| `IsClientRegistered` | `bool IsClientRegistered(Guid)` | `MeshHub.cs:396` |
| `ClientConnected` | `event EventHandler<ClientConnectionEventArgs>` — after registration completes | `MeshHub.cs:387` |
| `ClientDisconnected` | `event EventHandler<ClientConnectionEventArgs>` — after a client is removed | `MeshHub.cs:390` |

The last two constructor parameters are the **authentication seam** added in protocol version 3; see
[Authentication](#authentication) below. Both are optional and default to "no authentication", which
preserves the pre-v3 open-admission behaviour.

### Using it efficiently

- Construct with a started-or-not listener; `StartAsync` calls `listener.StartAsync` for you. Calling
  `StartAsync` twice throws `InvalidOperationException` ("already running", `MeshHub.cs:202-205`), as
  does starting while a shutdown is still in flight. Starting a **disposed** hub throws
  `ObjectDisposedException` (`MeshHub.cs:200`). See [Lifecycle & concurrency](#lifecycle) below.
- **The hub owns the listener** — `DisposeAsync` disposes it. Do not dispose the listener yourself.
- `ClientConnected` / `ClientDisconnected` fire **from the per-client handler task**, so they run
  **concurrently for different clients**. Handlers must be thread-safe. A throwing handler is caught and
  logged (`RaiseClientEvent`, `MeshHub.cs:920-940`) — it will not fault the hub.
- `ConnectedClientCount` and `IsClientRegistered` are point-in-time snapshots over a
  `ConcurrentDictionary`; treat them as advisory.
- Shut down with `StopAsync` or `await using`. `StopAsync` sends a best-effort `Disconnect` frame to
  every client, cancels the accept loop, waits for all handler tasks, then clears all state. It is
  idempotent (a hub that is not running returns `Task.CompletedTask`, `MeshHub.cs:273-276`) and **safe
  under concurrent invocation** — overlapping callers share one shutdown. See
  [Lifecycle & concurrency](#lifecycle) below.
- **A stopped hub is not restartable in general.** `StopAsync` releases the hub's own state, but
  `ITransportListener` has no stop, so the endpoint stays bound and both shipped listeners throw on a
  second `StartAsync`. Treat a stopped hub as spent and dispose it. [known-issues.md](known-issues.md) KI-25.
- **The constructor validates and then warns.** Non-positive timeouts/counts throw
  `ArgumentOutOfRangeException`; `maxMissedHeartbeats < 1` is rejected outright (`MeshHub.cs:150-154`).
  Beyond that, constructing with `heartbeatInterval` set **and** `maxMissedHeartbeats: 1` logs a
  `Warning` once at construction (`MeshHub.cs:177-188`), because that combination evicts on the first
  idle interval and never probes — see [the heartbeat schedule](#heartbeat-schedule) below. It is a
  warning, not a throw: the configuration is legal if your clients send continuously. If you are
  asserting on hub logs in a test, expect that line.

---

## Internal architecture

### State

- `ConcurrentDictionary<Guid, ClientConnection> _clients` — registered connections by id.
- `ConcurrentDictionary<string, Guid> _clientNames` — name → id, the uniqueness gate. `TryAdd` here is
  the atomic "claim this name" operation (`MeshHub.cs:586`).
- `int _reservedClientSlots` (`MeshHub.cs:41`) — **the counter `maxClients` is actually enforced
  against**, not `_clients.Count`. A slot is claimed by one atomic compare-and-swap
  (`TryReserveClientSlot`, `MeshHub.cs:794`) during registration and given back by `ReleaseClientSlot`
  (`:823`) in the handler's `finally`. Read it with `Volatile.Read`; never write it directly. Shutdown
  deliberately does **not** reset it — each still-running handler owns its own claim and returns it
  itself. See [Registration handshake](#registration-handshake-hub-side) and
  [known-issues.md](known-issues.md) KI-26.
- `ConcurrentDictionary<string, Group> _groups` (`StringComparer.Ordinal`) — group name → membership.
- `ConcurrentDictionary<Task, byte> _handlerTasks` — live per-client handler tasks, awaited on shutdown.
- One `CancellationTokenSource? _cts` (`MeshHub.cs:57`) + `Task? _acceptLoopTask` (`:58`) for the accept
  loop lifecycle, plus `Task? _stopTask`, `Task? _disposeTask`, `bool _starting` and `bool _disposed`.
  **All six are guarded by `Lock _stateLock` (`MeshHub.cs:55`)** and must only be read or written inside
  it — see [Lifecycle & concurrency](#lifecycle).
- `ClientAuthenticator? _authenticator` + `SemaphoreSlim? _authenticationSlots` — the authentication
  seam. The semaphore is **only allocated when an authenticator was supplied** (`MeshHub.cs:171-175`),
  so an unauthenticated hub does no extra work and allocates nothing.

`ClientConnection` (nested, `MeshHub.cs:1252`) holds the id, name, transport, a bounded outbound
`Channel<byte[]>` (capacity **1024**, single-reader/multi-writer), an `ActivitySequence` counter
(`Interlocked`-incremented per received frame), and the `HashSet<string> Groups` it has joined.

<a id="lifecycle"></a>

### Lifecycle & concurrency

`StartAsync`, `StopAsync` and `DisposeAsync` can each be called from a different thread at the same
time. Since PR #64 (issue #12) all three are serialised behind a single `Lock _stateLock`
(`MeshHub.cs:55`) and the whole lifecycle obeys one rule:

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

**`StartAsync` (`MeshHub.cs:193`)** claims the running slot before doing any I/O:

1. Under the lock: throw `ObjectDisposedException` if `_disposed` (`:200`); throw
   `InvalidOperationException` if `_cts`, `_stopTask` or `_starting` says the hub is spoken for
   (`:202-205`); otherwise set `_starting = true` (`:211`).
2. Outside the lock: `await _listener.StartAsync` (`:218`). On failure, release the claim and dispose
   the unused token source (`:220-232`) — a hub whose listener failed to start is startable again.
3. Under the lock again: clear `_starting`, **re-check `_disposed`** (`:241-245`, a disposal may have
   completed while the listener was starting), then publish `_cts` and `_acceptLoopTask`
   **together** (`:251-252`).

> **Why the `_starting` flag rather than publishing `_cts` early.** Publishing the token source before
> the accept loop exists would let a concurrent `StopAsync` take ownership of a hub that had just bound
> its listener and then report itself stopped — leaving the endpoint bound with nothing serving it and
> no way to recover, since the listener cannot be started a second time.

**`StopAsync` (`MeshHub.cs:264`) is not `async`** — it is a plain method returning a `Task`, so its
decision is taken **synchronously** under the lock before the caller gets a task back. Under the lock:
if `_stopTask` is already set, join it; otherwise read `_cts` and, if it is null, return
`Task.CompletedTask` (`:273-276`); otherwise take ownership — capture the token source and accept-loop
task into locals, null both fields, and publish the shutdown in `_stopTask` (`:278-284`). Every caller
then awaits that one task, so **clients are notified once, not once per caller**, and every caller
returns only once the hub has actually stopped.

- A caller's own `cancellationToken` is honoured via `WaitAsync` (`:292`), but **abandoning the wait
  does not cancel the shutdown** — that belongs to the caller which started it.
- The teardown is split in two. `StopCoreAsync` (`:299`) opens with `await Task.Yield()` (`:305`) so
  none of it runs on the caller's stack while the lock is held, then sends the best-effort `Disconnect`
  notification (`:309-320`). `ShutDownAsync` (`:336`) does the shutdown proper: cancel (`:341`), drain
  the accept loop (`:343-353`) and the handler tasks (`:357-365`), clear the four registries
  (`:367-370`), dispose the token source (`:372`).
- **`ShutDownAsync` runs from `StopCoreAsync`'s `finally` (`:322-329`).** That is load-bearing: the
  notification's exception filter covers only `IOException`/`ObjectDisposedException`/
  `OperationCanceledException`, so before this an unfiltered transport exception abandoned the shutdown
  half way — accept loop still running, token source undisposed, hub reporting itself stopped and no
  later call able to put it right.
- `ShutDownAsync`'s own `finally` clears `_stopTask` (`:374-383`), so a shutdown that failed part way
  leaves the hub *stopped* rather than wedged as permanently *stopping*.

**`DisposeAsync` (`MeshHub.cs:407`)** sets `_disposed = true` **first** (`:415`), before any teardown
begins, so a start racing a disposal is refused rather than racing the listener's teardown. It then
memoises its teardown in `_disposeTask` (`:416`); every later or concurrent call awaits that same task.
`DisposeCoreAsync` (`:426`) yields (`:430`) before awaiting `StopAsync` (`:432`), then disposes the
listener (`:433`) and the authentication semaphore (`:434`) — **exactly once**. Disposal is terminal.

> **If you change any of this, keep the shape.** Do not reintroduce a second read of a lifecycle field
> outside the lock; do not await inside the lock; do not move `ShutDownAsync` out of the `finally`; and
> do not make `StopAsync` `async` again — its synchronous decision is what makes the "join the existing
> shutdown" handover race-free, and it is what the tests pin. See
> [testing.md](testing.md#parking-a-caller-mid-lifecycle) for the seams that pin these interleavings.

### The three per-connection tasks

Every accepted connection is handled by `HandleClientAsync` (`MeshHub.cs:479`), which after a successful
handshake spins up:

1. **Receive loop** — the body of `HandleClientAsync` itself: `transport.ReceiveAsync` → dispatch by
   opcode → route. Reads against one long-lived `clientCts` token (no per-frame CTS).
2. **Send loop** — `SendLoopAsync` (`MeshHub.cs:946`): drains the outbound `Channel`, **coalescing**
   already-queued frames up to a 64 KiB byte budget (`SendCoalesceByteBudget`, `MeshHub.cs:944`) into a
   single batched write when the transport implements `IBatchSendTransport`; otherwise sends them one at
   a time. A lone frame is sent immediately (no latency added).
3. **Heartbeat monitor** — `MonitorHeartbeatAsync` (`MeshHub.cs:1003`), **only if `heartbeatInterval`
   is set**. One `PeriodicTimer`. Compares `ActivitySequence` between ticks; an interval in which the
   sequence did not move is a **silent interval** and increments a miss counter. The counter is
   **checked before the probe**: on reaching `maxMissedHeartbeats` it cancels the client's CTS to evict
   and returns (`MeshHub.cs:1033-1041`); otherwise it enqueues a `Ping` and loops
   (`MeshHub.cs:1045`). Any frame from the client resets the counter to zero.

All three share `clientCts` (linked to the hub's token). Teardown in the `finally` block
(`MeshHub.cs:716-779`) completes the outbound queue, cancels the CTS, awaits the send + heartbeat tasks,
removes the client from all groups and both dictionaries, disposes the connection (which disposes the
transport), and raises `ClientDisconnected`.

<a id="heartbeat-schedule"></a>

> **Heartbeat schedule (know this before tuning):** eviction fires when `missedHeartbeats >=
> _maxMissedHeartbeats` (`MeshHub.cs:1033`), and the check sits **above** the `TryWrite` of the `Ping`
> (`MeshHub.cs:1045`). So "max missed = N" means exactly what it says — a client that sends nothing is
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
> combination (`MeshHub.cs:177-188`) rather than throwing, because it is legal if your clients are
> known to send continuously. Values below 1 throw `ArgumentOutOfRangeException` (`MeshHub.cs:150-154`).
>
> This was previously off by one — eviction on the (N+1)th interval — and was corrected in PR #61
> (issue #9). See [known-issues.md](known-issues.md) KI-11. If you are reading older notes, or a hub
> built before that change, the schedule was one interval longer and the client was probed N times.

### The accept loop

`AcceptLoopAsync` (`MeshHub.cs:437`) loops `listener.AcceptAsync`. `OperationCanceledException` /
`ObjectDisposedException` break the loop (shutdown); **any other exception is logged and swallowed** so
one bad connection cannot kill the hub (`MeshHub.cs:454-462`) — this is the intentional broad catch at a
background-service boundary. Each accepted transport is handed to `HandleClientAsync`; the handler task
is tracked in `_handlerTasks` and a `ContinueWith` removes it and logs faults.

> **That two-way split is what makes `ITransportListener`'s disposal contract load-bearing.** The retry
> branch is `continue` with **no delay**, so a listener that is finished but reports itself with anything
> other than `ObjectDisposedException` puts this loop into an unbounded hot spin rather than stopping it.
> Both shipped listeners translate accordingly; a custom one must too. See
> [transport.md](transport.md#itransportlistener--transportitransportlistenercs23) and
> [known-issues.md](known-issues.md) KI-22.

### Registration handshake (hub side)

Inside `HandleClientAsync` (`MeshHub.cs:488-602`), in order. The frame layout is
`[type][version][name length (2, big-endian)][name][credential]` — see [protocol.md](protocol.md#registration-handshake).

1. Receive one frame under a **registration-timeout** linked CTS. Timeout → drop silently (`:491-506`).
2. Validate: frame must be ≥ **2** bytes and opcode `RegistrationRequest` (`0x04`) — else drop (`:510-515`).
3. Byte 1 must equal `Protocol.Version` (**3**) — else send `Error(UnsupportedProtocolVersion)` and drop
   (`:517-523`).
4. Frame must be ≥ 4 bytes, then read the `ushort` name length at offset 2. A **zero** length, or one
   that runs past the payload, is malformed → **drop silently, no error frame** (`:525-538`). Decode the
   name from `[4, 4+len)` (`:540`).
5. If `name.Length > 256` (chars) send `Error(ClientNameTooLong)` and drop (`:542-548`).
6. **If an authenticator is configured** (`:550-571`) — and only then — two things happen, in this order:
   first an at-capacity **early-out**, `Volatile.Read(ref _reservedClientSlots) >= maxClients` →
   `Error(HubAtCapacity)` and drop *without* running the authenticator (`:557-561`); then the callback
   itself (`:563-570`, see [Authentication](#authentication)), anything other than `true` →
   `Error(AuthenticationFailed)` and drop. With no authenticator neither happens and the handler falls
   straight through to step 7.
7. **Claim a client slot.** `TryReserveClientSlot()` (`:578-582`, implementation at `:794`)
   compare-and-swaps `_reservedClientSlots` up by one if and only if it is still below `maxClients`.
   **This single atomic operation is the binding capacity decision**; it fails →
   `Error(HubAtCapacity)` and drop. On success the handler sets `slotReserved = true` (`:584`), which
   arms the matching `ReleaseClientSlot()` in its `finally` (`:764-767`).
8. `_clientNames.TryAdd(name, id)` — if it fails, name is taken → `Error(DuplicateClientName)` and drop
   (`:586-591`). The slot claimed at step 7 is given back on the way out by the `finally`.
9. Create `ClientConnection`, add to `_clients`, send `RegistrationComplete` + assigned 16-byte id,
   raise `ClientConnected`, start the send loop (+ heartbeat monitor), enter the receive loop (`:593-613`).

The assigned id is a fresh `Guid.NewGuid()` generated at handler start (`MeshHub.cs:481`).

> **The ordering of steps 6–8 is deliberate and load-bearing, and it is not the obvious ordering.** The
> *binding* capacity decision (step 7) sits **after** authentication; the cheap early-out inside step 6
> sits **before** it. Three invariants to preserve if you touch this method:
>
> - **A full hub must not run the authenticator.** That is the early-out at `:557-561`, and it is only an
>   early-out — it does not decide admission. Removing it would not soften the cap, but it would reopen
>   the case the check exists to shed: a connection flood driving credential checks, or pinning handler
>   tasks on a slow authenticator, against a hub with nothing left to admit them to.
> - **A refused client must not claim a name.** `_clientNames.TryAdd` stays last, after both the
>   authenticator and the slot claim, so a client refused for either reason never reserves a name.
> - **The claim must not move ahead of authentication, and the claim/release pairing must stay intact.**
>   The claim is taken after the authenticator returns precisely so an unauthenticated peer cannot hold
>   capacity away from one that would authenticate. Every successful `TryReserveClientSlot()` is owned by
>   exactly one handler and must be given back exactly once, by that handler's `finally` (`:764-767`) —
>   including on the duplicate-name path. A claim that escapes its release leaks capacity for the
>   lifetime of the hub. See [known-issues.md](known-issues.md) KI-26.

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

`AuthenticateAsync` (`MeshHub.cs:840-918`) wraps the callback with four protections, all of which exist
because **the callback runs on unauthenticated input, once per accepted connection**:

| Protection | Mechanism | Source |
|---|---|---|
| Concurrency cap | `SemaphoreSlim` of `maxConcurrentAuthentications` (default **64**) permits; a connection that cannot get a slot within `registrationTimeout` is refused | `MeshHub.cs:852-860` |
| Time bound | the callback's `ValueTask` is `WaitAsync(_registrationTimeout)`-ed, so a hanging callback cannot pin the handler task or its connection | `MeshHub.cs:875-887` |
| Throw isolation | any exception is logged and becomes a refusal rather than faulting the handler (callback boundary) | `MeshHub.cs:898-904` |
| Cancellation isolation | an `OperationCanceledException` raised *inside* the callback (e.g. an identity-provider call timing out) — as opposed to hub shutdown — becomes a logged refusal, not a silent drop | `MeshHub.cs:888-897` |

Every one of those paths results in the client receiving `Error(AuthenticationFailed)` and the connection
being dropped. **The client cannot distinguish a bad credential from a slow, throwing or overloaded
authenticator** — that is deliberate (it leaks nothing) but it makes hub-side logs the only diagnostic.

The credential is **copied out** of the inbound registration buffer before the context is built
(`MeshHub.cs:866`), so `RegistrationContext.Credential` does not alias the larger frame. The XML doc on
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
  throws `ArgumentOutOfRangeException` (`MeshHub.cs:156-161`).

### Routing helpers

| Method | Opcode in | Behaviour | Source |
|---|---|---|---|
| `RouteMessage` | `SendMessage` | Look up recipient; build `DeliverMessage`; `TryWrite` to its queue. Unknown recipient → logged `Debug`, **dropped**. Full queue → logged `Warning`, **dropped**. | `MeshHub.cs:1054` |
| `BroadcastMessage` | `BroadcastMessage` | Build one shared `DeliverMessage` frame; `TryWrite` to every client **except the sender**. | `MeshHub.cs:1082` |
| `JoinGroup` | `JoinGroup` | `GetOrAdd` the `Group`, add member under its lock; empty name ignored. Retries if the group was concurrently removed (`Removed` flag). | `MeshHub.cs:1109` |
| `LeaveGroup` | `LeaveGroup` | Remove member; if the group is now empty, mark `Removed` and drop it from `_groups`. | `MeshHub.cs:1137` |
| `SendToGroup` | `GroupMessage` | Snapshot member ids under the group lock, then build one shared `DeliverGroupMessage` frame (carrying the group name) and `TryWrite` to each member **except the sender**. | `MeshHub.cs:1180` |

**Shared delivery frames:** broadcast and group sends allocate the delivery buffer **once** and hand the
same `byte[]` to every recipient's queue. Send loops only read it, so concurrent reads of the
never-mutated buffer are safe. Do not mutate a delivery frame after it is built.

### Group locking model

Each `Group` (`MeshHub.cs:1245`) has its own `Lock`, a `HashSet<Guid> Members`, and a `bool Removed`.
Distinct groups route in parallel; only same-group mutation contends. The `Removed` flag closes a
join/remove race: when the last member leaves, the group is marked removed **under its lock** and taken
out of `_groups` only if that exact instance is still mapped (`TryRemove(KeyValuePair)`,
`MeshHub.cs:1175`). A concurrent `JoinGroup` that fetched the dying instance sees `Removed` and retries
against a fresh one (`MeshHub.cs:1116-1126`). A connection's own `Groups` set is only touched by its
receive loop and teardown, so it needs no lock.

---

## Contracts & gotchas

- **Ownership:** the hub owns every accepted transport and the listener; all are disposed on teardown.
- **Delivery is best-effort.** See the routing table — unknown recipients and full queues drop frames.
  This is the single most important behavioural fact about the hub. [known-issues.md](known-issues.md) KI-1, KI-4.
- **The shutdown writes the `Disconnect` frame directly to each transport** (`StopCoreAsync`,
  `MeshHub.cs:310-320`), bypassing the send loop, concurrently with any in-flight send-loop write. This
  is only safe because `ITransport.SendAsync` is required to be concurrency-safe. A custom transport
  that violates that contract will corrupt framing during shutdown. [known-issues.md](known-issues.md) KI-6.
- **That notification is sequential and has no send timeout** — one registered peer that stops reading
  can hold a token-less shutdown open indefinitely, and the peers behind it are never notified.
  Pass a cancellable token to `StopAsync` if you need a bound. [known-issues.md](known-issues.md) KI-24.
- **Lifecycle calls are safe under concurrency but the hub is single-use.** Overlapping `StopAsync` /
  `DisposeAsync` calls share one teardown; a stopped hub cannot generally be started again.
  See [Lifecycle & concurrency](#lifecycle), [known-issues.md](known-issues.md) KI-25.
- **`ConnectedClientCount` is not the capacity gauge.** `maxClients` is enforced against
  `_reservedClientSlots`, so the count can read *below* the number of claimed slots — during a
  registration between the claim and the `_clients` insert, and during shutdown while a handler is still
  unwinding. The invariant is `reserved >= registered`, so the discrepancy only ever errs conservative
  (the hub refuses slightly early, never admits over the cap). Do not write an admission check against
  `ConnectedClientCount`. [known-issues.md](known-issues.md) KI-26.
- **Name check is in chars, not bytes** (`clientName.Length`, `MeshHub.cs:542`), matching the client.
  A 256-char name can exceed 256 UTF-8 bytes. [known-issues.md](known-issues.md) KI-3.
- **Group names in `Join`/`Leave` frames are unbounded** (whole remainder of the frame), but
  `SendToGroup` frames cap the name at `ushort.MaxValue`. Minor asymmetry — a name longer than 65 535
  bytes could be joined but never targeted by a group send. [known-issues.md](known-issues.md) KI-8.
- **Lookup responses are handled inline in the receive loop** and awaited on `clientCts.Token`
  (`MeshHub.cs:695`) — a slow/blocked transport write here stalls that client's inbound processing.
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
(`HandleClient_SilentClient_IsEvictedOnConfiguredIntervalNotTheOneAfter`, `MeshHubTests.cs:1826`), the
N=1 no-probe boundary by `HandleClient_SilentClientWithSingleMissedHeartbeat_IsEvictedWithoutPinging`
(`:1874`), and the no-false-eviction direction by
`HandleClient_ClientSendingFramesEveryInterval_IsNotEvicted` (`:1918`). A bare "was it evicted?"
assertion cannot tell the Nth interval from the (N+1)th — the ping count is what makes it a regression
test, so keep the counting shape if you extend these.

The **capacity claim is pinned by three tests** added in PR #64's successor, PR #65:

| Test | Pins | Source |
|---|---|---|
| `HandleClient_LastSlotClaimedButNotYetRegistered_RefusesRatherThanExceedingMaxClients` | a registration reaching the decision while another holds the last slot but is not yet in `_clients` is refused | `MeshHubTests.cs:822` |
| `HandleClient_RefusedForDuplicateNameAfterClaimingSlot_GivesTheSlotBack` | the duplicate-name refusal releases its claim rather than leaking it | `:878` |
| `HandleClient_ClientDisconnects_GivesItsSlotBackForAReplacement` | an ordinary disconnect frees the slot for a replacement | `:921` |

The first test is why `TryReserveClientSlot`/`ReleaseClientSlot` are **`internal`** rather than private
(`InternalsVisibleTo` in `AdamSalisbury.Meshworx.csproj:26`): it needs to put the hub into the state a
concurrent registration produces — slot taken, client not yet registered — which is exactly the window
`ConnectedClientCount` cannot see. Keep them internal; making them private would cost that test.

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
