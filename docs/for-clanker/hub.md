# Hub — `MeshHub` / `IMeshHub`

[← back to index](../for-clanker.md) · related: [client.md](client.md) · [transport.md](transport.md) · [protocol.md](protocol.md) · [known-issues.md](known-issues.md)

The server side. `MeshHub` accepts connections from an `ITransportListener`, runs the registration
handshake, tracks registered clients by id and by name, and routes direct / broadcast / group messages
between them. It never interprets payloads — it reads the one-byte opcode and forwards the body.

- **Type:** `public sealed class MeshHub : IMeshHub, IAsyncDisposable` — `src/AdamSalisbury.Meshworx/MeshHub.cs:15`
- **Interface:** `IMeshHub` — `src/AdamSalisbury.Meshworx/IMeshHub.cs:5`

---

## Public surface

| Member | Signature | Source |
|---|---|---|
| ctor | `MeshHub(ILogger<MeshHub>, ITransportListener, TimeSpan? registrationTimeout=null, int? maxClients=null, TimeSpan? heartbeatInterval=null, int maxMissedHeartbeats=2, ClientAuthenticator? authenticator=null, int? maxConcurrentAuthentications=null, GroupAuthoriser? groupAuthoriser=null, TimeSpan? groupAuthorisationTimeout=null, int? maxConnectionsPerRemoteEndpoint=null, bool notifyOnQueueSaturation=false, TimeSpan? backpressureAwaitTimeout=null, IOfflineStore? offlineStore=null, TimeSpan? offlineStoreTimeout=null, TimeSpan? sessionResumptionWindow=null)` | `MeshHub.cs:255` |
| `StartAsync` | `Task StartAsync(CancellationToken=default)` — binds listener, starts accept loop. Refuses a second concurrent start, a start during shutdown, and a start after disposal | `MeshHub.cs:446` |
| `StopAsync` | `Task StopAsync(CancellationToken=default)` — best-effort disconnect all, drain, reset. **Not `async`** — returns the shared shutdown task | `MeshHub.cs:517` |
| `DisposeAsync` | `ValueTask` — `StopAsync`, disposes the listener, then the authentication semaphore. Memoised; disposal is terminal | `MeshHub.cs:709` |
| `ConnectedClientCount` | `int` — snapshot of `_clients.Count`. **Not** the value `maxClients` is enforced against, and it can transiently read *below* the number of claimed slots | `MeshHub.cs:649` |
| `IsRunning` | `bool` — `true` from the moment `StartAsync` completes until `StopAsync` begins tearing the hub down. Added by PR #71 (issue #23) for the `AddMeshHub` health check | `MeshHub.cs:652-661` |
| `MaxClients` | `int` — the cap passed to the constructor (or `DefaultMaxClients` if omitted). Backed the admission check as a private field before PR #71; now also a public getter, unchanged in behaviour | `MeshHub.cs:664` |
| `ClaimedClientSlots` | `int` — `Volatile.Read` of `_reservedClientSlots`. **This, not `ConnectedClientCount`, is what `MaxClients` is actually enforced against** — see the bullet below and [known-issues.md](known-issues.md) KI-26. Added by PR #71 for the `AddMeshHub` health check | `MeshHub.cs:667` |
| `IsClientRegistered` | `bool IsClientRegistered(Guid)` | `MeshHub.cs:670` |
| `ClientConnected` | `event EventHandler<ClientConnectionEventArgs>` — after registration completes | `MeshHub.cs:640` |
| `ClientDisconnected` | `event EventHandler<ClientConnectionEventArgs>` — after a client is removed | `MeshHub.cs:643` |
| `QueueSaturated` | `event EventHandler<QueueSaturatedEventArgs>` — a message was dropped because the recipient's outbound queue was full. **Always raised, for every send shape**, independently of the constructor's `notifyOnQueueSaturation` flag. Added by PR #87 (issue #30) — see [Backpressure signalling](#backpressure-signalling-and-awaiting-capacity) | `MeshHub.cs:646` |

The constructor carries **three independent integrator seams**, all optional and all defaulting to "not
configured" — plus one further opt-in switch, `sessionResumptionWindow`, which is not a seam (there is
no callback to supply) but follows the same "null means the feature does not exist" rule; see
[Session resumption](#session-resumption).

| Seam | Params | Question it answers | Section |
|---|---|---|---|
| Authentication | `authenticator`, `maxConcurrentAuthentications` | may this peer **register at all**? | [Authentication](#authentication) |
| Authorisation | `groupAuthoriser`, `groupAuthorisationTimeout` | may this registered client **join this group**? | [Group authorisation](#group-authorisation) |
| Offline delivery | `offlineStore`, `offlineStoreTimeout` | where does a message for a client that **is not here** go? | [Offline delivery](#offline-delivery) |

Defaulting all three to `null` preserves the pre-v3 open-admission behaviour, and — for the third —
the original drop-on-unknown-recipient behaviour. **One group rule is not
optional and applies with or without an authoriser: sending to a group requires membership of it** — see
[Routing helpers](#routing-helpers).

### Using it efficiently

- Construct with a started-or-not listener; `StartAsync` calls `listener.StartAsync` for you. Calling
  `StartAsync` twice throws `InvalidOperationException` ("already running", `MeshHub.cs:455-458`), as
  does starting while a shutdown is still in flight. Starting a **disposed** hub throws
  `ObjectDisposedException` (`MeshHub.cs:453`). See [Lifecycle & concurrency](#lifecycle) below.
- **The hub owns the listener** — `DisposeAsync` disposes it. Do not dispose the listener yourself.
- `ClientConnected` / `ClientDisconnected` fire **from the per-client handler task**, so they run
  **concurrently for different clients**. Handlers must be thread-safe. A throwing handler is caught and
  logged (`RaiseClientEvent`, `MeshHub.cs:1485-1505`) — it will not fault the hub. That catch covers the
  handler only up to its **first suspension**: an `async void` handler's later exception escapes to the
  thread pool unobserved, so keep handlers synchronous or contain the failure inside the task you start
  (see [for-clanker.md](../for-clanker.md#4-threading--async-model-read-before-changing-any-loop) and the
  README's **Event handlers** section).
- **`QueueSaturated` (PR #87, issue #30) fires from whichever routing method dropped the frame** — the
  same per-connection-task concurrency and callback-boundary containment as `ClientConnected`/
  `ClientDisconnected` above applies (`RaiseQueueSaturated`, `MeshHub.cs:1517-1534`). It is raised for
  **every** shape of send — direct, broadcast and group alike — regardless of the constructor's
  `notifyOnQueueSaturation` flag, which only controls a *separate*, sender-facing wire notification. See
  [Backpressure signalling](#backpressure-signalling-and-awaiting-capacity) below.
- `ConnectedClientCount` and `IsClientRegistered` are point-in-time snapshots over a
  `ConcurrentDictionary`; treat them as advisory.
- **`IsRunning`, `MaxClients` and `ClaimedClientSlots` (PR #71, issue #23) exist so an integrator can build
  its own liveness/capacity signal without reaching for `internal` members** — this is exactly what
  `AdamSalisbury.Meshworx.Extensions.DependencyInjection`'s `AddMeshHub` health check does (see
  [dependency-injection.md](dependency-injection.md#health-checks)). **Judge capacity against
  `ClaimedClientSlots`, never `ConnectedClientCount`**: a slot is claimed as soon as a connection is
  accepted, before registration completes, and released only once its handler has fully finished, so
  `ClaimedClientSlots` stays ahead of `ConnectedClientCount` while a client is mid-handshake or
  mid-teardown — comparing `ConnectedClientCount` against `MaxClients` instead can read "below capacity"
  while the hub is already refusing new connections. See [known-issues.md](known-issues.md) KI-26 for the
  underlying admission mechanics `ClaimedClientSlots` exposes.
- Shut down with `StopAsync` or `await using`. `StopAsync` sends a best-effort `Disconnect` frame to
  every client, cancels the accept loop, waits for all handler tasks, then clears all state. It is
  idempotent (a hub that is not running returns `Task.CompletedTask`, `MeshHub.cs:526-529`) and **safe
  under concurrent invocation** — overlapping callers share one shutdown. See
  [Lifecycle & concurrency](#lifecycle) below.
- **A stopped hub is not restartable in general.** `StopAsync` releases the hub's own state, but
  `ITransportListener` has no stop, so the endpoint stays bound and both shipped listeners throw on a
  second `StartAsync`. Treat a stopped hub as spent and dispose it. [known-issues.md](known-issues.md) KI-25.
- **The constructor validates and then warns.** Non-positive timeouts/counts throw
  `ArgumentOutOfRangeException`; `maxMissedHeartbeats < 1` is rejected outright (`MeshHub.cs:278-282`),
  as is a non-positive `groupAuthorisationTimeout` (`MeshHub.cs:291-295`), and — since PR #87 (issue
  #30) — a `backpressureAwaitTimeout` that is neither positive nor `Timeout.InfiniteTimeSpan`
  (`MeshHub.cs:306-315`). Beyond that it logs **three**
  possible warnings, neither of which throws — if you assert on hub logs in a test, expect them:
  - `heartbeatInterval` set **and** `maxMissedHeartbeats: 1` (`MeshHub.cs:341-354`), because that
    combination evicts on the first idle interval and never probes — see
    [the heartbeat schedule](#heartbeat-schedule) below. Legal if your clients send continuously.
  - `groupAuthoriser` set **and** `groupAuthorisationTimeout >= heartbeatInterval × maxMissedHeartbeats`
    (`MeshHub.cs:356-421`). A client's receive loop is parked while its join is being authorised, so it
    looks idle to the heartbeat monitor however healthy it is; a decision that outlasts the eviction
    budget gets the client **evicted rather than refused** — and behind a `MeshClientReconnector` that
    becomes a reconnect loop. Keep the timeout comfortably below the budget. It warns rather than
    throwing because the default 10 s timeout would otherwise refuse construction of any hub with a
    short heartbeat interval, and a slow authoriser may never actually take that long.
  - **`backpressureAwaitTimeout == Timeout.InfiniteTimeSpan` (PR #87, issue #30, `MeshHub.cs:386-393`).**
    A parked sender is deliberately exempt from idle eviction (see
    [the heartbeat schedule](#heartbeat-schedule) below), so an infinite wait removes the *only* bound on
    how long a connection can sit parked against a recipient that never drains — legal if every recipient
    is known to drain eventually, but worth a second look if it fires unexpectedly.

---

## Internal architecture

### State

- `ConcurrentDictionary<Guid, ClientConnection> _clients` — registered connections by id.
- `ConcurrentDictionary<string, Guid> _clientNames` — name → id, the uniqueness gate. `TryAdd` here is
  the atomic "claim this name" operation (`MeshHub.cs:1032`).
- `int _reservedClientSlots` (`MeshHub.cs:109`) — **the counter `maxClients` is actually enforced
  against**, not `_clients.Count`. A slot is claimed by one atomic compare-and-swap
  (`TryReserveClientSlot`, `MeshHub.cs:1296`) during registration and given back by `ReleaseClientSlot`
  (`:1325`) in the handler's `finally`. Read it with `Volatile.Read`; never write it directly. Shutdown
  deliberately does **not** reset it — each still-running handler owns its own claim and returns it
  itself. See [Registration handshake](#registration-handshake-hub-side) and
  [known-issues.md](known-issues.md) KI-26.
- `ConcurrentDictionary<string, Group> _groups` (`StringComparer.Ordinal`) — group name → membership.
  This is the **authoritative** membership set: it gates joins (via the authoriser) and gates group sends.
  The client's own `JoinedGroups` is an optimistic mirror of it, not a second source of truth.
- `ConcurrentDictionary<Task, byte> _handlerTasks` — live per-client handler tasks, awaited on shutdown.
- One `CancellationTokenSource? _cts` (`MeshHub.cs:125`) + `Task? _acceptLoopTask` (`:126`) for the accept
  loop lifecycle, plus `Task? _stopTask`, `Task? _disposeTask`, `bool _starting` and `bool _disposed`.
  **All six are guarded by `Lock _stateLock` (`MeshHub.cs:123`)** and must only be read or written inside
  it — see [Lifecycle & concurrency](#lifecycle).
- `GroupAuthoriser? _groupAuthoriser` (`MeshHub.cs:55`) + `TimeSpan _groupAuthorisationTimeout` (`:56`)
  — the authorisation seam. `null` authoriser means every join is allowed. There is deliberately **no**
  companion semaphore here, unlike the authentication seam; the reasoning is in
  [Group authorisation](#group-authorisation).
- `ClientAuthenticator? _authenticator` + `SemaphoreSlim? _authenticationSlots` — the authentication
  seam. The semaphore is **only allocated when an authenticator was supplied** (`MeshHub.cs:335-339`),
  so an unauthenticated hub does no extra work and allocates nothing.
- `bool _notifyOnQueueSaturation` + `TimeSpan _backpressureAwaitTimeout` (`MeshHub.cs:58-59`, PR #87,
  issue #30) — the backpressure-signalling configuration. Neither gates anything; they only decide
  whether a direct send's sender is told over the wire when its recipient's queue is full
  (`_notifyOnQueueSaturation`) and how long a sender that opted into
  `DeliveryOptions.AwaitCapacity` is parked waiting for room (`_backpressureAwaitTimeout`, default 30 s,
  `DefaultBackpressureAwaitTimeout` at `:47`). See
  [Backpressure signalling](#backpressure-signalling-and-awaiting-capacity) below.
- `IOfflineStore? _offlineStore` + `TimeSpan _offlineStoreTimeout` (PR for issue #28) — the
  store-and-forward seam. **`_offlineStore` being null is the entire "disabled" switch**: every path
  that touches the feature is behind a null check on it, so a hub without one does exactly what it did
  before the feature existed. See [Offline delivery](#offline-delivery) below.
- `ConcurrentDictionary<Guid, string> _offlineNamesById` + `ConcurrentDictionary<string, Guid>
  _offlineIdsByName` — which name last held each id, populated at teardown and **only when a store is
  configured**. Two maps rather than one so a returning name's stale id can be forgotten in constant
  time. Bounded by `MaxClients`.
- `TimeSpan? _sessionResumptionWindow` + `ConcurrentDictionary<string, ResumableSession> _sessions`
  (issue #43) — resumable identities, **keyed by the hex SHA-256 hash of the token that reclaims them**
  rather than by the token, so the table is not a bag of live bearer credentials. Null window = the
  feature does not exist: no token is issued and the table stays empty. Bounded by `MaxClients`, with a
  lazy expiry sweep run only when the table is at that bound. See
  [Session resumption](#session-resumption) below.

`ClientConnection` (nested, `MeshHub.cs:2541`) holds the id, name, transport, a bounded outbound
`Channel<byte[]>` (capacity **1024**, single-reader/multi-writer), an `ActivitySequence` counter
(`Interlocked`-incremented per received frame), the `HashSet<string> Groups` it has joined, and — since
PR #74 (issue #32) — its own `NegotiatedProtocolVersion` (`byte`, captured once as a constructor
parameter at registration, immutable thereafter). This is what closes the gap
[known-issues.md](known-issues.md) KI-14 used to describe: `RouteMessageWithHeaders`/
`SendToGroupWithHeaders` read it per-recipient to pick the outgoing frame shape — see
[Routing helpers](#routing-helpers). Since PR #87 (issue #30) it also carries an internal
`_awaitingCapacityDepth` counter behind the public `IsAwaitingCapacity` (`MeshHub.cs:2588`) and
`BeginAwaitingCapacity()`/`CapacityWaitScope` (`:2593-2610`) — see
[Backpressure signalling](#backpressure-signalling-and-awaiting-capacity) below and
[the heartbeat schedule](#heartbeat-schedule).

<a id="lifecycle"></a>

### Lifecycle & concurrency

`StartAsync`, `StopAsync` and `DisposeAsync` can each be called from a different thread at the same
time. Since PR #64 (issue #12) all three are serialised behind a single `Lock _stateLock`
(`MeshHub.cs:123`) and the whole lifecycle obeys one rule:

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

**`StartAsync` (`MeshHub.cs:446`)** claims the running slot before doing any I/O:

1. Under the lock: throw `ObjectDisposedException` if `_disposed` (`:453`); throw
   `InvalidOperationException` if `_cts`, `_stopTask` or `_starting` says the hub is spoken for
   (`:455-458`); otherwise set `_starting = true` (`:464`).
2. Outside the lock: `await _listener.StartAsync` (`:471`). On failure, release the claim and dispose
   the unused token source (`:473-485`) — a hub whose listener failed to start is startable again.
3. Under the lock again: clear `_starting`, **re-check `_disposed`** (`:494-498`, a disposal may have
   completed while the listener was starting), then publish `_cts` and `_acceptLoopTask`
   **together** (`:504-505`).

> **Why the `_starting` flag rather than publishing `_cts` early.** Publishing the token source before
> the accept loop exists would let a concurrent `StopAsync` take ownership of a hub that had just bound
> its listener and then report itself stopped — leaving the endpoint bound with nothing serving it and
> no way to recover, since the listener cannot be started a second time.

**`StopAsync` (`MeshHub.cs:517`) is not `async`** — it is a plain method returning a `Task`, so its
decision is taken **synchronously** under the lock before the caller gets a task back. Under the lock:
if `_stopTask` is already set, join it; otherwise read `_cts` and, if it is null, return
`Task.CompletedTask` (`:526-529`); otherwise take ownership — capture the token source and accept-loop
task into locals, null both fields, and publish the shutdown in `_stopTask` (`:531-537`). Every caller
then awaits that one task, so **clients are notified once, not once per caller**, and every caller
returns only once the hub has actually stopped.

- A caller's own `cancellationToken` is honoured via `WaitAsync` (`:545`), but **abandoning the wait
  does not cancel the shutdown** — that belongs to the caller which started it.
- The teardown is split in two. `StopCoreAsync` (`:552`) opens with `await Task.Yield()` (`:558`) so
  none of it runs on the caller's stack while the lock is held, then sends the best-effort `Disconnect`
  notification (`:562-572`). `ShutDownAsync` (`:589`) does the shutdown proper: cancel (`:594`), drain
  the accept loop (`:596-606`) and the handler tasks (`:610-618`), clear the four registries
  (`:620-623`), dispose the token source (`:625`).
- **`ShutDownAsync` runs from `StopCoreAsync`'s `finally` (`:575-582`).** That is load-bearing: the
  notification's exception filter covers only `IOException`/`ObjectDisposedException`/
  `OperationCanceledException`, so before this an unfiltered transport exception abandoned the shutdown
  half way — accept loop still running, token source undisposed, hub reporting itself stopped and no
  later call able to put it right.
- `ShutDownAsync`'s own `finally` clears `_stopTask` (`:627-636`), so a shutdown that failed part way
  leaves the hub *stopped* rather than wedged as permanently *stopping*.

**`DisposeAsync` (`MeshHub.cs:709`)** sets `_disposed = true` **first** (`:717`), before any teardown
begins, so a start racing a disposal is refused rather than racing the listener's teardown. It then
memoises its teardown in `_disposeTask` (`:718`); every later or concurrent call awaits that same task.
`DisposeCoreAsync` (`:728`) yields (`:732`) before awaiting `StopAsync` (`:734`), then disposes the
listener (`:735`) and the authentication semaphore (`:736`) — **exactly once**. Disposal is terminal.

> **If you change any of this, keep the shape.** Do not reintroduce a second read of a lifecycle field
> outside the lock; do not await inside the lock; do not move `ShutDownAsync` out of the `finally`; and
> do not make `StopAsync` `async` again — its synchronous decision is what makes the "join the existing
> shutdown" handover race-free, and it is what the tests pin. See
> [testing.md](testing.md#parking-a-caller-mid-lifecycle) for the seams that pin these interleavings.

### The three per-connection tasks

Every accepted connection is handled by `HandleClientAsync` (`MeshHub.cs:924`), which after a successful
handshake spins up:

1. **Receive loop** — the body of `HandleClientAsync` itself: `transport.ReceiveAsync` → dispatch by
   opcode → route. Reads against one long-lived `clientCts` token (no per-frame CTS). Dispatch is
   otherwise synchronous, with **two** branches that await: the lookup response write (`MeshHub.cs:1188`)
   and, when a `groupAuthoriser` is configured, the group join (`MeshHub.cs:1113-1114`). The two
   header-bearing branches added by PR #74 (`SendMessageWithHeaders` → `RouteMessageWithHeaders`,
   `GroupMessageWithHeaders` → `SendToGroupWithHeaders`) are synchronous like their plain counterparts —
   they do not add a third await site. While a lookup or group join is in flight this client's loop reads
   **nothing else from this client** — which is what makes a slow authoriser a per-client problem rather
   than a hub-wide one, and also why a slow authoriser can get the client evicted by the heartbeat monitor
   (see the constructor warning above).
2. **Send loop** — `SendLoopAsync` (`MeshHub.cs:1722`): drains the outbound `Channel`, **dropping an
   already-expired frame before it is ever added to the batch** (PR #85 — see the callout below), then
   **coalescing** whatever remains, up to a 64 KiB byte budget (`SendCoalesceByteBudget`,
   `MeshHub.cs:1621`) into a single batched write when the transport implements `IBatchSendTransport`;
   otherwise sends them one at a time. A lone frame is sent immediately (no latency added). Its `catch`
   also treats an `ArgumentException` as a transport fault (`MeshHub.cs:1791`, alongside the pre-existing
   `IOException`/`ObjectDisposedException`) — see the note below.
3. **Heartbeat monitor** — `MonitorHeartbeatAsync` (`MeshHub.cs:1810`), **only if `heartbeatInterval`
   is set**. One `PeriodicTimer`. Compares `ActivitySequence` between ticks; an interval in which the
   sequence did not move is a **silent interval** and increments a miss counter — **unless the
   connection is currently parked awaiting capacity (PR #87, issue #30)**, in which case the interval is
   treated as liveness and the counter is reset instead (`connection.IsAwaitingCapacity`,
   `MeshHub.cs:1841-1845`, checked **before** the silent-interval branch below) — see
   [Backpressure signalling](#backpressure-signalling-and-awaiting-capacity). Otherwise, the counter is
   **checked before the probe**: on reaching `maxMissedHeartbeats` it cancels the client's CTS to evict
   and returns (`MeshHub.cs:1851-1859`); otherwise it enqueues a `Ping` and loops
   (`MeshHub.cs:1863`). Any frame from the client resets the counter to zero.

All three share `clientCts` (linked to the hub's token). Teardown in the `finally` block
(`MeshHub.cs:1209-1281`) completes the outbound queue, cancels the CTS, awaits the send + heartbeat tasks,
removes the client from all groups and both dictionaries, disposes the connection (which disposes the
transport), and raises `ClientDisconnected`.

> **An oversized delivery frame is a transport fault, not a crash — fixed by PR #74 (issue #32).**
> Combining a received frame's sender id / group name / header block with its body can produce an
> outbound frame larger than a transport is willing to send (`TcpTransport`'s 1 MiB cap, notably) —
> most reachable via a near-maximum body plus a large header block. Before this PR the transport's
> resulting `ArgumentException` was uncaught inside `SendLoopAsync`, which faults the task that
> `HandleClientAsync`'s own `finally` awaits — aborting that `finally` **partway through** and skipping
> the slot release, name removal and group removal that follow, permanently leaking the client's
> registration slot. `SendLoopAsync`'s `catch` now treats `ArgumentException` exactly like a transport
> fault (`MeshHub.cs:1791-1807`), so cleanup runs to completion and only the one oversized message is
> lost. See [known-issues.md](known-issues.md) KI-33.

<a id="dropping-expired-frames"></a>

> **The hub drops an already-expired frame at the moment it is dequeued for sending — PR #85 (issue
> #29).** `IsExpiredFrame(byte[] frame, Guid recipientId)` (`MeshHub.cs:1638-1668`) is called for every
> frame `SendLoopAsync` pulls off the outbound `Channel`, both the first frame of a batch
> (`:1738-1741`) and every subsequent frame drained into the same batch by the coalescing `while`
> (`:1752-1755`); if the coalescing drain empties out entirely because everything it looked at had
> already expired, the loop `continue`s again rather than sending an empty batch (`:1761-1765`). This is
> the companion check to [`MeshClient.IsExpired`](client.md#message-expiry-time-to-live) — a message can
> be dropped by either end, whichever notices the expiry first.
> - **The hub still never decodes a frame's body, and does not decode the header block via the general
>   `HeaderEnvelope.Read` path either.** `TryGetHeaderBlock` (`MeshHub.cs:1675-1720`) locates the header
>   block within a `DeliverMessageWithHeaders` (`0x12`) or `DeliverGroupMessageWithHeaders` (`0x14`) frame
>   by reading only the fixed-offset length-prefix fields, without touching the sender id, group name or
>   body bytes; `IsExpiredFrame` then calls the new `HeaderEnvelope.TryReadValue`
>   (`Messages/HeaderEnvelope.cs:175-233`) to scan that one block for `mesh.expires-at` specifically —
>   **without** allocating the `Dictionary<string, string>` a full `HeaderEnvelope.Read` decode would, a
>   deliberate hot-path optimisation since the vast majority of frames do not carry the key. Any other
>   opcode — including the plain `DeliverMessage`/`DeliverGroupMessage` frames a message with no
>   time-to-live produces — is not recognised by `TryGetHeaderBlock` at all and is therefore never treated
>   as expired.
> - **A malformed header block is treated as "not expired", not as an error.** `TryReadValue` throws
>   `FormatException` on a block that is internally malformed (exactly like `HeaderEnvelope.Read` does);
>   `IsExpiredFrame` catches it and returns `false` (`MeshHub.cs:1654-1657`) rather than letting a
>   hostile or corrupted frame fault the send loop — the recipient's own `TryReadHeaderBlock` decode is
>   what actually surfaces a malformed block (logged and the frame dropped there instead), so nothing is
>   lost twice over by treating it as non-expiring here.
> - **A drop here is counted on the existing `messages.dropped` counter, tagged `reason=expired`**
>   (`ExpiredDropTag`, `MeshHub.cs:98`; incremented at `:1666`) — a fourth reason alongside
>   `unknown-recipient` and `queue-full` (two call sites each, direct and group). See
>   [Metrics](#metrics) below.

<a id="heartbeat-schedule"></a>

> **Heartbeat schedule (know this before tuning):** eviction fires when `missedHeartbeats >=
> _maxMissedHeartbeats` (`MeshHub.cs:1851`), and the check sits **above** the `TryWrite` of the `Ping`
> (`MeshHub.cs:1863`). So "max missed = N" means exactly what it says — a client that sends nothing is
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
> **A connection parked awaiting capacity is exempt from this schedule entirely (PR #87, issue #30).**
> Its receive loop reads nothing while parked — not because the client went silent, but because the hub
> is deliberately not reading it — so an interval spent parked resets the miss counter to zero rather
> than counting as silent, and no `Ping` is sent (`MeshHub.cs:1841-1845`). This is the same hazard class
> the constructor already warns about for a slow `GroupAuthoriser` (a client parked awaiting a join
> decision also looks idle), but here the hub knows exactly when and for how long the loop is parked, so
> it can be precise rather than merely warn. See
> [Backpressure signalling](#backpressure-signalling-and-awaiting-capacity) below.
>
> **`maxMissedHeartbeats: 1` is the sharp edge.** There is no interval left in which a ping could be
> answered, so the hub never probes; a client that only *receives* — and therefore sends no frames of
> its own — is evicted every single interval. The constructor logs a `Warning` when it sees this
> combination (`MeshHub.cs:341-354`) rather than throwing, because it is legal if your clients are
> known to send continuously. Values below 1 throw `ArgumentOutOfRangeException` (`MeshHub.cs:278-282`).
>
> This was previously off by one — eviction on the (N+1)th interval — and was corrected in PR #61
> (issue #9). See [known-issues.md](known-issues.md) KI-11. If you are reading older notes, or a hub
> built before that change, the schedule was one interval longer and the client was probed N times.

### The accept loop

`AcceptLoopAsync` (`MeshHub.cs:740`) loops `listener.AcceptAsync`. `OperationCanceledException` /
`ObjectDisposedException` break the loop (shutdown); **any other exception is logged and swallowed** so
one bad connection cannot kill the hub (`MeshHub.cs:757-765`) — this is the intentional broad catch at a
background-service boundary. Since PR #68 (issue #16), a successfully accepted transport then runs the
[per-remote-endpoint cap check](#per-remote-endpoint-connection-cap) **before** it is handed to
`HandleClientAsync`; the handler task is tracked in `_handlerTasks` and a `ContinueWith` removes it and
logs faults.

> **That two-way split is what makes `ITransportListener`'s disposal contract load-bearing.** The retry
> branch is `continue` with **no delay**, so a listener that is finished but reports itself with anything
> other than `ObjectDisposedException` puts this loop into an unbounded hot spin rather than stopping it.
> Both shipped listeners translate accordingly; a custom one must too. See
> [transport.md](transport.md#itransportlistener--transportitransportlistenercs23) and
> [known-issues.md](known-issues.md) KI-22. **The per-remote-endpoint refusal path is a third `continue`**
> (`MeshHub.cs:781`) alongside the accept-failure retry — it is not part of the two-way
> finished-vs-failed split above, since it fires on a connection that was accepted successfully but is
> then refused for capacity, not on an `AcceptAsync` failure.

<a id="per-remote-endpoint-connection-cap"></a>

### Per-remote-endpoint connection cap

Added by PR #68 (issue #16), to close the gap `maxClients` leaves open: `maxClients` only bounds
**registered** clients, so a flood of connections that never complete the handshake — deliberately, to
exhaust handler tasks, sockets and outbound queues — sails straight past it. This cap is checked in
`AcceptLoopAsync`, **before** a handler task is even created, and covers the whole connection lifetime
including the pre-registration window.

- **`ExtractRemoteAddress`** (`MeshHub.cs:809`) reads the transport's remote address, but only if it
  implements [`IRemoteEndPointTransport`](transport.md#iremoteendpointtransport-public--transportiremoteendpointtransportcs16)
  and reports an `IPEndPoint` — a transport that doesn't (the in-memory transport, or a custom one that
  hasn't implemented the interface) is **never capped** by this. `TcpTransport` is the only shipped
  implementer.
- **The address is masked before use.** `NormaliseForEndpointCap` (`MeshHub.cs:833-848`) returns IPv4
  addresses unchanged but reduces an IPv6 address to its `/64` network prefix (`IPv6CapPrefixLength`,
  `MeshHub.cs:819`), zeroing the low 8 bytes (the interface identifier). **This is load-bearing, not a
  tidy-up:** a single host is routinely handed an entire `/64` by its ISP or cloud provider, and without
  the mask a source could defeat the cap by connecting from a fresh address within that `/64` every time
  — each one would look like a brand-new, never-before-seen source under a full-address key.
- **The claim is a CAS loop against a `ConcurrentDictionary<IPAddress, int>`** (`_connectionsByRemoteAddress`,
  `MeshHub.cs:66`), mirroring `TryReserveClientSlot`/`ReleaseClientSlot`:
  `TryReserveEndpointSlot` (`:875-890`) only ever claims from a count still under
  `maxConnectionsPerRemoteEndpoint` at the instant of the claim, and `ReleaseEndpointSlot` (`:901-922`)
  removes the dictionary entry once its count reaches zero rather than leaving a zero-valued entry
  behind — a long-lived hub does not accumulate one entry per distinct address it has ever seen.
- **A refusal disposes the transport immediately and reads nothing from it**
  (`DisposeRefusedTransportAsync`, `MeshHub.cs:853-864`) — no registration frame is ever received from a
  refused connection, so a flood cannot force any parsing work at all, not even a malformed-frame check.
- **The slot is released in `HandleClientAsync`'s `finally`** (`MeshHub.cs:1263-1269`), unconditionally
  whenever a remote address was captured — regardless of whether registration ever completed. This is
  the same release-in-`finally` discipline as the client-slot claim; see
  [Registration handshake](#registration-handshake-hub-side) below.

**Gotchas:**
- **This is a connection cap, not a client cap.** It is enforced in the accept loop, independently of
  `maxClients`, and a connection that is refused here never reaches the registration handshake at all —
  it gets no `Error(HubAtCapacity)` frame, just an immediate close, because the hub hasn't read anything
  from it yet to reply on.
- **Only a transport that reports its remote address is covered.** Do not assume this cap protects a
  deployment using a custom transport unless that transport implements `IRemoteEndPointTransport`.
- **The default (100) is per address, not global** — `maxClients` (default 1000) is still the only
  global ceiling. A hub facing many distinct sources can still be driven to its `maxClients` limit by a
  wide-enough flood; this cap only stops a **single** source from doing it alone.

See [known-issues.md](known-issues.md) KI-29 and [testing.md](testing.md#per-remote-endpoint-connection-cap).

### Registration handshake (hub side)

Inside `HandleClientAsync` (`MeshHub.cs:934-1050`), in order. The frame layout is
`[type][versionMin][versionMax][name length (2, big-endian)][name][credential]` — see
[protocol.md](protocol.md#registration-handshake).

1. Receive one frame under a **registration-timeout** linked CTS. Timeout → drop silently (`:937-952`).
2. Validate: frame must be ≥ **3** bytes and opcode `RegistrationRequest` (`0x04`) — else drop (`:956-961`).
3. **Negotiate a protocol version.** `TryNegotiateProtocolVersion(registrationData[1], registrationData[2],
   out negotiatedVersion)` (`:963`, implementation at `:1340-1362`) intersects the client's advertised
   `[versionMin, versionMax]` with the hub's own `[Protocol.MinSupportedVersion,
   Protocol.MaxSupportedVersion]` and, on overlap, picks the **highest** common version. An inverted
   client range or no overlap → `Error(UnsupportedProtocolVersion)` and drop (`:963-969`). This replaced a
   single-byte `Protocol.Version` equality check in PR #73 (issue #47) — see
   [protocol.md](protocol.md#versioning) and [known-issues.md](known-issues.md) KI-14.
4. Frame must be ≥ 5 bytes, then read the `ushort` name length at offset 3. A **zero** length, or one
   that runs past the payload, is malformed → **drop silently, no error frame** (`:971-984`). Decode the
   name from `[5, 5+len)` (`:986`).
5. If `name.Length > 256` (chars) send `Error(ClientNameTooLong)` and drop (`:988-994`).
6. **If an authenticator is configured** (`:996-1017`) — and only then — two things happen, in this order:
   first an at-capacity **early-out**, `Volatile.Read(ref _reservedClientSlots) >= maxClients` →
   `Error(HubAtCapacity)` and drop *without* running the authenticator (`:1003-1007`); then the callback
   itself (`:1009-1016`, see [Authentication](#authentication)), anything other than `true` →
   `Error(AuthenticationFailed)` and drop. With no authenticator neither happens and the handler falls
   straight through to step 7.
7. **Claim a client slot.** `TryReserveClientSlot()` (`:1024-1028`, implementation at `:1255`)
   compare-and-swaps `_reservedClientSlots` up by one if and only if it is still below `maxClients`.
   **This single atomic operation is the binding capacity decision**; it fails →
   `Error(HubAtCapacity)` and drop. On success the handler sets `slotReserved = true` (`:1030`), which
   arms the matching `ReleaseClientSlot()` in its `finally` (`:1217-1220`).
8. `_clientNames.TryAdd(name, id)` — if it fails, name is taken → `Error(DuplicateClientName)` and drop
   (`:1032-1037`). The slot claimed at step 7 is given back on the way out by the `finally`.
9. Create `ClientConnection`, add to `_clients`, send `RegistrationComplete` — assigned 16-byte id plus
   the `negotiatedVersion` byte from step 3, plus (issue #43) a resumption token when
   `sessionResumptionWindow` is set and the negotiated version is 6 or higher — raise `ClientConnected`,
   start the send loop (+ heartbeat monitor), drain the offline store (issue #28), enter the receive
   loop. The reply is byte-identical to the pre-#43 18-byte frame whenever no token is issued.

The assigned id is a fresh `Guid.NewGuid()` generated at handler start (`MeshHub.cs:927`).

> **The ordering of steps 6–8 is deliberate and load-bearing, and it is not the obvious ordering.** The
> *binding* capacity decision (step 7) sits **after** authentication; the cheap early-out inside step 6
> sits **before** it. Three invariants to preserve if you touch this method:
>
> - **A full hub must not run the authenticator.** That is the early-out at `:1003-1007`, and it is only an
>   early-out — it does not decide admission. Removing it would not soften the cap, but it would reopen
>   the case the check exists to shed: a connection flood driving credential checks, or pinning handler
>   tasks on a slow authenticator, against a hub with nothing left to admit them to.
> - **A refused client must not claim a name.** `_clientNames.TryAdd` stays last, after both the
>   authenticator and the slot claim, so a client refused for either reason never reserves a name.
> - **The claim must not move ahead of authentication, and the claim/release pairing must stay intact.**
>   The claim is taken after the authenticator returns precisely so an unauthenticated peer cannot hold
>   capacity away from one that would authenticate. Every successful `TryReserveClientSlot()` is owned by
>   exactly one handler and must be given back exactly once, by that handler's `finally` (`:1217-1220`) —
>   including on the duplicate-name path. A claim that escapes its release leaks capacity for the
>   lifetime of the hub. See [known-issues.md](known-issues.md) KI-26.
>
> **Negotiation (step 3) picks a version, and — since PR #74 (issue #32) — something does branch on the
> result.** `negotiatedVersion` still flows into the `RegistrationComplete` reply exactly as before, but
> it is also stored on the `ClientConnection` (`NegotiatedProtocolVersion`, `MeshHub.cs:2541`) and read
> per-recipient by `RouteMessageWithHeaders`/`SendToGroupWithHeaders` to choose between the header-bearing
> frame and the plain, stripped one — see [Routing helpers](#routing-helpers). This was the gap
> [known-issues.md](known-issues.md) KI-14 used to describe; it is now resolved for the header envelope
> specifically. The general lesson still holds for the *next* capability: widening the range again does
> not by itself make anything conditional — whoever does it must add the branch, the way this PR did.

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

`AuthenticateAsync` (`MeshHub.cs:1405-1483`) wraps the callback with four protections, all of which exist
because **the callback runs on unauthenticated input, once per accepted connection**:

| Protection | Mechanism | Source |
|---|---|---|
| Concurrency cap | `SemaphoreSlim` of `maxConcurrentAuthentications` (default **64**) permits; a connection that cannot get a slot within `registrationTimeout` is refused | `MeshHub.cs:1417-1425` |
| Time bound | the callback's `ValueTask` is `WaitAsync(_registrationTimeout)`-ed, so a hanging callback cannot pin the handler task or its connection | `MeshHub.cs:1440-1452` |
| Throw isolation | any exception is logged and becomes a refusal rather than faulting the handler (callback boundary) | `MeshHub.cs:1463-1469` |
| Cancellation isolation | an `OperationCanceledException` raised *inside* the callback (e.g. an identity-provider call timing out) — as opposed to hub shutdown — becomes a logged refusal, not a silent drop | `MeshHub.cs:1453-1462` |

Every one of those paths results in the client receiving `Error(AuthenticationFailed)` and the connection
being dropped. **The client cannot distinguish a bad credential from a slow, throwing or overloaded
authenticator** — that is deliberate (it leaks nothing) but it makes hub-side logs the only diagnostic.

The credential is **copied out** of the inbound registration buffer before the context is built
(`MeshHub.cs:1431`), so `RegistrationContext.Credential` does not alias the larger frame. The XML doc on
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
  throws `ArgumentOutOfRangeException` (`MeshHub.cs:284-289`).

### Routing helpers

| Method | Opcode in | Behaviour | Source |
|---|---|---|---|
| `RouteMessage` | `SendMessage` | Look up recipient; build `DeliverMessage`; `TryWrite` to its queue. Unknown recipient → offered to the offline store if one is configured (see [Offline delivery](#offline-delivery)), else logged `Debug`, **dropped**. **`async Task` since issue #28** — deliberately still named without the `Async` suffix, to match its sibling below; nothing on the delivered path awaits, so a connected recipient returns the cached completed task. Full queue → logged `Warning`, **dropped**, raises `QueueSaturated`, and — since PR #87 (issue #30) — best-effort sends the sender the `0x15 QueueSaturated` wire frame if the hub was constructed with `notifyOnQueueSaturation`. | `MeshHub.cs:1872` |
| `RouteMessageWithHeaders` | `SendMessageWithHeaders` | As `RouteMessage` — including the offline-store offer on an unknown recipient, which carries the header block through so it survives the wait — but the outgoing frame shape is chosen from **the recipient's own** `NegotiatedProtocolVersion`: `DeliverMessageWithHeaders` (header block forwarded unchanged) if `>= Protocol.HeaderEnvelopeMinVersion`, else the plain `DeliverMessage` with the header block stripped. The header block is never decoded beyond one well-known key. **`async` since PR #87** (issue #30): if the initial `TryWrite` fails and the sender set `BackpressureHeaderKeys.AwaitCapacity` (via `DeliveryOptions.AwaitCapacity`), it awaits free capacity on the recipient's queue, bounded by `backpressureAwaitTimeout`, before falling back to the same drop/notify/`QueueSaturated` path as `RouteMessage`. | `MeshHub.cs:1924` |
| `BroadcastMessage` | `BroadcastMessage` | Build one shared `DeliverMessage` frame; `TryWrite` to every client **except the sender**. Each full-queue drop raises `QueueSaturated`, but — deliberately, since PR #87 — sends **no** wire frame: the dropped recipient's id comes from the hub's own client registry, not from the sender, so echoing it back would let a sender enumerate every connected client's id by broadcasting until somebody's queue filled. | `MeshHub.cs:2021` |
| `JoinGroupAsync` | `JoinGroup` | **`async`, and awaited by the receive loop.** Empty name ignored. With a `groupAuthoriser`: copies the inbound name bytes, asks the authoriser, and on refusal calls `LeaveGroup` then `RefuseGroupJoin`. Otherwise, or on approval, calls `AddToGroup`. | `MeshHub.cs:2079` |
| `AddToGroup` | — | The former `JoinGroup` body: `GetOrAdd` the `Group`, add member under its lock. Retries if the group was concurrently removed (`Removed` flag). | `MeshHub.cs:2255` |
| `AuthoriseGroupJoinAsync` | — | Invokes the authoriser behind a `WaitAsync(_groupAuthorisationTimeout)`, with a sync fast path. Refuses on `false`, throw, self-cancellation or timeout. | `MeshHub.cs:2145` |
| `RefuseGroupJoin` | — | Builds `[0x10][echoed name bytes]` and `TryWrite`s it to the client's own queue. Full queue → logged `Warning`, **dropped**. | `MeshHub.cs:2232` |
| `LeaveGroup` | `LeaveGroup` | Remove member; if the group is now empty, mark `Removed` and drop it from `_groups`. Also called by `JoinGroupAsync` on refusal and by `RemoveFromAllGroups` (`:2291`) at teardown. | `MeshHub.cs:2278` |
| `SendToGroup` | `GroupMessage` | **Requires the sender to be a member.** Tests `group.Members.Contains(senderId)` *inside* the group lock (`:2341`); a non-member is logged `Debug` and **dropped** (`:2351-2357`). A member snapshots the ids, then one shared `DeliverGroupMessage` frame is `TryWrite`n to each member **except the sender**. A full-queue drop raises `QueueSaturated` only — same reasoning as `BroadcastMessage` above, since the recipient's id comes from the group's own membership set, not the sender. | `MeshHub.cs:2321` |
| `SendToGroupWithHeaders` | `GroupMessageWithHeaders` | As `SendToGroup`, but each recipient's own `NegotiatedProtocolVersion` picks its frame shape, exactly like `RouteMessageWithHeaders`. At most **two** shared frames are built regardless of group size — one with the header block, one without — each lazily built (`??=`) only if some member actually needs that shape. A full-queue drop raises `QueueSaturated` only, same as `SendToGroup`. **Does not** honour `DeliveryOptions.AwaitCapacity` — only the two direct-send paths above do. | `MeshHub.cs:2416` |

**Shared delivery frames:** broadcast and group sends allocate the delivery buffer **once** (or, for the
two header-bearing group/direct paths, once per distinct frame shape actually needed) and hand the same
`byte[]` to every recipient's queue that shape applies to. Send loops only read it, so concurrent reads
of the never-mutated buffer are safe. Do not mutate a delivery frame after it is built.

**The membership test is inside the lock, and that placement is deliberate.** `SendToGroup` (and
`SendToGroupWithHeaders`, which shares the same snapshot logic) sets `recipients` to `null` when the
sender is not a member and takes the logging branch *after* releasing the lock (`MeshHub.cs:2338-2357`)
— testing against the live `Members` set rather than scanning the snapshot afterwards means a sender
removed from the group cannot slip a message through the gap between the test and the copy. Keep the
test where it is; do not "simplify" it into a post-snapshot check.

Note the empty-group early return is **gone**: a group with no members can no longer be reached by a
non-member send anyway, and a lone member sending to itself still short-circuits further down
(`MeshHub.cs:2360-2364`). A send to a group that does not exist at all still returns at the
`_groups.TryGetValue` miss (`MeshHub.cs:2327`).

<a id="backpressure-signalling-and-awaiting-capacity"></a>

### Backpressure signalling and awaiting capacity

Added by PR #87 (issue #30). Before this, a full outbound queue dropped a frame with nothing but a log
line (`known-issues.md` KI-1) — the sender had no way to know, in-process or over the wire, that its
message never queued. Three independent, separately opt-in pieces now sit on top of the same drop:

1. **`QueueSaturated` (in-process, always on).** Every one of the five queue-full drop sites in the
   [Routing helpers](#routing-helpers) table above calls `RaiseQueueSaturated(senderId, recipientId)`
   (`MeshHub.cs:1517-1534`), which raises the `QueueSaturated` event with both ids. This costs nothing to
   opt into — it fires whether or not anyone is listening, and regardless of the constructor's
   `notifyOnQueueSaturation` flag. It is the only one of the three that ever sees a **fan-out** drop
   (`BroadcastMessage`/`SendToGroup`/`SendToGroupWithHeaders`).
2. **The `0x15 QueueSaturated` wire frame (opt-in via `notifyOnQueueSaturation`, direct sends only).**
   `NotifySenderOfQueueSaturation(senderId, recipientId)` (`MeshHub.cs:1555-1576`) always raises
   `QueueSaturated` first, then — only when the hub was constructed with `notifyOnQueueSaturation: true`
   — best-effort `TryWrite`s a `[0x15][recipientId(16)]` frame onto the **sender's own** outbound queue.
   **Only `RouteMessage` and `RouteMessageWithHeaders` ever call this** — the three fan-out methods call
   `RaiseQueueSaturated` directly instead and send no frame at all. This asymmetry is deliberate and
   security-motivated, not an oversight: a direct send's `recipientId` was supplied by the sender itself,
   so telling it back discloses nothing new; a fan-out's dropped recipient comes from the hub's own
   client or group registry, and any client can broadcast or (if a member) address a group, so echoing
   the id back would let a sender enumerate every other connected client's identity simply by
   broadcasting until somebody's queue happened to be full — an identity census the name-based lookup
   deliberately does not offer (that requires already knowing the name first). The notification is
   itself best-effort: if the sender's own queue is full, the notification frame is silently dropped
   rather than retried or escalated. `MeshClient` surfaces it as `SendRejected` — see
   [client.md](client.md#backpressure-signalling).
3. **`DeliveryOptions.AwaitCapacity` (opt-in per send, direct sends only).** A sender can ask the hub to
   wait for room instead of dropping immediately, via `DeliveryOptions.AwaitingCapacity()` or
   `.WithAwaitCapacity()` on an existing options value — see
   [client.md](client.md#backpressure-signalling). This travels as a new reserved `MessageHeaders` key,
   `mesh.await-capacity` (`Messages/BackpressureHeaderKeys.cs`), checked by `WantsAwaitCapacity`
   (`MeshHub.cs:1979-1990`) without decoding anything else in the header block — mirroring
   `IsExpiredFrame`'s single-key scan (PR #85). Only `RouteMessageWithHeaders` honours it (it is the one
   routing method that is `async` and has a single, unambiguous recipient); `RouteMessage` (the
   header-less overload) and the three fan-out methods never do.

**`TryAwaitCapacityAsync` (`MeshHub.cs:1587-1617`) is where the wait actually happens, and it has two
consequences worth knowing before touching it:**

- **It parks the sender's own receive loop, not just the one message.** `sender.BeginAwaitingCapacity()`
  marks the connection via a `using ClientConnection.CapacityWaitScope`, so `RouteMessageWithHeaders`
  does not return to `HandleClientAsync`'s dispatch loop until the wait resolves. Because frames from one
  connection are read and routed in order, **every other message that same sender addresses to any other
  recipient — however healthy — queues up behind the one being awaited**, for up to
  `backpressureAwaitTimeout` (default 30 s). This is head-of-line blocking at the sender, not the hub:
  other clients' traffic is unaffected.
- **The park is bounded by `backpressureAwaitTimeout` and exempt from heartbeat eviction while it
  lasts** — see [the heartbeat schedule](#heartbeat-schedule) above. On timeout, or if the recipient
  disconnects mid-wait (`ChannelClosedException`), `TryAwaitCapacityAsync` returns `false` and
  `RouteMessageWithHeaders` falls back to the ordinary drop/notify/`QueueSaturated` path — the caller
  gets no distinct signal for "timed out waiting" versus "queue was already full and stayed that way".

**Interaction with `DeliveryOptions.RequireAck`:** the two flags combine (`WithAwaitCapacity()` on a
`RequireAck` options value), but their timeouts are independent and enforced by different parties — the
acknowledgement timeout by the sending `MeshClient`, the capacity wait by this hub. An acknowledgement
timeout shorter than `backpressureAwaitTimeout` can report the send as failed while the hub is still
waiting; if the wait then succeeds, the message is delivered anyway, and a caller that retries on the
acknowledgement timeout would duplicate it. See [known-issues.md](known-issues.md) for the full
register entry, and [client.md](client.md#backpressure-signalling) for the client-side contract.

<a id="session-resumption"></a>

### Session resumption

Added for issue #43, and the fix for the cliff [Offline delivery](#offline-delivery) leaves behind
(KI-50): every `ConnectAsync` used to mint a fresh `Guid`, so after a drop every peer holding the old
one was addressing nothing, and the client's group memberships were gone. Setting
`sessionResumptionWindow` lets a returning client reclaim both.

**The whole feature is behind `_sessionResumptionWindow is not null`.** With it off no token is minted,
`RegistrationComplete` is the same 18 bytes it always was, and the two resume opcodes are refused.

**The exchange is post-registration and that is not a style choice** — see
[protocol.md](protocol.md#session-resumption) for why a token in the registration frame would have been
misparsed as credential bytes by any older hub. The consequences for this file: the resume arrives as
an ordinary frame in the client's dispatch ladder, *after* the connection is fully registered, and its
handler reassigns `HandleClientAsync`'s own `clientId` local so everything downstream — the sender id on
routed frames, and the registry keys the `finally` removes — follows the reclaimed identity.

**Four methods, in lifecycle order:**

1. **`IssueSessionToken(connection)`** — at registration. 32 bytes from `RandomNumberGenerator`;
   only `HashSessionToken` of it is retained, on the connection (`SessionTokenHash`) and as the
   `_sessions` key. Returns null (no token, 18-byte reply) when the feature is off, the connection
   negotiated below `SessionResumptionMinVersion`, or the table is full — **refusing rather than
   evicting**, since a client with no token simply cannot resume, whereas eviction would silently break
   a resumption somebody else is entitled to.
2. **`MakeSessionDormant(connection)`** — in `HandleClientAsync`'s `finally`, **before
   `RemoveFromAllGroups`**, which is what empties the group set it has to capture. Get that order wrong
   and resumption restores nothing, silently. Stamps `DormantUntil`, which is what makes the session
   resumable at all.
3. **`ResumeSessionAsync(connection, token, ct)`** — validates, then rebinds. The ordering of the swap
   is deliberate: publish under the reclaimed id, `Rebind`, update `_clientNames`, *then* withdraw the
   fresh id — so a peer addressing either reaches the connection throughout, rather than falling into a
   gap where neither resolves. Both resolving for an instant is harmless; neither is not.
4. **`RestoreGroupMembershipAsync(connection, groups, ct)`** — **re-authorises, never reinstates.**

> **The re-authorisation is the load-bearing part, and it is why this is not a two-line state restore.**
> `JoinGroup_AfterReconnect_IsAuthorisedAgainRatherThanRestored` pins that a reconnect cannot bypass a
> `GroupAuthoriser` — `MeshClientReconnector` restores membership by re-joining over the wire precisely
> so it goes through the decision again. Resumption is exactly the sort of state-reinstating shortcut
> that would have become the back door around that rule, so it asks the authoriser for every group it
> restores and drops the ones now refused. A refused group gets **no `GroupJoinRefused` frame**: the
> client did not ask to join anything on this connection, and re-encoding a stored name to echo it
> could not preserve the size property that frame's echo depends on (see
> [protocol.md](protocol.md#message-headers)).

**Validation rules, and the reasoning behind the two that are easy to get wrong:**

- **`TryGetValue` to validate, `TryRemove` to claim** — in that order. Claiming first would let anyone
  presenting a token burn the session even when validation then fails, so a token that is (say) still
  held by a live connection would destroy the resumption its rightful owner is entitled to. The winning
  `TryRemove` is separately what makes two connections racing the same token resolve to exactly one
  resumption.
- **The session's name must match the resuming connection's registered name.** Without it, any token
  holder could take over any identity. With it, a token is only as strong as the name it was issued to —
  which, on a hub with no `ClientAuthenticator`, is self-asserted; see
  [known-issues.md](known-issues.md) KI-52 for the trust model in full.
- Dormant only (a live session is never resumable), within the window, and the reclaimed id must not
  currently be in `_clients`.

**Gotchas:**
- **`ClientConnection.Id` is mutable for this one purpose.** Only the connection's own receive loop
  writes it, while dispatching the resume; routing only ever looks the connection up by whichever id it
  holds and reaches the same object either way.
- **The offline store's drain is not part of resumption** and needs no integration with it: the store is
  keyed by name, so registration has already drained it before the resume frame arrives.
- **`StopAsync` clears `_sessions`** along with the registries — a session only means anything on the hub
  that issued its token, and a stopped hub should not still be holding material that reclaims identities
  on it.

<a id="offline-delivery"></a>

### Offline delivery (store and forward)

Added for issue #28. Before it, a direct message to a recipient the hub had no entry for was dropped
with a `Debug` log — fine for a mesh where everything is online at once, lossy for an intermittently
connected one. Supplying an `IOfflineStore` holds those messages against the recipient's **name** and
delivers them when that name registers again.

**The whole feature is behind `_offlineStore is not null`.** A hub with no store does not retain
identities, does not build an `OfflineMessage`, and does not await anything it did not await before.

**Three pieces, in the order a message meets them:**

1. **`RetainOfflineIdentity(id, name)`** — called from `HandleClientAsync`'s `finally`, **after** the
   connection is out of `_clients` and `_clientNames`. That ordering is load-bearing: a sender racing
   the teardown either finds the recipient still connected and routes to it, or finds it gone and
   resolves the id to a name — it can never see both at once. Bounded by `MaxClients`; once the
   retention table is full, further disconnects are simply not retained rather than evicting an
   identity that may be about to be reclaimed.
2. **`TryStoreForOfflineDeliveryAsync(senderId, recipientId, headerBlock, body, ct)`** — called from
   both direct-routing methods' unknown-recipient branch, and only there. It resolves the id to a name,
   copies both byte ranges out of the receive buffer (the buffer is reused; a store may hold them for
   minutes), and offers the result to the store. Returns whether the offline path took ownership: a
   store that *refuses* still counts as owned, because the refusal is counted here as
   `reason=offline-queue-full` rather than falling through to `unknown-recipient`.
3. **`DeliverStoredMessagesAsync(connection, ct)`** — called once per registration, **after** the send
   loop is started and **before** the receive loop begins. After, so held frames are written out as
   they are queued rather than waiting for the client's first frame; before, so anything sent to the
   client from that moment queues behind what it missed.

**A stale id stops resolving the moment its name comes back.** `ForgetOfflineIdentity(name)` runs at
registration, and again lazily from `TryStoreForOfflineDeliveryAsync` if it finds the name in
`_clientNames`. Without this, a peer holding the pre-disconnect id would go on filling a queue that only
the *next* reconnect would drain, while the recipient sat connected — strictly worse than the ordinary
unknown-recipient drop, which at least tells the truth. **Issue #43 closes this gap** — see
[Session resumption](#session-resumption): a resumed client keeps its id, so a peer's cached id stays
correct across the reconnect and the stale-id problem never arises. It only closes it for hubs that
*enable* resumption and clients that reconnect inside the window; with resumption off, KI-50 stands.

**Frame shape is decided at drain time, not at storage time**, which is why `OfflineMessage` holds
`(senderId, headerBlock, body)` rather than a built frame — the shape depends on the version the
*returning* connection negotiates, which is unknowable when the message is stored. The drain reuses
`BuildDeliverMessage`/`BuildDeliverMessageWithHeaders` and `ReadPriority`, so a held message lands on
the same lane, in the same shape, as it would have live.

**Gotchas:**
- **Direct sends only.** `BroadcastMessage`, `SendToGroup` and `SendToGroupWithHeaders` never store —
  a disconnected client has already been removed from every group, so there is nothing to fan out to.
- **The store is on a live connection's path**, called from a sender's receive loop and from a
  registration. Both calls are bounded by `offlineStoreTimeout` (default 10 s) for the same reason
  every other integrator callback here is bounded: a parked receive loop looks idle to the heartbeat
  monitor, which would evict the very client the store exists to serve.
- **Callback boundary.** A throwing store is logged and treated as a refusal on the store path, and as
  "nothing held" on the drain path — it never faults a receive loop or fails a registration.
- **A drain can overflow the outbound queue** (1024 frames). Overflow is dropped and counted
  `queue-full`, and the drain carries on through the rest rather than abandoning it — the send loop is
  already running concurrently, so a later message may still fit.
- **Per-message expiry composes with it for free.** A held frame carrying `mesh.expires-at` that
  lapsed while stored is dropped by `SendLoopAsync`'s existing `IsExpiredFrame` check on the way out —
  see [dropping expired frames](#dropping-expired-frames).

### Group locking model

Each `Group` (`MeshHub.cs:2534`) has its own `Lock`, a `HashSet<Guid> Members`, and a `bool Removed`.
Distinct groups route in parallel; only same-group mutation contends. The `Removed` flag closes a
join/remove race: when the last member leaves, the group is marked removed **under its lock** and taken
out of `_groups` only if that exact instance is still mapped (`TryRemove(KeyValuePair)`,
`MeshHub.cs:2316`). A concurrent `AddToGroup` that fetched the dying instance sees `Removed` and retries
against a fresh one (`MeshHub.cs:2259-2267`). A connection's own `Groups` set is only touched by its
receive loop and teardown, so it needs no lock.

**The authoriser runs *outside* the group lock, and must.** `JoinGroupAsync` awaits the decision before
it ever reaches `AddToGroup` (`MeshHub.cs:2106`), so no integrator callback is ever invoked while a
`Group.Lock` is held. The send-side membership test, by contrast, is *inside* the lock — see
[Routing helpers](#routing-helpers). If you add anything to this path, keep awaits out of the lock.

<a id="group-authorisation"></a>

### Group authorisation

Groups are the hub's only **enforceable** boundary. Two separate rules, one unconditional and one opt-in:

| Rule | Applies | Enforced at |
|---|---|---|
| **A group send requires membership of that group** | always | `SendToGroup`/`SendToGroupWithHeaders`, `MeshHub.cs:2341` |
| **A join must be authorised** | only when a `GroupAuthoriser` is supplied | `JoinGroupAsync`, `MeshHub.cs:2090-2118` |

**With no `groupAuthoriser` (the default) the hub authorises no joins and any client may join any
group** — groups are then a routing convenience, not isolation. The send-side rule still holds, so a
client that never joined still cannot inject into a group. See
[known-issues.md](known-issues.md) KI-2.

`AuthoriseGroupJoinAsync` (`MeshHub.cs:2145-2226`) wraps the callback with three protections. Compare it
with the four in [Authentication](#authentication) above — the **missing** one is the point:

| Protection | Mechanism | Source |
|---|---|---|
| Time bound | `WaitAsync(_groupAuthorisationTimeout)` (default **10 s**), with a sync fast path that skips the `AsTask()` entirely | `MeshHub.cs:2169-2180` |
| Throw isolation | any exception is logged at `Error` and becomes a refusal rather than faulting the receive loop (callback boundary) | `MeshHub.cs:2216-2225` |
| Cancellation isolation | an `OperationCanceledException` raised *inside* the callback — as opposed to the client disconnecting or the hub shutting down — becomes a logged refusal, not a dropped connection | `MeshHub.cs:2203-2215` |
| ~~Concurrency cap~~ | **deliberately absent.** The callback runs on input from an *already-admitted* client and is driven from that client's own receive loop, which reads nothing further from it until the callback returns — so one client cannot have two decisions in flight. The authentication semaphore exists because that callback runs on **un**authenticated input, where any peer reaching the port can drive it; this one is not in that position. | comment at `MeshHub.cs:2157-2168` |

Every refusal path results in a `GroupJoinRefused` frame to the client and **no** membership. The client
cannot distinguish a policy `false` from a throwing, cancelling or slow authoriser — as with
authentication, that is deliberate, and hub-side logs are the only diagnostic (`Warning` for refusal,
timeout and cancellation; `Error` for a throw).

**A refusal revokes.** Before replying, `JoinGroupAsync` calls `LeaveGroup` (`MeshHub.cs:2114`). This
matters because re-joining a group you are already in is legal and idempotent: if the authoriser has
since changed its mind, leaving the existing membership in place would mean a deliberate "no" left the
client still receiving that group's traffic and still entitled to send to it. Removing here also keeps
the two sides in step, since the client drops the group when it sees the refusal.

**Every join is authorised on its own merits, including reconnects.** `MeshClientReconnector` restores
membership by re-joining over the wire rather than by reinstating state, so a restore runs through this
same path and cannot resurrect a membership you would now refuse. A refusal is **not** retried, so an
authoriser that fails closed on a transient outage costs the client its membership until something asks
again.

```csharp
// README, "Security" section
GroupAuthoriser groupAuthoriser = (context, _) =>
    ValueTask.FromResult(TenantDirectory.MayJoin(context.ClientName, context.GroupName));

await using var hub = new MeshHub(
    logger, listener, authenticator: authenticator, groupAuthoriser: groupAuthoriser);
```

Gotchas when writing a group authoriser:

- **`context.ClientName` is only as strong as your `ClientAuthenticator`.** With no authenticator the
  name is self-asserted. Authorise on a name you have actually authenticated.
- **`context.GroupName` is untrusted and unbounded.** Match it against known groups; do not parse
  structure out of it. See [known-issues.md](known-issues.md) KI-8.
- **`context.ClientId` is per connection**, not per client — a reconnect brings a new one.
- **Bound your own concurrency.** The timeout bounds how long the *hub waits*, not how long your
  callback *runs*. An abandoned callback carries on executing, and a mass reconnect re-joins every group
  at once. [known-issues.md](known-issues.md) KI-28.
- **Keep the timeout below `heartbeatInterval × maxMissedHeartbeats`** or a slow-but-working authoriser
  gets its client evicted mid-decision. The constructor warns; see
  [Using it efficiently](#using-it-efficiently).
- **Do not throw to signal refusal** — same reasoning as the authenticator: it works, but logs at
  `Error` and costs an exception per rejected join, and joins recur across a connection's life.
- **Group names are clipped to 64 characters in the hub's log lines** (`MaxLoggedGroupNameLength` /
  `ForLog`, `MeshHub.cs:2128`, `:2134-2139`). The refusal paths log at `Warning`/`Error` and are
  reachable at will by any admitted client, so an unclipped name would let one client choose how much
  the hub writes. If you add a log line on this path, run the name through `ForLog`.

---

<a id="metrics"></a>

### Metrics

Added by PR #72 (issue #24). `MeshHub` owns one `System.Diagnostics.Metrics.Meter` per instance
(`_meter`, `MeshHub.cs:82`), created in the constructor (`:395`) and disposed in `DisposeCoreAsync`
(`:737`) — so a torn-down hub's instruments stop reporting rather than going on publishing stale values
for a resource that no longer exists. The meter is named via the shared internal constant
`MeshworxMeterName.Value` (`"AdamSalisbury.Meshworx"`, `Diagnostics/MeshworxMeterName.cs:15`) —
`MeshClientReconnector` names its own meter the same way (see [client.md](client.md#metrics)) — so one
`AddMeter("AdamSalisbury.Meshworx")` call on an OpenTelemetry `MeterProvider` collects every Meshworx
instrument regardless of which component recorded it. `GetMeterForTesting()` (`:698`) exposes the
instance itself, `internal` rather than `private`, so a test can filter a `MeterListener` down to exactly
one hub's instruments by reference rather than by meter name alone (several hubs in a test run share the
name). See `MetricsCapture<T>` in [testing.md](testing.md#metrics-tests).

| Instrument | Kind | Tags | Source |
|---|---|---|---|
| `meshworx.hub.clients.connected` | `UpDownCounter<int>` | — | created `:396`; `+1` on registration (`:1041`), `-1` on removal (`:1249`) |
| `meshworx.hub.messages.routed` | `Counter<long>` | `direction`: `direct` / `broadcast` / `group` | created `:400`; recorded in `RouteMessage` (`:1906`), `RouteMessageWithHeaders` (`:1969`), `BroadcastMessage` (`:2066`), `SendToGroup` (`:2380`), `SendToGroupWithHeaders` (`:2453`) |
| `meshworx.hub.bytes.routed` | `Counter<long>` | `direction`: `direct` / `broadcast` / `group` | created `:405`; recorded alongside each `messages.routed` add, with the body's length |
| `meshworx.hub.messages.dropped` | `Counter<long>` | `reason`: `unknown-recipient` / `queue-full` / `expired` (PR #85) / `offline-queue-full` (issue #28) | created `:410`; recorded at every drop site — `RouteMessage` unknown-recipient (`:1883`) and queue-full (`:1898`), `RouteMessageWithHeaders` the same pair (`:1937`, `:1964`), `BroadcastMessage` queue-full (`:2055`), `SendToGroup` queue-full (`:2398`), `SendToGroupWithHeaders` queue-full (`:2482`), `SendLoopAsync` expired (`:1666`, via `IsExpiredFrame` — see [dropping expired frames](#dropping-expired-frames)). **Every one of the five queue-full sites also raises `QueueSaturated` immediately alongside this counter add (PR #87) — see [Backpressure signalling](#backpressure-signalling-and-awaiting-capacity) below; the counter itself gained no new tag** |
| `meshworx.hub.messages.offline_queued` | `Counter<long>` | — | issue #28; incremented in `TryStoreForOfflineDeliveryAsync` once a store has accepted a message. The same message is counted on `messages.routed` (`direction=direct`) **later**, when the recipient returns and `DeliverStoredMessagesAsync` queues it — so a held message is counted once here and once there, never twice on the same instrument |
| `meshworx.hub.outbound_queue.depth` | `ObservableGauge<int>` | — | created `:415`; callback `ObserveOutboundQueueDepth` (`:433-442`) sums `ClientConnection.OutboundQueue.Reader.Count` across every entry in `_clients` on each collection pass |

> **Several of this table's own citations, and the bullets below it, were found to be stale by roughly
> 40–130 lines each — pre-dating PR #87 and not caused by it — and are corrected in place by this pass**
> (ground-truthed directly against the source rather than mechanically shifted, per the standing
> "touching the file makes fixing it free" rule, since the drop sites this table describes are exactly
> what PR #87 touches). The wrong numbers were self-consistent (each resolved to real, plausible-looking
> code), so they were not caught by a range check — only by re-deriving every citation from the current
> symbol names directly.

**The routed counters are not "once per delivery" for every send kind, and the difference is deliberate:**

- **`RouteMessage`/`RouteMessageWithHeaders` (direct) only count `messages.routed`/`bytes.routed` once the
  frame has actually been written to the recipient's queue** (`:1906`, after the `TryWrite` check at
  `:1892` returns `true`; header variant identically at `:1969`, after either the initial `TryWrite` at
  `:1945` or a successful `TryAwaitCapacityAsync` (PR #87) sets `queued` at `:1953-1955`). A direct
  send has exactly one
  candidate recipient, so "routed" and "successfully enqueued" are the same event — there is no
  partial-success case to reconcile, and a message that only queues after awaiting capacity is still
  routed exactly once, not double-counted.
- **`BroadcastMessage`, `SendToGroup` and `SendToGroupWithHeaders` count `messages.routed`/`bytes.routed`
  once per call that reaches at least one candidate recipient, not once per recipient, and not at all when
  there was nobody to receive it.** `BroadcastMessage` tracks a local `hasRecipient` flag, set the first
  time the loop sees an entry that is not the sender (`:2037`, set `:2047`), and only records routed/bytes
  after the loop if that flag is set (`:2066-2067`) — a broadcast from a hub's only connected client
  therefore records nothing. `SendToGroup`/`SendToGroupWithHeaders` reach the same effect with an early
  return: a `recipients` snapshot of exactly `[senderId]` (the sender is the group's only member) returns
  before any frame is built (`:2360`), so the routed/bytes add just after (`:2380-2381`) is only ever
  reached when at least one other member exists. `SendToGroupWithHeaders` additionally never builds the
  frame shape a header-aware member would need if no member is negotiated at `HeaderEnvelopeMinVersion` or
  above (and vice versa for the plain shape) — that lazy build does not change when `messages.routed` is
  counted, only which of the (at most two) `byte[]` allocations actually happen.
- **Consequence for anything consuming these metrics:** for a broadcast or group send (with or without
  headers), `messages.routed` can be incremented for a call where every individual recipient's queue was
  full — `messages.dropped` (`reason=queue-full`) is incremented once per failed recipient in the same
  call, independently of the routed counter. `messages.routed − messages.dropped` is **not** a
  delivered-message count for `broadcast`/`group`; it only held that identity for `direct` **until
  PR #85 (issue #29)**. See [known-issues.md](known-issues.md) KI-32 for the fuller write-up of this
  asymmetry — PR #74's two new routing methods inherit it unchanged, they do not introduce a new variant
  of it.
- **PR #85 makes `reason=expired` a genuine exception to "routed and dropped are mutually exclusive for
  `direct`".** `SendLoopAsync` (a separate task from the one that increments `routed`) can drop a direct
  frame as expired *after* `RouteMessage`/`RouteMessageWithHeaders` already counted it as `routed` — see
  [dropping expired frames](#dropping-expired-frames) above and [known-issues.md](known-issues.md) KI-32.
- **PR #87 adds no new tag and no new instrument.** `QueueSaturated` (in-process) and the optional
  `0x15 QueueSaturated` wire notification are additional signalling alongside the existing
  `messages.dropped(reason=queue-full)` add, not a replacement or a second counter — a dashboard built on
  these metrics alone is unaffected by whether `notifyOnQueueSaturation` is set.

**Three internal, test-only members exist purely to make the metrics deterministically testable, and are
not part of the public contract:**

- `GetMeterForTesting()` (`:698`) — see above.
- `TryQueueRawFrameForTesting(Guid clientId, byte[] frame)` (`:1343-1347`) — writes a raw frame directly
  onto a registered client's outbound queue, bypassing routing entirely, so a test can drive a client's
  queue to capacity deterministically rather than racing a real producer against a real consumer.
- `OutboundQueueCapacityForTesting` (`:1353`) — the queue capacity (`1024`), exposed so a test knows how
  many frames `TryQueueRawFrameForTesting` needs. Backed by `ClientConnection.OutboundQueueCapacity`,
  widened from `private` to `internal` (`:2546`) for exactly this purpose — a private member of a nested
  type is not accessible from its enclosing type in C#.

---

## Contracts & gotchas

- **Ownership:** the hub owns every accepted transport and the listener; all are disposed on teardown.
- **Delivery is best-effort.** See the routing table — unknown recipients and full queues drop frames.
  This is the single most important behavioural fact about the hub. [known-issues.md](known-issues.md) KI-1, KI-4.
- **The shutdown writes the `Disconnect` frame directly to each transport** (`StopCoreAsync`,
  `MeshHub.cs:562-572`), bypassing the send loop, concurrently with any in-flight send-loop write. This
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
- **Name check is in chars, not bytes** (`clientName.Length`, `MeshHub.cs:988`), matching the client.
  A 256-char name can exceed 256 UTF-8 bytes. [known-issues.md](known-issues.md) KI-3.
- **Group names in `Join`/`Leave` frames are unbounded** (whole remainder of the frame), but
  `SendToGroup` frames cap the name at `ushort.MaxValue`. Minor asymmetry — a name longer than 65 535
  bytes could be joined but never targeted by a group send. [known-issues.md](known-issues.md) KI-8.
- **Lookup responses are handled inline in the receive loop** and awaited on `clientCts.Token`
  (`MeshHub.cs:1188`) — a slow/blocked transport write here stalls that client's inbound processing.
- **Malformed frames are silently ignored** — the dispatch chain is a series of length-guarded
  `else if`s with no terminal `else`. [known-issues.md](known-issues.md) KI-9. A malformed *registration*
  frame is dropped the same way, without an error reply.
- **A hub with no authenticator admits anyone who can reach the listener.** The seam exists; using it is
  opt-in. [known-issues.md](known-issues.md) KI-2.
- **A group send from a non-member is dropped, silently and unconditionally.** No error frame, a `Debug`
  log only (`MeshHub.cs:2351-2357`). This holds with or without a `groupAuthoriser`, and it is a
  behavioural break for any client that used to publish to a group without joining it — such a client
  must now join, and will then also start receiving that group's traffic. There is no send-only
  capability. [known-issues.md](known-issues.md) KI-2.
- **A refused join revokes an existing membership.** `JoinGroupAsync` calls `LeaveGroup` before replying
  (`MeshHub.cs:2114`), so re-joining a group you are already in is a live re-authorisation, not a no-op.
  Do not "optimise" the re-join into an early return — that would make the first `true` permanent.
- **The join path awaits an integrator callback inside the receive loop.** That parks the calling
  client's inbound processing and makes it look idle to the heartbeat monitor. See
  [Group authorisation](#group-authorisation) and [known-issues.md](known-issues.md) KI-28.
- **Every constructor default is now finite (PR #68, issue #16).** `maxClients` defaults to 1000
  (was unlimited), `heartbeatInterval` defaults to 30 s (was disabled), and a new
  `maxConnectionsPerRemoteEndpoint` (default 100) caps connections per remote address. A hub built with
  no arguments is a **behavioural change** from any hub built before this PR. See
  [Per-remote-endpoint connection cap](#per-remote-endpoint-connection-cap) below and
  [known-issues.md](known-issues.md) KI-29.

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
It now also takes `groupAuthoriser` / `groupAuthorisationTimeout` (`Fixtures/MeshHubFixture.cs:28-29`,
passed through at `:56-57`) and carries frame builders for the group opcodes:
`CreateJoinGroupRequest(name)`, `CreateGroupMessage(name, message)`, `CreateDirectMessage(id, message)`
and `CreateLookupRequest(correlationId, name)`. Since PR #68 (issue #16) it also takes
`maxConnectionsPerRemoteEndpoint` (`:30`, passed through at `:58`), and both `CreateMockTransport` and
`RegisterClientAsync` take an optional `IPEndPoint? remoteEndPoint` so a test can put a mock transport
behind the per-remote-endpoint cap — see [testing.md](testing.md#per-remote-endpoint-connection-cap).
Since PR #87 (issue #30) it also takes `notifyOnQueueSaturation` / `backpressureAwaitTimeout`
(`:31-32`, passed through at `:59-60`), both defaulted so existing call sites are unaffected.

The **group authorisation boundary is pinned by twelve tests** added in PR #66, grouped under the
`// Groups as an authorisation boundary` banner at `MeshHubTests.cs:2328`:

| Test | Pins | Source |
|---|---|---|
| `SendToGroup_SenderIsNotAMember_MessageIsNotDelivered` | the unconditional membership requirement on sends | `MeshHubTests.cs:2348` |
| `SendToGroup_SenderIsAMember_MessageIsDeliveredToOtherMembers` | the requirement did not break the normal path | `:2377` |
| `JoinGroup_AuthoriserRefuses_ClientIsToldAndReceivesNoGroupMessages` | refusal reaches the client **and** withholds traffic | `:2408` |
| `JoinGroup_AuthoriserAllows_AdmitsClientAndSeesItsIdentity` | the context carries the registered identity | `:2442` |
| `JoinGroup_AuthoriserThrows_JoinIsRefused` | fail-closed on throw, connection stays live | `:2480` |
| `JoinGroup_AuthoriserCancels_JoinIsRefused` | fail-closed on self-cancellation | `:2507` |
| `JoinGroup_AuthoriserHangs_JoinIsRefusedAtTheTimeout` | fail-closed at `groupAuthorisationTimeout` | `:2532` |
| `JoinGroup_AfterReconnect_IsAuthorisedAgainRatherThanRestored` | a restore cannot bypass the decision | `:2561` |
| `JoinGroup_AuthoriserRefusesAnInvalidUtf8Name_RefusalEchoesTheNameWithoutGrowingIt` | the echo, not a re-encode — the size property | `:2636` |
| `JoinGroup_AuthoriserRefusesAnExistingMember_RevokesTheMembership` | a refusal revokes rather than declines | `:2668` |
| `JoinGroup_NoAuthoriser_AdmitsAnyClient` | the default stays open admission | `:2705` |
| `Constructor_NonPositiveGroupAuthorisationTimeout_ThrowsArgumentOutOfRangeException` | the range guard | `:2733` |

> **`FrameRecorder` (`Fixtures/MeshHubFixture.cs:222`) is the reusable piece here, and its shape is the
> lesson.** Waiting for a frame the hub must **not** send is not deterministic on its own. These tests
> pair the absence with a frame the hub certainly *will* send afterwards on the same connection — a
> direct message to the same client — and because a client's outbound queue is drained in order, the
> arrival of the later frame proves the earlier one was never queued. Copy that pairing rather than
> sleeping; a bare "assert nothing arrived" is a flaky test.

The **heartbeat schedule above is pinned by tests**, and they are the reference if you change it: the
eviction interval is asserted indirectly by counting pings up to the moment of teardown
(`HandleClient_SilentClient_IsEvictedOnConfiguredIntervalNotTheOneAfter`, `MeshHubTests.cs:2103`), the
N=1 no-probe boundary by `HandleClient_SilentClientWithSingleMissedHeartbeat_IsEvictedWithoutPinging`
(`:2105`), and the no-false-eviction direction by
`HandleClient_ClientSendingFramesEveryInterval_IsNotEvicted` (`:2149`). A bare "was it evicted?"
assertion cannot tell the Nth interval from the (N+1)th — the ping count is what makes it a regression
test, so keep the counting shape if you extend these.

The **capacity claim is pinned by three tests** added in PR #64's successor, PR #65:

| Test | Pins | Source |
|---|---|---|
| `HandleClient_LastSlotClaimedButNotYetRegistered_RefusesRatherThanExceedingMaxClients` | a registration reaching the decision while another holds the last slot but is not yet in `_clients` is refused | `MeshHubTests.cs:870` |
| `HandleClient_RefusedForDuplicateNameAfterClaimingSlot_GivesTheSlotBack` | the duplicate-name refusal releases its claim rather than leaking it | `:926` |
| `HandleClient_ClientDisconnects_GivesItsSlotBackForAReplacement` | an ordinary disconnect frees the slot for a replacement | `:969` |

The first test is why `TryReserveClientSlot`/`ReleaseClientSlot` are **`internal`** rather than private
(`InternalsVisibleTo` in `AdamSalisbury.Meshworx.csproj:26`): it needs to put the hub into the state a
concurrent registration produces — slot taken, client not yet registered — which is exactly the window
`ConnectedClientCount` cannot see. Keep them internal; making them private would cost that test.

The **lifecycle concurrency contract is pinned by eight tests** added in PR #64, grouped under the
`// StopAsync / DisposeAsync under concurrent invocation` banner at `MeshHubTests.cs:118`:

| Test | Pins | Source |
|---|---|---|
| `StopAsync_CalledWhileAShutdownIsInFlight_NotifiesEachClientOnce` | clients notified once, not once per caller | `MeshHubTests.cs:133` |
| `StopAsync_CalledWhileAShutdownIsInFlight_ReturnsOnlyOnceTheShutdownCompletes` | a joining caller does not return early | `:165` |
| `StopAsync_CalledConcurrently_AllCallersCompleteWithoutError` | reproduces the original `NullReferenceException` | `:205` |
| `DisposeAsync_CalledConcurrently_TearsTheHubDownOnce` | teardown memoised, listener disposed once | `:222` |
| `StartAsync_AfterDispose_ThrowsObjectDisposedException` | disposal is terminal | `:243` |
| `StartAsync_ListenerFailsToStart_LeavesTheHubStartable` | the `_starting` claim is released on failure | `:256` |
| `StopAsync_AfterCompleting_ReleasesTheHubsRunningClaim` | `_stopTask` cleared — hub not wedged as *stopping* | `:282` |
| `StopAsync_WhileAStartIsInProgress_LeavesTheStartedHubIntact` | a stop cannot abandon a just-bound listener | `:306` |

Two of these pin the interleaving **deterministically** rather than hoping for it, and the seams they
use are reusable — see [testing.md](testing.md#parking-a-caller-mid-lifecycle).
`StopAsync_CalledConcurrently_AllCallersCompleteWithoutError` is deliberately a genuine thread race and
is documented in its own `<remarks>` as the weaker of the pair; the deterministic test beside it is the
guard. Keep that pairing if you extend these.

The **metrics added by PR #72 are pinned by a dedicated `MeshHubMetricsTests.cs`**, using
`fixture.Hub.GetMeterForTesting()` plus a `MetricsCapture<T>` fixture (a `MeterListener` filtered to one
hub's `Meter` by reference) rather than a bare `Assert.Equal` against a global collector — see
[testing.md](testing.md#metrics-tests) for the fixture and the full test list, including the two tests
that specifically pin the zero-recipient exclusion (`BroadcastMessage_SenderIsOnlyClient_DoesNotIncrementRoutedCounter`)
and the once-per-call-not-per-recipient behaviour
(`BroadcastMessage_MultipleRecipients_IncrementsRoutedCounterOnceTaggedBroadcast`).
