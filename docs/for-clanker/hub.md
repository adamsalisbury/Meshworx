# Hub — `MeshHub` / `IMeshHub`

[← back to index](../for-clanker.md) · related: [client.md](client.md) · [transport.md](transport.md) · [protocol.md](protocol.md) · [known-issues.md](known-issues.md)

The server side. `MeshHub` accepts connections from an `ITransportListener`, runs the registration
handshake, tracks registered clients by id and by name, and routes direct / broadcast / group messages
between them. It never interprets payloads — it reads the one-byte opcode and forwards the body.

- **Type:** `public sealed class MeshHub : IMeshHub, IAsyncDisposable` — `src/AdamSalisbury.Meshworx/MeshHub.cs:13`
- **Interface:** `IMeshHub` — `src/AdamSalisbury.Meshworx/IMeshHub.cs:5`

---

## Public surface

| Member | Signature | Source |
|---|---|---|
| ctor | `MeshHub(ILogger<MeshHub>, ITransportListener, TimeSpan? registrationTimeout=null, int? maxClients=null, TimeSpan? heartbeatInterval=null, int maxMissedHeartbeats=2, ClientAuthenticator? authenticator=null, int? maxConcurrentAuthentications=null, GroupAuthoriser? groupAuthoriser=null, TimeSpan? groupAuthorisationTimeout=null, int? maxConnectionsPerRemoteEndpoint=null)` | `MeshHub.cs:180` |
| `StartAsync` | `Task StartAsync(CancellationToken=default)` — binds listener, starts accept loop. Refuses a second concurrent start, a start during shutdown, and a start after disposal | `MeshHub.cs:310` |
| `StopAsync` | `Task StopAsync(CancellationToken=default)` — best-effort disconnect all, drain, reset. **Not `async`** — returns the shared shutdown task | `MeshHub.cs:381` |
| `DisposeAsync` | `ValueTask` — `StopAsync`, disposes the listener, then the authentication semaphore. Memoised; disposal is terminal | `MeshHub.cs:557` |
| `ConnectedClientCount` | `int` — snapshot of `_clients.Count`. **Not** the value `maxClients` is enforced against, and it can transiently read *below* the number of claimed slots | `MeshHub.cs:510` |
| `IsRunning` | `bool` — `true` from the moment `StartAsync` completes until `StopAsync` begins tearing the hub down. Added by PR #71 (issue #23) for the `AddMeshHub` health check | `MeshHub.cs:513-522` |
| `MaxClients` | `int` — the cap passed to the constructor (or `DefaultMaxClients` if omitted). Backed the admission check as a private field before PR #71; now also a public getter, unchanged in behaviour | `MeshHub.cs:525` |
| `ClaimedClientSlots` | `int` — `Volatile.Read` of `_reservedClientSlots`. **This, not `ConnectedClientCount`, is what `MaxClients` is actually enforced against** — see the bullet below and [known-issues.md](known-issues.md) KI-26. Added by PR #71 for the `AddMeshHub` health check | `MeshHub.cs:528` |
| `IsClientRegistered` | `bool IsClientRegistered(Guid)` | `MeshHub.cs:531` |
| `ClientConnected` | `event EventHandler<ClientConnectionEventArgs>` — after registration completes | `MeshHub.cs:504` |
| `ClientDisconnected` | `event EventHandler<ClientConnectionEventArgs>` — after a client is removed | `MeshHub.cs:507` |

The constructor carries **two independent integrator seams**, both optional and both defaulting to "not
configured":

| Seam | Params | Question it answers | Section |
|---|---|---|---|
| Authentication | `authenticator`, `maxConcurrentAuthentications` | may this peer **register at all**? | [Authentication](#authentication) |
| Authorisation | `groupAuthoriser`, `groupAuthorisationTimeout` | may this registered client **join this group**? | [Group authorisation](#group-authorisation) |

Defaulting both to `null` preserves the pre-v3 open-admission behaviour. **One group rule is not
optional and applies with or without an authoriser: sending to a group requires membership of it** — see
[Routing helpers](#routing-helpers).

### Using it efficiently

- Construct with a started-or-not listener; `StartAsync` calls `listener.StartAsync` for you. Calling
  `StartAsync` twice throws `InvalidOperationException` ("already running", `MeshHub.cs:319-322`), as
  does starting while a shutdown is still in flight. Starting a **disposed** hub throws
  `ObjectDisposedException` (`MeshHub.cs:317`). See [Lifecycle & concurrency](#lifecycle) below.
- **The hub owns the listener** — `DisposeAsync` disposes it. Do not dispose the listener yourself.
- `ClientConnected` / `ClientDisconnected` fire **from the per-client handler task**, so they run
  **concurrently for different clients**. Handlers must be thread-safe. A throwing handler is caught and
  logged (`RaiseClientEvent`, `MeshHub.cs:1224-1244`) — it will not fault the hub. That catch covers the
  handler only up to its **first suspension**: an `async void` handler's later exception escapes to the
  thread pool unobserved, so keep handlers synchronous or contain the failure inside the task you start
  (see [for-clanker.md](../for-clanker.md#4-threading--async-model-read-before-changing-any-loop) and the
  README's **Event handlers** section).
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
  idempotent (a hub that is not running returns `Task.CompletedTask`, `MeshHub.cs:390-393`) and **safe
  under concurrent invocation** — overlapping callers share one shutdown. See
  [Lifecycle & concurrency](#lifecycle) below.
- **A stopped hub is not restartable in general.** `StopAsync` releases the hub's own state, but
  `ITransportListener` has no stop, so the endpoint stays bound and both shipped listeners throw on a
  second `StartAsync`. Treat a stopped hub as spent and dispose it. [known-issues.md](known-issues.md) KI-25.
- **The constructor validates and then warns.** Non-positive timeouts/counts throw
  `ArgumentOutOfRangeException`; `maxMissedHeartbeats < 1` is rejected outright (`MeshHub.cs:220-224`),
  as is a non-positive `groupAuthorisationTimeout` (`MeshHub.cs:233-237`). Beyond that it logs **two**
  possible warnings, neither of which throws — if you assert on hub logs in a test, expect them:
  - `heartbeatInterval` set **and** `maxMissedHeartbeats: 1` (`MeshHub.cs:269-282`), because that
    combination evicts on the first idle interval and never probes — see
    [the heartbeat schedule](#heartbeat-schedule) below. Legal if your clients send continuously.
  - `groupAuthoriser` set **and** `groupAuthorisationTimeout >= heartbeatInterval × maxMissedHeartbeats`
    (`MeshHub.cs:284-305`). A client's receive loop is parked while its join is being authorised, so it
    looks idle to the heartbeat monitor however healthy it is; a decision that outlasts the eviction
    budget gets the client **evicted rather than refused** — and behind a `MeshClientReconnector` that
    becomes a reconnect loop. Keep the timeout comfortably below the budget. It warns rather than
    throwing because the default 10 s timeout would otherwise refuse construction of any hub with a
    short heartbeat interval, and a slow authoriser may never actually take that long.

---

## Internal architecture

### State

- `ConcurrentDictionary<Guid, ClientConnection> _clients` — registered connections by id.
- `ConcurrentDictionary<string, Guid> _clientNames` — name → id, the uniqueness gate. `TryAdd` here is
  the atomic "claim this name" operation (`MeshHub.cs:879`).
- `int _reservedClientSlots` (`MeshHub.cs:74`) — **the counter `maxClients` is actually enforced
  against**, not `_clients.Count`. A slot is claimed by one atomic compare-and-swap
  (`TryReserveClientSlot`, `MeshHub.cs:1098`) during registration and given back by `ReleaseClientSlot`
  (`:1127`) in the handler's `finally`. Read it with `Volatile.Read`; never write it directly. Shutdown
  deliberately does **not** reset it — each still-running handler owns its own claim and returns it
  itself. See [Registration handshake](#registration-handshake-hub-side) and
  [known-issues.md](known-issues.md) KI-26.
- `ConcurrentDictionary<string, Group> _groups` (`StringComparer.Ordinal`) — group name → membership.
  This is the **authoritative** membership set: it gates joins (via the authoriser) and gates group sends.
  The client's own `JoinedGroups` is an optimistic mirror of it, not a second source of truth.
- `ConcurrentDictionary<Task, byte> _handlerTasks` — live per-client handler tasks, awaited on shutdown.
- One `CancellationTokenSource? _cts` (`MeshHub.cs:90`) + `Task? _acceptLoopTask` (`:91`) for the accept
  loop lifecycle, plus `Task? _stopTask`, `Task? _disposeTask`, `bool _starting` and `bool _disposed`.
  **All six are guarded by `Lock _stateLock` (`MeshHub.cs:88`)** and must only be read or written inside
  it — see [Lifecycle & concurrency](#lifecycle).
- `GroupAuthoriser? _groupAuthoriser` (`MeshHub.cs:46`) + `TimeSpan _groupAuthorisationTimeout` (`:47`)
  — the authorisation seam. `null` authoriser means every join is allowed. There is deliberately **no**
  companion semaphore here, unlike the authentication seam; the reasoning is in
  [Group authorisation](#group-authorisation).
- `ClientAuthenticator? _authenticator` + `SemaphoreSlim? _authenticationSlots` — the authentication
  seam. The semaphore is **only allocated when an authenticator was supplied** (`MeshHub.cs:263-267`),
  so an unauthenticated hub does no extra work and allocates nothing.

`ClientConnection` (nested, `MeshHub.cs:1748`) holds the id, name, transport, a bounded outbound
`Channel<byte[]>` (capacity **1024**, single-reader/multi-writer), an `ActivitySequence` counter
(`Interlocked`-incremented per received frame), and the `HashSet<string> Groups` it has joined.

<a id="lifecycle"></a>

### Lifecycle & concurrency

`StartAsync`, `StopAsync` and `DisposeAsync` can each be called from a different thread at the same
time. Since PR #64 (issue #12) all three are serialised behind a single `Lock _stateLock`
(`MeshHub.cs:88`) and the whole lifecycle obeys one rule:

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

**`StartAsync` (`MeshHub.cs:310`)** claims the running slot before doing any I/O:

1. Under the lock: throw `ObjectDisposedException` if `_disposed` (`:317`); throw
   `InvalidOperationException` if `_cts`, `_stopTask` or `_starting` says the hub is spoken for
   (`:319-322`); otherwise set `_starting = true` (`:328`).
2. Outside the lock: `await _listener.StartAsync` (`:335`). On failure, release the claim and dispose
   the unused token source (`:337-349`) — a hub whose listener failed to start is startable again.
3. Under the lock again: clear `_starting`, **re-check `_disposed`** (`:358-362`, a disposal may have
   completed while the listener was starting), then publish `_cts` and `_acceptLoopTask`
   **together** (`:368-369`).

> **Why the `_starting` flag rather than publishing `_cts` early.** Publishing the token source before
> the accept loop exists would let a concurrent `StopAsync` take ownership of a hub that had just bound
> its listener and then report itself stopped — leaving the endpoint bound with nothing serving it and
> no way to recover, since the listener cannot be started a second time.

**`StopAsync` (`MeshHub.cs:381`) is not `async`** — it is a plain method returning a `Task`, so its
decision is taken **synchronously** under the lock before the caller gets a task back. Under the lock:
if `_stopTask` is already set, join it; otherwise read `_cts` and, if it is null, return
`Task.CompletedTask` (`:390-393`); otherwise take ownership — capture the token source and accept-loop
task into locals, null both fields, and publish the shutdown in `_stopTask` (`:395-401`). Every caller
then awaits that one task, so **clients are notified once, not once per caller**, and every caller
returns only once the hub has actually stopped.

- A caller's own `cancellationToken` is honoured via `WaitAsync` (`:409`), but **abandoning the wait
  does not cancel the shutdown** — that belongs to the caller which started it.
- The teardown is split in two. `StopCoreAsync` (`:416`) opens with `await Task.Yield()` (`:422`) so
  none of it runs on the caller's stack while the lock is held, then sends the best-effort `Disconnect`
  notification (`:426-436`). `ShutDownAsync` (`:453`) does the shutdown proper: cancel (`:458`), drain
  the accept loop (`:460-470`) and the handler tasks (`:474-482`), clear the four registries
  (`:484-487`), dispose the token source (`:489`).
- **`ShutDownAsync` runs from `StopCoreAsync`'s `finally` (`:439-446`).** That is load-bearing: the
  notification's exception filter covers only `IOException`/`ObjectDisposedException`/
  `OperationCanceledException`, so before this an unfiltered transport exception abandoned the shutdown
  half way — accept loop still running, token source undisposed, hub reporting itself stopped and no
  later call able to put it right.
- `ShutDownAsync`'s own `finally` clears `_stopTask` (`:491-500`), so a shutdown that failed part way
  leaves the hub *stopped* rather than wedged as permanently *stopping*.

**`DisposeAsync` (`MeshHub.cs:557`)** sets `_disposed = true` **first** (`:565`), before any teardown
begins, so a start racing a disposal is refused rather than racing the listener's teardown. It then
memoises its teardown in `_disposeTask` (`:566`); every later or concurrent call awaits that same task.
`DisposeCoreAsync` (`:576`) yields (`:580`) before awaiting `StopAsync` (`:582`), then disposes the
listener (`:583`) and the authentication semaphore (`:584`) — **exactly once**. Disposal is terminal.

> **If you change any of this, keep the shape.** Do not reintroduce a second read of a lifecycle field
> outside the lock; do not await inside the lock; do not move `ShutDownAsync` out of the `finally`; and
> do not make `StopAsync` `async` again — its synchronous decision is what makes the "join the existing
> shutdown" handover race-free, and it is what the tests pin. See
> [testing.md](testing.md#parking-a-caller-mid-lifecycle) for the seams that pin these interleavings.

### The three per-connection tasks

Every accepted connection is handled by `HandleClientAsync` (`MeshHub.cs:771`), which after a successful
handshake spins up:

1. **Receive loop** — the body of `HandleClientAsync` itself: `transport.ReceiveAsync` → dispatch by
   opcode → route. Reads against one long-lived `clientCts` token (no per-frame CTS). Dispatch is
   otherwise synchronous, with **two** branches that await: the lookup response write (`MeshHub.cs:991`)
   and, when a `groupAuthoriser` is configured, the group join (`MeshHub.cs:943-944`). While either is
   in flight this client's loop reads **nothing else from this client** — which is what makes a slow
   authoriser a per-client problem rather than a hub-wide one, and also why a slow authoriser can get
   the client evicted by the heartbeat monitor (see the constructor warning above).
2. **Send loop** — `SendLoopAsync` (`MeshHub.cs:1250`): drains the outbound `Channel`, **coalescing**
   already-queued frames up to a 64 KiB byte budget (`SendCoalesceByteBudget`, `MeshHub.cs:1248`) into a
   single batched write when the transport implements `IBatchSendTransport`; otherwise sends them one at
   a time. A lone frame is sent immediately (no latency added).
3. **Heartbeat monitor** — `MonitorHeartbeatAsync` (`MeshHub.cs:1307`), **only if `heartbeatInterval`
   is set**. One `PeriodicTimer`. Compares `ActivitySequence` between ticks; an interval in which the
   sequence did not move is a **silent interval** and increments a miss counter. The counter is
   **checked before the probe**: on reaching `maxMissedHeartbeats` it cancels the client's CTS to evict
   and returns (`MeshHub.cs:1337-1345`); otherwise it enqueues a `Ping` and loops
   (`MeshHub.cs:1349`). Any frame from the client resets the counter to zero.

All three share `clientCts` (linked to the hub's token). Teardown in the `finally` block
(`MeshHub.cs:1012-1083`) completes the outbound queue, cancels the CTS, awaits the send + heartbeat tasks,
removes the client from all groups and both dictionaries, disposes the connection (which disposes the
transport), and raises `ClientDisconnected`.

<a id="heartbeat-schedule"></a>

> **Heartbeat schedule (know this before tuning):** eviction fires when `missedHeartbeats >=
> _maxMissedHeartbeats` (`MeshHub.cs:1337`), and the check sits **above** the `TryWrite` of the `Ping`
> (`MeshHub.cs:1349`). So "max missed = N" means exactly what it says — a client that sends nothing is
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
> combination (`MeshHub.cs:269-282`) rather than throwing, because it is legal if your clients are
> known to send continuously. Values below 1 throw `ArgumentOutOfRangeException` (`MeshHub.cs:220-224`).
>
> This was previously off by one — eviction on the (N+1)th interval — and was corrected in PR #61
> (issue #9). See [known-issues.md](known-issues.md) KI-11. If you are reading older notes, or a hub
> built before that change, the schedule was one interval longer and the client was probed N times.

### The accept loop

`AcceptLoopAsync` (`MeshHub.cs:587`) loops `listener.AcceptAsync`. `OperationCanceledException` /
`ObjectDisposedException` break the loop (shutdown); **any other exception is logged and swallowed** so
one bad connection cannot kill the hub (`MeshHub.cs:604-612`) — this is the intentional broad catch at a
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
> (`MeshHub.cs:628`) alongside the accept-failure retry — it is not part of the two-way
> finished-vs-failed split above, since it fires on a connection that was accepted successfully but is
> then refused for capacity, not on an `AcceptAsync` failure.

<a id="per-remote-endpoint-connection-cap"></a>

### Per-remote-endpoint connection cap

Added by PR #68 (issue #16), to close the gap `maxClients` leaves open: `maxClients` only bounds
**registered** clients, so a flood of connections that never complete the handshake — deliberately, to
exhaust handler tasks, sockets and outbound queues — sails straight past it. This cap is checked in
`AcceptLoopAsync`, **before** a handler task is even created, and covers the whole connection lifetime
including the pre-registration window.

- **`ExtractRemoteAddress`** (`MeshHub.cs:656`) reads the transport's remote address, but only if it
  implements [`IRemoteEndPointTransport`](transport.md#iremoteendpointtransport-public--transportiremoteendpointtransportcs16)
  and reports an `IPEndPoint` — a transport that doesn't (the in-memory transport, or a custom one that
  hasn't implemented the interface) is **never capped** by this. `TcpTransport` is the only shipped
  implementer.
- **The address is masked before use.** `NormaliseForEndpointCap` (`MeshHub.cs:680-695`) returns IPv4
  addresses unchanged but reduces an IPv6 address to its `/64` network prefix (`IPv6CapPrefixLength`,
  `MeshHub.cs:666`), zeroing the low 8 bytes (the interface identifier). **This is load-bearing, not a
  tidy-up:** a single host is routinely handed an entire `/64` by its ISP or cloud provider, and without
  the mask a source could defeat the cap by connecting from a fresh address within that `/64` every time
  — each one would look like a brand-new, never-before-seen source under a full-address key.
- **The claim is a CAS loop against a `ConcurrentDictionary<IPAddress, int>`** (`_connectionsByRemoteAddress`,
  `MeshHub.cs:55`), mirroring `TryReserveClientSlot`/`ReleaseClientSlot`:
  `TryReserveEndpointSlot` (`:722-737`) only ever claims from a count still under
  `maxConnectionsPerRemoteEndpoint` at the instant of the claim, and `ReleaseEndpointSlot` (`:748-769`)
  removes the dictionary entry once its count reaches zero rather than leaving a zero-valued entry
  behind — a long-lived hub does not accumulate one entry per distinct address it has ever seen.
- **A refusal disposes the transport immediately and reads nothing from it**
  (`DisposeRefusedTransportAsync`, `MeshHub.cs:700-711`) — no registration frame is ever received from a
  refused connection, so a flood cannot force any parsing work at all, not even a malformed-frame check.
- **The slot is released in `HandleClientAsync`'s `finally`** (`MeshHub.cs:1065-1071`), unconditionally
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

Inside `HandleClientAsync` (`MeshHub.cs:781-895`), in order. The frame layout is
`[type][version][name length (2, big-endian)][name][credential]` — see [protocol.md](protocol.md#registration-handshake).

1. Receive one frame under a **registration-timeout** linked CTS. Timeout → drop silently (`:784-799`).
2. Validate: frame must be ≥ **2** bytes and opcode `RegistrationRequest` (`0x04`) — else drop (`:803-808`).
3. Byte 1 must equal `Protocol.Version` (**3**) — else send `Error(UnsupportedProtocolVersion)` and drop
   (`:810-816`).
4. Frame must be ≥ 4 bytes, then read the `ushort` name length at offset 2. A **zero** length, or one
   that runs past the payload, is malformed → **drop silently, no error frame** (`:818-831`). Decode the
   name from `[4, 4+len)` (`:833`).
5. If `name.Length > 256` (chars) send `Error(ClientNameTooLong)` and drop (`:835-841`).
6. **If an authenticator is configured** (`:843-864`) — and only then — two things happen, in this order:
   first an at-capacity **early-out**, `Volatile.Read(ref _reservedClientSlots) >= maxClients` →
   `Error(HubAtCapacity)` and drop *without* running the authenticator (`:850-854`); then the callback
   itself (`:856-863`, see [Authentication](#authentication)), anything other than `true` →
   `Error(AuthenticationFailed)` and drop. With no authenticator neither happens and the handler falls
   straight through to step 7.
7. **Claim a client slot.** `TryReserveClientSlot()` (`:871-875`, implementation at `:1098`)
   compare-and-swaps `_reservedClientSlots` up by one if and only if it is still below `maxClients`.
   **This single atomic operation is the binding capacity decision**; it fails →
   `Error(HubAtCapacity)` and drop. On success the handler sets `slotReserved = true` (`:877`), which
   arms the matching `ReleaseClientSlot()` in its `finally` (`:1060-1063`).
8. `_clientNames.TryAdd(name, id)` — if it fails, name is taken → `Error(DuplicateClientName)` and drop
   (`:879-884`). The slot claimed at step 7 is given back on the way out by the `finally`.
9. Create `ClientConnection`, add to `_clients`, send `RegistrationComplete` + assigned 16-byte id,
   raise `ClientConnected`, start the send loop (+ heartbeat monitor), enter the receive loop (`:886-906`).

The assigned id is a fresh `Guid.NewGuid()` generated at handler start (`MeshHub.cs:774`).

> **The ordering of steps 6–8 is deliberate and load-bearing, and it is not the obvious ordering.** The
> *binding* capacity decision (step 7) sits **after** authentication; the cheap early-out inside step 6
> sits **before** it. Three invariants to preserve if you touch this method:
>
> - **A full hub must not run the authenticator.** That is the early-out at `:850-854`, and it is only an
>   early-out — it does not decide admission. Removing it would not soften the cap, but it would reopen
>   the case the check exists to shed: a connection flood driving credential checks, or pinning handler
>   tasks on a slow authenticator, against a hub with nothing left to admit them to.
> - **A refused client must not claim a name.** `_clientNames.TryAdd` stays last, after both the
>   authenticator and the slot claim, so a client refused for either reason never reserves a name.
> - **The claim must not move ahead of authentication, and the claim/release pairing must stay intact.**
>   The claim is taken after the authenticator returns precisely so an unauthenticated peer cannot hold
>   capacity away from one that would authenticate. Every successful `TryReserveClientSlot()` is owned by
>   exactly one handler and must be given back exactly once, by that handler's `finally` (`:1060-1063`) —
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

`AuthenticateAsync` (`MeshHub.cs:1144-1222`) wraps the callback with four protections, all of which exist
because **the callback runs on unauthenticated input, once per accepted connection**:

| Protection | Mechanism | Source |
|---|---|---|
| Concurrency cap | `SemaphoreSlim` of `maxConcurrentAuthentications` (default **64**) permits; a connection that cannot get a slot within `registrationTimeout` is refused | `MeshHub.cs:1156-1164` |
| Time bound | the callback's `ValueTask` is `WaitAsync(_registrationTimeout)`-ed, so a hanging callback cannot pin the handler task or its connection | `MeshHub.cs:1179-1191` |
| Throw isolation | any exception is logged and becomes a refusal rather than faulting the handler (callback boundary) | `MeshHub.cs:1202-1208` |
| Cancellation isolation | an `OperationCanceledException` raised *inside* the callback (e.g. an identity-provider call timing out) — as opposed to hub shutdown — becomes a logged refusal, not a silent drop | `MeshHub.cs:1192-1201` |

Every one of those paths results in the client receiving `Error(AuthenticationFailed)` and the connection
being dropped. **The client cannot distinguish a bad credential from a slow, throwing or overloaded
authenticator** — that is deliberate (it leaks nothing) but it makes hub-side logs the only diagnostic.

The credential is **copied out** of the inbound registration buffer before the context is built
(`MeshHub.cs:1170`), so `RegistrationContext.Credential` does not alias the larger frame. The XML doc on
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
  throws `ArgumentOutOfRangeException` (`MeshHub.cs:226-231`).

### Routing helpers

| Method | Opcode in | Behaviour | Source |
|---|---|---|---|
| `RouteMessage` | `SendMessage` | Look up recipient; build `DeliverMessage`; `TryWrite` to its queue. Unknown recipient → logged `Debug`, **dropped**. Full queue → logged `Warning`, **dropped**. | `MeshHub.cs:1358` |
| `BroadcastMessage` | `BroadcastMessage` | Build one shared `DeliverMessage` frame; `TryWrite` to every client **except the sender**. | `MeshHub.cs:1386` |
| `JoinGroupAsync` | `JoinGroup` | **`async`, and awaited by the receive loop.** Empty name ignored. With a `groupAuthoriser`: copies the inbound name bytes, asks the authoriser, and on refusal calls `LeaveGroup` then `RefuseGroupJoin`. Otherwise, or on approval, calls `AddToGroup`. | `MeshHub.cs:1421` |
| `AddToGroup` | — | The former `JoinGroup` body: `GetOrAdd` the `Group`, add member under its lock. Retries if the group was concurrently removed (`Removed` flag). | `MeshHub.cs:1597` |
| `AuthoriseGroupJoinAsync` | — | Invokes the authoriser behind a `WaitAsync(_groupAuthorisationTimeout)`, with a sync fast path. Refuses on `false`, throw, self-cancellation or timeout. | `MeshHub.cs:1487` |
| `RefuseGroupJoin` | — | Builds `[0x10][echoed name bytes]` and `TryWrite`s it to the client's own queue. Full queue → logged `Warning`, **dropped**. | `MeshHub.cs:1574` |
| `LeaveGroup` | `LeaveGroup` | Remove member; if the group is now empty, mark `Removed` and drop it from `_groups`. Also called by `JoinGroupAsync` on refusal and by `RemoveFromAllGroups` (`:1633`) at teardown. | `MeshHub.cs:1620` |
| `SendToGroup` | `GroupMessage` | **Requires the sender to be a member.** Tests `group.Members.Contains(senderId)` *inside* the group lock (`:1683`); a non-member is logged `Debug` and **dropped** (`:1693-1699`). A member snapshots the ids, then one shared `DeliverGroupMessage` frame is `TryWrite`n to each member **except the sender**. | `MeshHub.cs:1663` |

**Shared delivery frames:** broadcast and group sends allocate the delivery buffer **once** and hand the
same `byte[]` to every recipient's queue. Send loops only read it, so concurrent reads of the
never-mutated buffer are safe. Do not mutate a delivery frame after it is built.

**The membership test is inside the lock, and that placement is deliberate.** `SendToGroup` sets
`recipients` to `null` when the sender is not a member and takes the logging branch *after* releasing the
lock (`MeshHub.cs:1680-1699`) — testing against the live `Members` set rather than scanning the snapshot
afterwards means a sender removed from the group cannot slip a message through the gap between the test
and the copy. Keep the test where it is; do not "simplify" it into a post-snapshot check.

Note the empty-group early return is **gone**: a group with no members can no longer be reached by a
non-member send anyway, and a lone member sending to itself still short-circuits further down
(`MeshHub.cs:1702-1706`). A send to a group that does not exist at all still returns at the
`_groups.TryGetValue` miss (`MeshHub.cs:1669`).

### Group locking model

Each `Group` (`MeshHub.cs:1741`) has its own `Lock`, a `HashSet<Guid> Members`, and a `bool Removed`.
Distinct groups route in parallel; only same-group mutation contends. The `Removed` flag closes a
join/remove race: when the last member leaves, the group is marked removed **under its lock** and taken
out of `_groups` only if that exact instance is still mapped (`TryRemove(KeyValuePair)`,
`MeshHub.cs:1658`). A concurrent `AddToGroup` that fetched the dying instance sees `Removed` and retries
against a fresh one (`MeshHub.cs:1601-1609`). A connection's own `Groups` set is only touched by its
receive loop and teardown, so it needs no lock.

**The authoriser runs *outside* the group lock, and must.** `JoinGroupAsync` awaits the decision before
it ever reaches `AddToGroup` (`MeshHub.cs:1448`), so no integrator callback is ever invoked while a
`Group.Lock` is held. The send-side membership test, by contrast, is *inside* the lock — see
[Routing helpers](#routing-helpers). If you add anything to this path, keep awaits out of the lock.

<a id="group-authorisation"></a>

### Group authorisation

Groups are the hub's only **enforceable** boundary. Two separate rules, one unconditional and one opt-in:

| Rule | Applies | Enforced at |
|---|---|---|
| **A group send requires membership of that group** | always | `SendToGroup`, `MeshHub.cs:1683` |
| **A join must be authorised** | only when a `GroupAuthoriser` is supplied | `JoinGroupAsync`, `MeshHub.cs:1432-1460` |

**With no `groupAuthoriser` (the default) the hub authorises no joins and any client may join any
group** — groups are then a routing convenience, not isolation. The send-side rule still holds, so a
client that never joined still cannot inject into a group. See
[known-issues.md](known-issues.md) KI-2.

`AuthoriseGroupJoinAsync` (`MeshHub.cs:1487-1568`) wraps the callback with three protections. Compare it
with the four in [Authentication](#authentication) above — the **missing** one is the point:

| Protection | Mechanism | Source |
|---|---|---|
| Time bound | `WaitAsync(_groupAuthorisationTimeout)` (default **10 s**), with a sync fast path that skips the `AsTask()` entirely | `MeshHub.cs:1511-1522` |
| Throw isolation | any exception is logged at `Error` and becomes a refusal rather than faulting the receive loop (callback boundary) | `MeshHub.cs:1558-1567` |
| Cancellation isolation | an `OperationCanceledException` raised *inside* the callback — as opposed to the client disconnecting or the hub shutting down — becomes a logged refusal, not a dropped connection | `MeshHub.cs:1545-1557` |
| ~~Concurrency cap~~ | **deliberately absent.** The callback runs on input from an *already-admitted* client and is driven from that client's own receive loop, which reads nothing further from it until the callback returns — so one client cannot have two decisions in flight. The authentication semaphore exists because that callback runs on **un**authenticated input, where any peer reaching the port can drive it; this one is not in that position. | comment at `MeshHub.cs:1499-1510` |

Every refusal path results in a `GroupJoinRefused` frame to the client and **no** membership. The client
cannot distinguish a policy `false` from a throwing, cancelling or slow authoriser — as with
authentication, that is deliberate, and hub-side logs are the only diagnostic (`Warning` for refusal,
timeout and cancellation; `Error` for a throw).

**A refusal revokes.** Before replying, `JoinGroupAsync` calls `LeaveGroup` (`MeshHub.cs:1456`). This
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
  `ForLog`, `MeshHub.cs:1470`, `:1476-1481`). The refusal paths log at `Warning`/`Error` and are
  reachable at will by any admitted client, so an unclipped name would let one client choose how much
  the hub writes. If you add a log line on this path, run the name through `ForLog`.

---

## Contracts & gotchas

- **Ownership:** the hub owns every accepted transport and the listener; all are disposed on teardown.
- **Delivery is best-effort.** See the routing table — unknown recipients and full queues drop frames.
  This is the single most important behavioural fact about the hub. [known-issues.md](known-issues.md) KI-1, KI-4.
- **The shutdown writes the `Disconnect` frame directly to each transport** (`StopCoreAsync`,
  `MeshHub.cs:426-436`), bypassing the send loop, concurrently with any in-flight send-loop write. This
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
- **Name check is in chars, not bytes** (`clientName.Length`, `MeshHub.cs:835`), matching the client.
  A 256-char name can exceed 256 UTF-8 bytes. [known-issues.md](known-issues.md) KI-3.
- **Group names in `Join`/`Leave` frames are unbounded** (whole remainder of the frame), but
  `SendToGroup` frames cap the name at `ushort.MaxValue`. Minor asymmetry — a name longer than 65 535
  bytes could be joined but never targeted by a group send. [known-issues.md](known-issues.md) KI-8.
- **Lookup responses are handled inline in the receive loop** and awaited on `clientCts.Token`
  (`MeshHub.cs:991`) — a slow/blocked transport write here stalls that client's inbound processing.
- **Malformed frames are silently ignored** — the dispatch chain is a series of length-guarded
  `else if`s with no terminal `else`. [known-issues.md](known-issues.md) KI-9. A malformed *registration*
  frame is dropped the same way, without an error reply.
- **A hub with no authenticator admits anyone who can reach the listener.** The seam exists; using it is
  opt-in. [known-issues.md](known-issues.md) KI-2.
- **A group send from a non-member is dropped, silently and unconditionally.** No error frame, a `Debug`
  log only (`MeshHub.cs:1693-1699`). This holds with or without a `groupAuthoriser`, and it is a
  behavioural break for any client that used to publish to a group without joining it — such a client
  must now join, and will then also start receiving that group's traffic. There is no send-only
  capability. [known-issues.md](known-issues.md) KI-2.
- **A refused join revokes an existing membership.** `JoinGroupAsync` calls `LeaveGroup` before replying
  (`MeshHub.cs:1456`), so re-joining a group you are already in is a live re-authorisation, not a no-op.
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
It now also takes `groupAuthoriser` / `groupAuthorisationTimeout` (`Fixtures/MeshHubFixture.cs:27-28`,
passed through at `:53-54`) and carries frame builders for the group opcodes:
`CreateJoinGroupRequest(name)`, `CreateGroupMessage(name, message)`, `CreateDirectMessage(id, message)`
and `CreateLookupRequest(correlationId, name)`. Since PR #68 (issue #16) it also takes
`maxConnectionsPerRemoteEndpoint` (`:29`, passed through at `:55`), and both `CreateMockTransport` and
`RegisterClientAsync` take an optional `IPEndPoint? remoteEndPoint` so a test can put a mock transport
behind the per-remote-endpoint cap — see [testing.md](testing.md#per-remote-endpoint-connection-cap).

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
