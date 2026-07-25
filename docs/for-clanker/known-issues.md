# Known issues, foot-guns & load-bearing behaviour

[← back to index](../for-clanker.md)

The single complete register of what will bite a change in this codebase. Most-impactful first. Every
entry is grounded in the code. Many of these are **intentional design choices**, not bugs — but each is
a trap for code that assumes the obvious, so treat them as constraints to work *with*. "Severity" rates
the risk to a change, not a claim that the code is defective.

| ID | Title | Where | Severity | Status |
|---|---|---|---|---|
| KI-1 | Full outbound queue silently drops the frame | `MeshHub.cs:1360`, `:1386`, `:1710` | high (correctness) | open — by design |
| KI-2 | Open admission by default; authorisation covers groups only | system-wide | high (security) | **partly addressed** — authentication seam (PR #56), transport TLS (PR #59), group authorisation seam + unconditional group-send membership (PR #66); open admission and cleartext remain the *defaults* |
| KI-3 | Client-name length checked in chars, not UTF-8 bytes | `MeshHub.cs:818`, `MeshClient.cs:102` | medium (correctness) | open |
| KI-4 | Unknown recipient drops the message silently | `MeshHub.cs:1346-1352` | medium (correctness) | open — by design |
| KI-5 | Delivery is unordered/unacked across the fan-out; no persistence | system-wide | medium (correctness) | open — by design |
| KI-6 | `StopAsync` writes `Disconnect` outside the send loop | `MeshHub.cs:427-437` | medium (correctness) | open |
| KI-7 | `InMemoryTransport` uses unbounded channels (no back-pressure) | `InMemoryTransport.cs:34-39` | medium (perf) | open — by design |
| KI-8 | Group-name length asymmetry (join unbounded, send ≤ 65 535) | `MeshHub.cs:923`, `MeshClient.cs:345-354` | low (correctness) | open — now also reaches the group authoriser and the refusal frame |
| KI-9 | Malformed/short/unknown frames silently ignored | `MeshHub.cs:909-985`, `MeshClient.cs:714-835` | medium (maintainability) | open — by design, and now load-bearing for additive opcodes |
| KI-10 | `JoinedGroups` can drift from the hub | `MeshClient.cs:392-430`, `MeshClientReconnector.cs:286-310` | low (correctness) | **largely addressed** — auto-rejoin landed in PR #52; PR #66 made a refusal correct the client's view. Residual drift only, see KI-27 |
| KI-11 | Heartbeat eviction was off-by-one vs "max missed" | `MeshHub.cs:1320` | low (behaviour) | **fixed** — corrected by PR #61 (issue #9); inclusive comparison, do not loosen |
| KI-12 | Only one client lookup in flight at a time | `MeshClient.cs:390-472` | low (perf) | open — by design |
| KI-13 | Event `Data` is a view over the received frame | `MeshClient.cs:586`, `:612` | low (correctness) | open |
| KI-14 | Protocol v3 is a hard break: v2 peers are refused, and `ConnectAsync` is source-breaking | `Messages/Protocol.cs:5`, `IMeshClient.cs:49` | high (compatibility) | open — by design |
| KI-15 | `AuthenticationFailed` conflates refusal, throw, timeout and slot starvation | `MeshHub.cs:1127-1205` | medium (maintainability) | open — by design |
| KI-16 | A reconnector's credential is fixed at construction and cannot be rotated | `MeshClientReconnector.cs:87`, `:143`, `:222` | medium (correctness) | open |
| KI-17 | Sender identity is hop-by-hop: a compromised hub can forge any sender | system-wide | medium (security) | open — by design |
| KI-18 | A failed TLS handshake is silent — the hub sees nothing at all | `TcpTransportListener.cs:583-597` | medium (maintainability) | open — by design |
| KI-19 | A queued reconnect signal means the connection *was* lost, not that it still is | `MeshClientReconnector.cs:247-250`, `:174-177` | high (correctness) | **load-bearing** — guard added by PR #60; do not remove |
| KI-20 | The caller owns the transport until `ConnectAsync` accepts it | `MeshClient.cs:166-180`, `MeshClientReconnector.cs:263-270`, `:145-159` | medium (resource correctness) | **partly addressed** — retry path fixed by PR #60; `StartAsync` still leaks |
| KI-21 | A `DisconnectAsync` arriving after the teardown publishes its state still raises `Disconnected` | `MeshClient.cs:913-934`, `:289-292` | low (correctness) | open — **accepted residual** of PR #62 (issue #10); the claim protocol around it is **load-bearing**, do not remove |
| KI-22 | A listener disposed under a pending accept must end it with `ObjectDisposedException` | `Transport/ITransportListener.cs:6-22`, `TcpTransportListener.cs:242-297`, `:307-380`, `InMemoryTransportListener.cs:57`, `:75-90` | high (correctness) | **fixed** — PR #63 (issue #11); the contract and both translations are **load-bearing**, do not remove |
| KI-23 | `MeshHub.StopAsync` was not safe under concurrent invocation | `MeshHub.cs:89`, `:382-411`, `:417-448`, `:454-502` | high (correctness) | **fixed** — PR #64 (issue #12); the lock discipline and the shared `_stopTask` are **load-bearing**, do not remove |
| KI-24 | The shutdown's disconnect notification is sequential and has no send timeout | `MeshHub.cs:427-437` | medium (perf / availability) | open — pre-existing, unchanged by PR #64 |
| KI-25 | A stopped hub is not restartable in general | `MeshHub.cs:382`, `Transport/ITransportListener.cs` | medium (maintainability) | open — by design, documented on `IMeshHub.StopAsync` |
| KI-26 | `maxClients` was a soft cap — non-atomic check-then-act let a burst overshoot it | `MeshHub.cs:75`, `:833-837`, `:854-860`, `:1043-1046`, `:1081`, `:1110` | high (correctness) | **fixed** — PR #65 (issue #13); the atomic claim, its position *after* authentication, and the claim/release pairing are **load-bearing**, do not remove |
| KI-27 | `GroupJoinRefused` carries no correlation identifier | `MeshHub.cs:1567-1569`, `MeshClient.cs:768-780` | low (correctness) | open — **accepted**, fail-safe by construction |
| KI-28 | The group authoriser has no concurrency cap, and the timeout does not stop the callback | `MeshHub.cs:1482-1512` | medium (perf / availability) | open — **deliberate**; bounding it is the integrator's job |
| KI-29 | `MeshHub` had unbounded resource-consumption defaults | `MeshHub.cs:24-38`, `:181-262`, `:602-753` | high (availability / security) | **fixed** — PR #68 (issue #16, draft); the new defaults are a **breaking behavioural change** for any hub relying on the old unlimited/disabled ones |

---

### KI-1 — Full outbound queue silently drops the frame
- **Where:** `RouteMessage` `MeshHub.cs:1360`, `BroadcastMessage` `:1386`, `SendToGroup` `:1710`. Queue is
  a bounded `Channel<byte[]>` of capacity **1024** per connection (`MeshHub.cs:1733`, `:1759`).
- **Why it bites:** the hub delivers with `TryWrite`. If a recipient's consumer (its transport) is slow
  enough to fill 1024 queued frames, every further frame for it is **dropped and logged at Warning** —
  no exception, no back-pressure to the sender. A `SendAsync` that "succeeds" guarantees only that the
  hub accepted the frame, never that it was queued for, let alone delivered to, the recipient.
- **What to do:** treat delivery as lossy. If you need reliability, build acks/retries at the
  application layer. If you raise the cap or change to blocking writes, understand you are trading drop
  for head-of-line blocking of the router — do it deliberately and test under a stalled consumer.

### KI-2 — Open admission by default; authorisation covers groups only
- **Where:** system-wide; registration `MeshHub.cs:764-878`, authentication `MeshHub.cs:1127-1205`,
  group authorisation `MeshHub.cs:1404-1551`, group-send membership `MeshHub.cs:1663-1682`.
- **Status:** the *authentication* half has a supported seam (PR #56, protocol version 3), the
  *transport* half can be secured with TLS (PR #59), and since PR #66 (issue #14) there is an
  *authorisation* half — but it covers **groups only**. All three seams are **opt-in** except the
  group-send membership rule; the defaults are still open admission over a cleartext socket.
- **What exists:**
  - **Application-level authentication.** Pass a `ClientAuthenticator` to the `MeshHub` constructor and
    it is invoked for every registration with the client's name and an opaque credential; returning
    `false` refuses the client with `RegistrationErrorCode.AuthenticationFailed`. Clients supply the
    credential via `ConnectAsync(transport, name, credential)` and `MeshClientReconnector`'s
    `credential` parameter, which re-sends it on every reconnect. See [hub.md](hub.md#authentication).
  - **Transport-level confidentiality, integrity and peer authentication.** Pass
    `SslServerAuthenticationOptions` to `TcpTransportListener` and `SslClientAuthenticationOptions` to
    `TcpTransport.ConnectAsync`; add `ClientCertificateRequired` for mutual TLS. Framing is unchanged.
    See [transport.md](transport.md#turning-tls-on-both-ends).
  - **Group authorisation (PR #66).** Two rules:
    - **A group send always requires membership of the target group**, with or without a callback. The
      hub drops a `GroupMessage` from a non-member (`MeshHub.cs:1666`, `:1676-1682`), so a client cannot
      inject a frame into a group it never joined. Membership is the **single** capability — there is no
      send-only permission, so a client that previously published to a group without joining it must now
      join, and will then also receive that group's traffic.
    - **Who may join is yours to decide.** Pass a `GroupAuthoriser` to the `MeshHub` constructor and it
      gates every join, including the re-joins that follow a reconnect. `false` refuses, and the client
      is told through `GroupJoinRefused`. See [hub.md](hub.md#group-authorisation).
- **Why it still bites:**
  1. **No authenticator is the default.** A `MeshHub` constructed without one admits any peer that
     completes the handshake — exactly the pre-v3 behaviour. Nothing warns you.
  2. **Authorisation stops at groups, and is itself opt-in.** With no `GroupAuthoriser` the hub
     authorises no joins and **any client may join any group** — groups are then a routing convenience,
     not isolation, and nothing warns you. Even *with* one, an admitted client can still enumerate peers
     via `GetClientIdByNameAsync`, broadcast to everyone, and direct-send to any id it can resolve. There
     are no per-client capabilities outside groups, and the hub passes identity into **group** routing
     only.
  3. **Names are not bound to identity.** The authenticator sees the requested name, but the hub's only
     name rule is uniqueness. Unless *your* callback ties credential to name, an authenticated client
     may register under any unused name, including one it has no right to. **This is what a
     `GroupAuthoriser` inherits**: `GroupJoinContext.ClientName` is only as trustworthy as the
     authenticator that admitted it, and with no authenticator it is self-asserted — so a group policy
     keyed on names is only as strong as the authentication underneath it.
  4. **Confidentiality and integrity are opt-in, and nothing warns you.** A listener constructed
     without `tlsOptions`, or a client using the three-argument `TcpTransport.ConnectAsync(host, port,
     ct)`, is **cleartext**: client names, assigned ids, group names and every message payload cross the
     wire in the clear and can be modified in flight. There is no log line, no property check and no
     handshake failure to tell you — only `TcpTransport.IsEncrypted` (`TcpTransport.cs:61`), which you
     have to assert yourself.
  5. **Even with TLS, sender identity is not end to end.** TLS secures each client–hub hop separately;
     a delivered message's sender id is still asserted by the hub rather than signed by the sending
     client. See KI-17.
  6. **The group-send membership rule is a silent behavioural break.** It shipped **without** a protocol
     version bump, so an older client that published to a group it had never joined still connects,
     still sends, and is simply never delivered — no error frame, a hub-side `Debug` log only. If you
     are upgrading a deployment, audit for publish-without-join before you roll the hub.
- **What to do:** supply an authenticator for anything beyond a trusted network, and bind the credential
  to the requested `ClientName` inside it if names are meaningful to your application. Supply a
  `GroupAuthoriser` if groups carry any isolation meaning at all — without one they carry none. Configure TLS on
  both ends where traffic crosses an untrusted segment — or run inside an already-encrypted channel
  (VPN, service-mesh mTLS, a TLS-terminating proxy) if that is how your deployment already works. Assert
  `IsEncrypted` in a start-up check so a mis-wired deployment fails loudly rather than quietly running
  in the clear. Note the `TcpTransportListener(int port)` convenience constructor binds loopback only
  ([transport.md](transport.md)), so remote exposure is a deliberate act.
- **What not to do:** do not treat an authenticator's `true` as an authorisation decision, and do not put
  expensive or non-constant-time credential checks in the callback without reading
  [hub.md](hub.md#authentication) first — it runs on unauthenticated input. Do not treat groups as an
  isolation boundary unless a `GroupAuthoriser` is configured, and do not assume the group seam extends
  to direct sends, broadcast or lookup — it does not.

### KI-3 — Client-name length checked in chars, not UTF-8 bytes
- **Where:** hub `clientName.Length > Protocol.MaxClientNameLength` (`MeshHub.cs:818`); client
  `clientName.Length > Protocol.MaxClientNameLength` (`MeshClient.cs:102`). `MaxClientNameLength = 256`.
- **Why it bites:** `.Length` counts UTF-16 code units, not encoded bytes. A 256-"character" name of
  multi-byte code points encodes to well over 256 bytes on the wire, and a name of astral characters
  (surrogate pairs) counts each pair as 2. Both sides use the same check so they agree, but any external
  reimplementation that validates bytes will disagree. Not a buffer risk (frames are 1 MiB-bounded), but
  a spec ambiguity.
- **What to do:** if you tighten this, decide bytes-vs-chars explicitly and change both sides + the wire
  docs together.

### KI-4 — Unknown recipient drops the message silently
- **Where:** `RouteMessage` `MeshHub.cs:1346-1352` (logs `Debug`, returns). `SendToGroup` drops silently
  on three paths: the group does not exist (`MeshHub.cs:1652`, plain `return`, **no log**), **the sender
  is not a member** (`:1676-1682`, logs `Debug`), and the sender is the group's only member
  (`:1685-1689`, no log).
- **Why it bites:** sending to a stale/never-registered id, or to a group nobody has joined, is a no-op.
  The sender gets no signal. Combined with KI-1, "message sent" never implies "message delivered".
  Since PR #66 the **commonest** cause of a silently-dropped group send is the sender not being a member
  — including the window after `JoinGroupAsync` returned but before the hub applied the join, and the
  case where the join was refused outright. The old "empty group" early return is gone; an empty group is
  simply one you cannot be a member of.
- **What to do:** resolve ids via `GetClientIdByNameAsync` immediately before sending if freshness
  matters, and design for silent loss. For groups, join before you send, and subscribe to
  `GroupJoinRefused` so a refusal is not mistaken for a delivery problem.

### KI-5 — Unordered/unacked delivery, no persistence
- **Where:** system-wide. Fan-out is per-recipient queues; there is no sequence number, ack, or store.
- **Why it bites:** ordering holds only within a single connection's stream. Across a broadcast/group
  fan-out there is no global order, and nothing is retained if a client is offline.
- **What to do:** layer any ordering/durability guarantees on top; do not assume them.

### KI-6 — `StopAsync` writes `Disconnect` outside the send loop
- **Where:** `MeshHub.cs:427-437` (in `StopCoreAsync` since PR #64) — iterates `_clients` and calls
  `client.Transport.SendAsync` directly, concurrently with each connection's still-running send loop.
- **Why it bites:** two writers hit the same transport at once. This is **safe for `TcpTransport`**
  (internal `SemaphoreSlim` write lock, `TcpTransport.cs:32`) and any transport that honours the
  "`SendAsync` must be concurrency-safe" contract — but a custom transport that serialises incorrectly
  will interleave/corrupt framing during shutdown.
- **What to do:** if you write a custom transport, make `SendAsync` genuinely concurrency-safe. Don't
  "optimise" `TcpTransport`'s write lock away.

### KI-7 — `InMemoryTransport` unbounded channels
- **Where:** `InMemoryTransport.CreatePair` uses `Channel.CreateUnbounded` (`InMemoryTransport.cs:34-35`);
  `InMemoryTransportListener` pending-connections channel is also unbounded
  (`InMemoryTransportListener.cs:12`) — disposal now drains and closes whatever accumulated there
  (KI-22), but nothing bounds it while the listener is alive.
- **Why it bites:** no back-pressure — a fast producer against a stalled consumer grows memory without
  bound. Fine for tests and cooperative in-process use; not for adversarial/production load.
- **What to do:** use TCP (or a bounded custom transport) where back-pressure matters.

### KI-8 — Group-name length asymmetry
- **Where:** `Join`/`Leave` frames carry the name as the whole frame remainder (`MeshHub.cs:923`, `:931`)
  — effectively bounded only by the 1 MiB frame cap. `GroupMessage`/`DeliverGroupMessage` encode the
  name length as a `u16`, and the client rejects names over `ushort.MaxValue` (`MeshClient.cs:345-354`).
- **Why it bites:** a group name between 65 536 bytes and 1 MiB can be joined but never targeted by a
  group send from the stock client. An edge case, but a real inconsistency.
- **Two consequences PR #66 added, both already mitigated in the code** — know them before you touch
  either path:
  - **The unbounded name reaches your `GroupAuthoriser`** as `GroupJoinContext.GroupName`. Do not use it
    as a dictionary key, a log field or a filesystem path without bounding it yourself.
  - **The unbounded name reaches the hub's log lines.** The refusal paths log at `Warning`/`Error` and
    are reachable at will by any admitted client, so an unclipped name would let one client choose how
    much the hub writes. `ForLog` clips to 64 characters (`MeshHub.cs:1453`, `:1459-1464`); run any new
    log line on this path through it. Note `MeshClient` does **not** clip (`MeshClient.cs:782`) — the
    name there came from your own hub.
- **What to do:** keep group names short. If you unify the limit, apply it at join time too.

### KI-9 — Malformed/short/unknown frames silently ignored
- **Where:** dispatch ladders `MeshHub.cs:909-985` and `MeshClient.cs:714-835` — length-guarded
  `else if` chains with no terminal `else`.
- **Why it bites:** a frame that is too short for its opcode, or has an unknown opcode, is dropped with
  no error and no warning-level log. A framing/offset bug manifests as "nothing happens", which is hard
  to diagnose.
- **Also applies to registration** (`MeshHub.cs:786-814`): a frame under 2 bytes, under 4 bytes, with a
  zero name length, or with a name length running past the payload, drops the connection with **no error
  frame**. The client sees a closed connection rather than a `RegistrationRefusedException`, so a
  framing bug there looks like "the hub is down".
- **It is also load-bearing, not merely tolerated.** The fall-through is what let `GroupJoinRefused`
  (`0x10`) be added **within** protocol version 3: an older client that receives one ignores it, which is
  indistinguishable from the opcode never having existed. Any future hub → client opcode may rely on the
  same property — but a *client → hub* opcode may not, and neither may a change to an existing frame's
  layout. See [protocol.md](protocol.md#additive-opcodes-within-a-version).
- **What to do:** when adding an opcode, add the guard *and* the branch on the correct side at the exact
  offsets in [protocol.md](protocol.md). When debugging missing messages, check framing first.

### KI-10 — `JoinedGroups` drift — **LARGELY ADDRESSED (PR #52, then PR #66)**
- **Where:** `JoinGroupAsync` `MeshClient.cs:392-430`, `LeaveGroupAsync` `:433-446`, refusal handling
  `:768-793`; reconnector restore `MeshClientReconnector.cs:286-310`.
- **Status:** the two claims this entry originally made are **both now false** and are corrected here
  rather than deleted, because older notes still repeat them:
  - *"No auto-rejoin on reconnect"* — wrong since **PR #52**. `restoreGroupMembership` defaults to
    `true` and `RestoreGroupMembershipAsync` re-joins each group over the wire
    (`MeshClientReconnector.cs:305`).
  - *"The client records membership after sending"* — wrong since **PR #66** for joins. `JoinGroupAsync`
    now records **before** sending (`MeshClient.cs:403-411`) precisely so a refusal that arrives first is
    not undone, and rolls the record back on send failure only when that call is what added it
    (`:413-429`). `LeaveGroupAsync` still sends first, then removes (`:442-445`).
- **What still bites (the residual):**
  1. **A lost or refused frame can still diverge the two views.** Membership is fire-and-forget; there is
     no ack for a *successful* join. A refusal now corrects the client (`MeshClient.cs:768-780` removes
     the group before raising the event), so the drift is one-directional in the safe direction — the
     client may believe it is in a group it is not, but a hub-side refusal will correct it.
  2. **A refusal has no correlation id**, so it can clear a membership a *later* join legitimately
     obtained. See KI-27.
  3. **A restored join can be refused**, and is not retried. After a reconnect, membership is whatever
     the authoriser allows now — not what it was before the drop.
- **What to do:** treat `JoinGroupAsync` returning as "requested", not "joined". Subscribe to
  `GroupJoinRefused` if membership matters, and re-check after a reconnect rather than assuming
  restoration succeeded (see [client.md](client.md#group-membership)).

### KI-11 — Heartbeat eviction off-by-one — **FIXED (PR #61, issue #9)**
- **Where:** `MonitorHeartbeatAsync`, `MeshHub.cs:1320`.
- **Status:** resolved by PR #61. The comparison is now `missedHeartbeats >= _maxMissedHeartbeats`
  (it was `>`), so eviction fires on the **Nth** consecutive silent interval, which is what the
  parameter name and the XML docs always claimed. Retained here because the behaviour of any hub built
  before that commit differs, and because the fix is easy to regress.
- **What it was:** "max missed heartbeats = N" tolerated N idle intervals and evicted on the (N+1)th.
  With the default 2, a silent client was pinged twice and evicted after 3 idle intervals, so sizing a
  client `idleTimeout` or an SLA around the literal parameter value came out one interval short.
- **What it is now:** a silent client is evicted on the Nth silent interval and probed **N − 1** times
  on the way there, because the threshold check precedes the `Ping` enqueue (`MeshHub.cs:1320-1332`). The
  ping cadence for a live client is unchanged — only the eviction point moved. **N = 1 therefore never
  probes at all**; the constructor logs a `Warning` for that combination (`MeshHub.cs:270-283`). Full
  schedule table in [hub.md](hub.md#heartbeat-schedule).
- **What to do:** if you had compensated for the old +1 in a deployment's `idleTimeout`, hub-side
  eviction now happens one interval sooner than it used to — re-check that the client's `idleTimeout`
  is still comfortably above `heartbeatInterval`. Do not loosen the comparison back to `>`; three tests
  pin the schedule (`MeshHubTests.cs:2057`, `:2105`, `:2149`), and they assert the **ping count** at
  eviction precisely because an "was it evicted?" assertion cannot distinguish N from N+1.
- **Note:** since PR #68 (issue #16) `heartbeatInterval` defaults to 30 seconds rather than `null`, so
  idle eviction — and this schedule — now applies to every hub that does not explicitly pass
  `Timeout.InfiniteTimeSpan`. See [hub.md](hub.md#using-it-efficiently) and the constructor row in
  [for-clanker.md](../for-clanker.md#5-configuration--environment).

### KI-12 — One client lookup in flight at a time
- **Where:** `GetClientIdByNameAsync` serialised by `_lookupLock` (`SemaphoreSlim(1,1)`) with a
  single-slot `_pendingLookup` (`MeshClient.cs:390-472`).
- **Why it bites:** concurrent lookups on the same client queue rather than pipeline — throughput of
  name resolution is one round-trip at a time. Correct (correlation ids prevent cross-talk), just not
  parallel.
- **What to do:** batch/caches name→id at the app layer if you resolve many names hot. Don't remove the
  correlation-id guard if you parallelise — it is what prevents a cancelled lookup resolving a later one.

### KI-13 — Event `Data` is a view over the received frame
- **Where:** `MessageReceived`/`GroupMessageReceived` args carry `ReadOnlyMemory<byte>` slices of the
  frame (`MeshClient.cs:586`, `:612`).
- **Why it bites:** the memory is only contractually valid during the handler. It happens to be backed
  by a fresh per-frame `byte[]` today (`TcpTransport.ReceiveAsync` allocates per frame), so retaining it
  works — but a future pooled-buffer transport would invalidate anything you kept.
- **What to do:** copy (`.ToArray()` / `Span.CopyTo`) if you retain the payload past the handler.

### KI-14 — Protocol v3 is a hard break, on the wire and in source
- **Where:** `Protocol.Version = 3` (`Messages/Protocol.cs:5`); the reshaped `RegistrationRequest`
  (`MeshHub.cs:801-816`, `MeshClient.cs:184-192`); the signature change on `IMeshClient.ConnectAsync`
  (`IMeshClient.cs:49`) and `MeshClient.ConnectAsync` (`MeshClient.cs:147`).
- **Why it bites — two distinct ways:**
  1. **On the wire.** A v2 client against a v3 hub is refused with `UnsupportedProtocolVersion`; a v3
     client against a v2 hub gets its length-prefixed name interpreted as a raw UTF-8 name. There is no
     negotiation and no compatibility shim. **Hub and clients must be upgraded together** — a rolling
     upgrade of a live mesh will refuse connections until both sides are on v3.
  2. **In source.** `credential` was inserted **before** the `CancellationToken`, so any call site
     written as `ConnectAsync(transport, name, cancellationToken)` no longer compiles. It fails loudly
     rather than silently binding, but every call site must be revisited.
- **What to do:** upgrade hub and clients in one step. Fix broken call sites by naming the token
  (`cancellationToken: ct`) or by passing a credential. If you ever need on-the-wire compatibility, it
  has to be built as version negotiation in the handshake — nothing in the current design supports it.

### KI-15 — `AuthenticationFailed` conflates every non-success outcome
- **Where:** `AuthenticateAsync` (`MeshHub.cs:1127-1205`) returns `false` for a refusal, a throw, a
  cancellation inside the callback, a callback that exceeds `registrationTimeout`, and a failure to
  acquire an authentication slot. The caller sends the same
  `Error(AuthenticationFailed)` in all cases (`MeshHub.cs:843-844`).
- **Why it bites:** a client that catches `RegistrationRefusedException` with
  `ErrorCode == AuthenticationFailed` **cannot tell "your credential is wrong" from "the hub's
  authenticator is broken, slow or saturated"**. A client that treats it as terminal will give up on a
  transient hub problem; a client that retries hard will hammer a hub that is already overloaded. The
  distinction exists **only in the hub's logs**, at `Warning`/`Error`, with distinct messages per cause.
- **What to do:** when diagnosing, read the hub logs — the message text identifies the cause. When
  writing client retry logic, back off rather than assuming either extreme. This conflation is
  deliberate (an error code that distinguished them would leak authenticator state to unauthenticated
  peers); do not "fix" it by adding codes without thinking about what that discloses.

### KI-16 — A reconnector's credential is fixed at construction
- **Where:** `MeshClientReconnector` captures `credential` into a `readonly` field
  (`MeshClientReconnector.cs:87`, `:113`) and replays it on every connect and reconnect (`:151`, `:261`).
- **Why it bites:** there is no setter and no factory callback for the credential, unlike
  `transportFactory` which *is* re-invoked per attempt. If the credential expires or is rotated
  mid-session, every subsequent reconnect fails with `AuthenticationFailed` and
  `ConnectWithRetryAsync` keeps retrying at `retryDelay` **indefinitely** — the reconnector never gives
  up and never surfaces the cause to the application beyond logs.
- **What to do:** for expiring credentials, do not rely on the reconnector to carry them. Either use a
  long-lived credential, or dispose the reconnector and construct a new one with a fresh credential when
  rotation happens. If you need this properly, the shape to add is a credential factory mirroring
  `transportFactory` — note that would be a constructor change on a public sealed type.

### KI-17 — Sender identity is hop-by-hop, not end to end
- **Where:** system-wide. The hub stamps the sender id into every delivery frame it builds —
  `RouteMessage` `MeshHub.cs:1357`, `BroadcastMessage` `:1375`, `SendToGroup` `:1697` — from its own record
  of the connection, and nothing in `Messages/` carries a signature. TLS, where configured, secures the
  client↔hub connection only.
- **Why it bites:** TLS makes it tempting to conclude the mesh is now "secure end to end". It is not.
  Each client authenticates *the hub* (and, under mTLS, the hub authenticates *that client*), but a
  recipient's trust in `SenderId` is entirely trust in the hub. **A compromised or malicious hub can
  forge any sender id**, and mutual TLS does not change that — the hub is a full participant, not a
  transparent pipe. Nor does TLS help with the authorisation gap in KI-2: any admitted client can still
  message any other.
- **What to do:** if recipients must be able to attribute a message to its sender independently of the
  hub, sign the payload at the application layer and verify it in your `MessageReceived` handler. Do not
  present transport TLS to stakeholders as end-to-end authenticity. Treat the hub as trusted
  infrastructure and scope its blast radius accordingly.

### KI-18 — A failed TLS handshake is invisible to everything above the transport
- **Where:** `TcpTransportListener.HandshakeAsync`'s catch-all (`TcpTransportListener.cs:583-597`) —
  disposes the connection and swallows the cause. The transport layer has no logger.
- **Why it bites:** an untrusted or absent client certificate, a protocol/cipher mismatch, a peer that
  exceeded `tlsHandshakeTimeout`, a cleartext client dialling a TLS listener, and a peer that reset all
  produce **exactly the same observable outcome: nothing**. The hub never sees a connection, logs no
  error, and the client gets a generic `AuthenticationException` or a closed socket. "The client cannot
  connect and neither side says why" is the expected symptom of a TLS misconfiguration here, and it will
  cost you time if you do not know that going in. The same silence covers the pending-bound shedding
  path (`:500-504`), where a connection is dropped before a handshake is even attempted.
- **What to do:** diagnose from the *client* side first — its exception is the only signal in the
  system. Reproduce against a known-good configuration (`TcpTransportTlsTests` is the reference), and
  narrow with `SSLKEYLOGFILE`/platform TLS tracing rather than expecting library logs. If you add
  logging here, note that it would be the first `ILogger` dependency in the transport layer — a design
  change, not a tweak, and one that logs unauthenticated peer input.
- **What not to do:** do not narrow that `catch` so a handshake failure escapes. It runs on a background
  pump; an escaping exception there kills the pump and, via the channel completion, the whole listener —
  which is precisely the "one bad peer stops the hub" outcome the design avoids.

### KI-19 — A queued reconnect signal means the connection *was* lost, not that it still is
- **Where:** the revalidation guard at the top of `ConnectWithRetryAsync`
  (`MeshClientReconnector.cs:247-250`); the signal sources are `OnDisconnected` (`:199`) and the
  post-subscription state re-check in `StartAsync` (`:174-177`).
- **Why it bites:** the reconnector's trigger is **level-based, not edge-based**. A signal sitting in
  the capacity-1 channel is a record that a drop *happened*, not a guarantee the client is still down by
  the time the loop services it. Three ways a signal goes stale:
  1. **One drop, two signals.** `Client.IsConnected` is false during `Disconnecting` as well as
     `Disconnected`, so a teardown that straddles the subscription line is seen *both* by `StartAsync`'s
     state re-check and, moments later, by `OnDisconnected` when the event finally fires. The channel is
     `DropWrite` capacity 1, so it coalesces only signals that overlap *in the queue* — these two do
     not, and the second survives as a duplicate for a drop already serviced.
  2. **An application handler reconnected from inside `Disconnected`.** `MeshClient` explicitly supports
     this (`MeshClient.cs:227-237`), so the connection can be live again before the loop wakes.
  3. **An earlier pass already recovered it.**
- **What it costs if the guard goes:** `MeshClient.ConnectAsync` **refuses** a connect unless the client
  is fully `Disconnected` (`MeshClient.cs:166-176`), so servicing a stale signal does not merely waste a
  round trip — it throws `InvalidOperationException` every time, which
  `ConnectWithRetryAsync`'s catch-all treats as a retryable failure. The loop then retries an
  already-connected client **for ever** at `retryDelay`, building and discarding a transport each pass.
  This is the regression PR #60's second commit exists to prevent;
  `StartAsync_DropSignalledTwice_SettlesWithoutRetryingConnectedClient`
  (`MeshClientReconnectorTests.cs:409`) is the test that pins it.
- **What to do:** treat "revalidate the goal before acting on the signal" as the invariant. Any new
  reconnect trigger you add (a heartbeat watchdog, a manual `Reconnect()` method) must go through the
  same channel and inherit the same guard. If you ever need to distinguish "reconnect happened" from
  "signal was stale", note that the current code returns from `ConnectWithRetryAsync` identically in both
  cases, so `Reconnected` fires for a stale signal too — see the gotchas under
  [`MeshClientReconnector`](client.md#meshclientreconnector).
- **What not to do:** do not "simplify" the guard away on the reasoning that the coalescing channel
  already deduplicates. It deduplicates concurrent writes, not a signal that arrives after the previous
  one was drained.

### KI-20 — The caller owns the transport until `ConnectAsync` accepts it
- **Where:** `MeshClient.ConnectAsync` adopts the transport at `MeshClient.cs:179`, *after* its argument
  and state validation (`:153-173`). A throw from that validation therefore leaves the transport
  unowned and unclosed; a throw after adoption is cleaned up by `CleanUpAsync` (`:241`, disposal at
  `:616-619`). The reconnector's retry path handles the gap (`MeshClientReconnector.cs:263-270`).
- **Why it bites:** the reachable case is not a programming error. A reconnect attempt racing a teardown
  hits the `ConnectionState.Disconnecting` guard (`MeshClient.cs:173`) and is rejected with
  `InvalidOperationException` before adoption — so before PR #60 every such attempt **abandoned a live
  transport**, one connected socket leaked per rejected retry, on a path that retries indefinitely.
- **Two things this leaves you with:**
  1. **`StartAsync` has no equivalent guard.** Its connect (`MeshClientReconnector.cs:145-159`) resets
     the started flag and rethrows without disposing the transport. Bounded to one per call, but
     `StartAsync` is documented as retryable, so a caller looping it leaks one transport per attempt.
     *(Inference from reading both paths; no test covers it.)*
  2. **`ITransport.DisposeAsync` must now be idempotent.** The reconnector disposes on *any*
     `ConnectAsync` throw, including the post-adoption ones the client already cleaned up — the code
     relies on the second disposal being harmless. `ITransport`'s `<remarks>` documents a concurrency
     contract but says **nothing** about idempotent disposal (`Transport/ITransport.cs:3-15`). Both
     in-tree implementations happen to satisfy it — `InMemoryTransport` guards explicitly
     (`InMemoryTransport.cs:68-77`), `TcpTransport` inherits it from `Stream`/`TcpClient`/`SemaphoreSlim`
     (`TcpTransport.cs:377-382`) — but a custom transport that throws on second disposal will fail here.
     **This gap is still open.** PR #63 closed only the *listener* half: `ITransportListener` now states
     the idempotent-and-concurrency-safe disposal contract explicitly (KI-22). `ITransport` was not
     touched, so the requirement the reconnector depends on remains undocumented on the interface that
     needs it. If you write the missing `<remarks>`, mirror the wording already on
     `ITransportListener.cs:18-21`.
- **What to do:** in any new code that hands a transport to `ConnectAsync`, dispose it yourself if the
  call throws. If you write a custom `ITransport`, make `DisposeAsync` idempotent, as both shipped
  implementations are. If you fix the `StartAsync` path, mirror the retry path's shape exactly rather
  than inventing a second convention.

### KI-21 — A `DisconnectAsync` arriving after the teardown publishes its state still raises `Disconnected`
- **Where:** `HandleReceiveLoopTerminationAsync` releases `_stateLock` at `MeshClient.cs:923` and
  invokes the event at `:934`. The claim `DisconnectAsync` would need to lay is at `:289-292`.
- **Status:** **open, and deliberately so.** This is the residual window that PR #62 (issue #10)
  knowingly did not close, not an oversight. The code says as much in the XML docs on
  `HandleReceiveLoopTerminationAsync` (`MeshClient.cs:881-884`), `IMeshClient.DisconnectAsync`
  (`IMeshClient.cs:58-71`) and `IMeshClient.Disconnected` (`:186-196`), and in `README.md`.
- **Why it bites:** PR #62 made a local disconnect racing a remote drop silent whichever side wins, and
  it is tempting to read that as an absolute guarantee. It is not. The guarantee holds only up to the
  moment the teardown takes its raise decision. Concretely, `HandleReceiveLoopTerminationAsync` reads
  `_localDisconnectRequested` into `raiseDisconnected` inside the same locked block that sets
  `_state = ConnectionState.Disconnected` (`MeshClient.cs:913-923`), then **releases the lock** before
  invoking the delegate (`:932-935`). A `DisconnectAsync` entering in that gap finds the state is
  already `Disconnected`, not `Disconnecting`, so the `if (_state is ConnectionState.Disconnecting)`
  claim at `:289` does not fire. It lays no claim, returns as a genuine no-op — and the event the
  application was trying to suppress fires anyway, with `DisconnectReason.ConnectionLost`.
  The window is a handful of instructions wide, so it is rare, but it is real and it is not testable by
  the seam the PR's own tests use (those pin the *earlier* interleaving; see
  [testing.md](testing.md#testing-conventions-follow-these)).
- **Why it is not closed:** closing it would mean holding `_stateLock` across the
  `Disconnected?.Invoke` so that no `DisconnectAsync` could interleave between the decision and the
  raise. That directly contradicts a documented, supported pattern — **a handler may reconnect
  synchronously via `ConnectAsync` from inside `Disconnected`** (`IMeshClient.cs:186-196`), which is
  exactly how `MeshClientReconnector` behaves. `ConnectAsync` takes `_stateLock` itself
  (`MeshClient.cs:174`), so invoking the event under the lock would deadlock every such handler. The
  trade is deliberate: a rare spurious `Disconnected` is preferable to a guaranteed deadlock on a
  supported path.
- **What to do:**
  - Treat "no `Disconnected` after `DisconnectAsync`" as **overwhelmingly reliable, not guaranteed**.
    If your handler must be exactly-once, make it idempotent, or gate it on your own
    "I asked for this" flag set before you call `DisconnectAsync` — do not rely solely on the client's
    suppression.
  - **Do not remove the claim protocol** (`MeshClient.cs:289-292`, `:911-930`, `:187`) on the reasoning
    that it "does not fully work". It closes the wide, easily-hit window; only the narrow one remains.
    This is load-bearing in the same sense as KI-19's revalidation guard.
  - If you do attempt to close the residual window, the constraint to design against is the synchronous
    reconnect-from-handler pattern, not the lock itself. Anything that ends with the event being raised
    under `_stateLock` is wrong. Prove any change with a test that reconnects from inside `Disconnected`.

### KI-22 — A listener disposed under a pending accept must end it with `ObjectDisposedException` — **FIXED (PR #63, issue #11)**
- **Where:** the contract on `ITransportListener`'s `<remarks>` (`Transport/ITransportListener.cs:6-22`);
  the `TcpTransportListener` implementation (`:242-297` for accept, `:307-380` for disposal); the
  `InMemoryTransportListener` implementation (`InMemoryTransportListener.cs:57`, `:75-90`); the consumer
  that depends on it, `MeshHub.AcceptLoopAsync` (`MeshHub.cs:570-595`).
- **Status:** resolved by PR #63. Retained because the resulting behaviour is **load-bearing** in three
  separate places, each easy to regress, and because a custom `ITransportListener` has to satisfy the
  same contract.
- **Why the exception type is the whole issue:** `MeshHub.AcceptLoopAsync` breaks on
  `OperationCanceledException`/`ObjectDisposedException` and treats **everything else** as one bad
  connection — logged at Warning and retried, with `continue` and **no delay** (`MeshHub.cs:587-595`).
  Against a listener that is never coming back, "anything else" is therefore an unbounded hot spin that
  floods the log and pins a core. A listener that fails to report its own disposal correctly does not
  merely confuse a caller; it takes the hub with it.
- **What was wrong, and what each part now guarantees:**
  1. **The data race the issue was raised for.** `TcpTransportListener.AcceptAsync` null-checked
     `_listener` and then dereferenced it, while `DisposeAsync` set it to `null`. A dispose landing
     between the two produced a `NullReferenceException` — which the accept loop then logged and
     retried. All mutable state is now guarded by a `Lock _stateLock` (`:74`) and every entry point
     captures what it needs **once** into locals (`:290-296`). *The hub itself never triggered this — it
     cancels the accept token and awaits the loop before disposing — so the reachable case was
     standalone use that ignored the interface's "cancel first" remark. The interface now says
     implementations may not rely on callers doing so.*
  2. **The cleartext path did not translate.** `ObjectDisposedException` translation existed only on the
     TLS branch (via `ChannelClosedException`). A **cleartext** listener disposed under a pending accept
     surfaced the raw `SocketException`/`InvalidOperationException` that a stopped `TcpListener` throws —
     straight into the retry-without-delay branch. The new `internal static
     IsStoppedListenerFailure(Exception)` (`:631`) plus a `when (_disposed && …)` filter (`:503-511`)
     makes both branches end the same way, so the spin is gone from the cleartext path too. **The
     `_disposed` conjunct is as load-bearing as the filter:** without it an ordinary transient socket
     error on a healthy listener would be reported as disposal and would stop the accept loop for good.
  3. **The in-memory listener could serve a connection after disposal, and leaked the rest.**
     Completing a channel does not discard what is already buffered, so a disposed
     `InMemoryTransportListener` would still hand out a queued live connection; and its `DisposeAsync`
     was a synchronous no-op beyond `TryComplete`, so every established-but-never-accepted pair leaked
     with its client half parked on a server end nobody would read. It now checks `_disposed` **ahead of
     the started guard and ahead of the read** (`InMemoryTransportListener.cs:57`), is idempotent via
     `Interlocked.Exchange`, and drains the channel disposing each queued transport (`:82-89`).
  4. **`StartAsync` on a disposed listener bound a fresh socket.** It now throws
     `ObjectDisposedException` (`:194`), and publishes `_listener` only after a successful bind
     (`:201-206`) so a failed bind leaves the listener startable rather than permanently "already
     running".
  5. **Concurrent disposal ran the teardown twice over half-cleared state.** `DisposeAsync` is no longer
     `async`: the first caller elects a single teardown under the lock, hands it the state, clears the
     fields and stores the `Task`; everyone else awaits that same task (`:307-339`). Every caller returns
     only once teardown is complete.
- **What to do:** if you write an `ITransportListener`, satisfy all of it — pending accept ends in
  `ObjectDisposedException`, later accepts throw the same, disposal is idempotent, concurrency-safe and
  complete on return. Use the two shipped listeners as worked examples.
- **What not to do:** do not "simplify" `AcceptAsync` back to reading the fields directly; do not remove
  the `_disposed` conjunct from the cleartext filter; do not widen `IsStoppedListenerFailure`; do not
  make it `private` — it is `internal` so a test can assert against the framework directly that what a
  stopped `TcpListener` actually throws is still recognised, and the narrow third case
  (`InvalidOperationException`, "Not listening") is not reliably reachable through the listener, so
  nothing else would notice a framework change. Fifteen tests pin all of this
  (`TcpTransportListenerTests.cs`, `Transport/InMemory/InMemoryTransportListenerTests.cs` — see
  [testing.md](testing.md)).

---

### KI-23 — `MeshHub.StopAsync` was not safe under concurrent invocation — **FIXED (PR #64, issue #12)**
- **Where:** `MeshHub.cs:89` (`Lock _stateLock`), `:382-411` (`StopAsync`), `:417-448`
  (`StopCoreAsync`), `:454-502` (`ShutDownAsync`), `:311-372` (`StartAsync`), `:540-568`
  (`DisposeAsync` / `DisposeCoreAsync`).
- **Severity:** high (correctness). Every failure below is reachable from ordinary shutdown code — two
  threads calling `StopAsync`, or a `StopAsync` racing an `await using`.
- **What was wrong, and what each part now guarantees:**
  1. **The data race the issue was raised for.** `StopAsync` null-checked `_cts` and then dereferenced
     it. A concurrent caller that finished first nulled the field in between, and the second caller died
     with a `NullReferenceException`. Every lifecycle field is now guarded by `_stateLock`, and each
     entry point captures what it needs **once** into locals.
  2. **Clients were notified once per caller.** The call that finds the hub running now takes ownership
     of its state and publishes the shutdown in `_stopTask`; concurrent callers await that same task.
     The `Disconnect` frame is sent **once**, and the token source is disposed once.
  3. **A caller could return while the hub was still stopping.** Because every caller awaits the one
     shared task, each returns only once the hub has actually stopped. A call on a hub that is not
     running returns `Task.CompletedTask`.
  4. **An unfiltered transport exception abandoned the shutdown half way.** The notification's filter
     covers only `IOException`/`ObjectDisposedException`/`OperationCanceledException`; anything else
     escaped and skipped the teardown, leaving the accept loop running and the token source undisposed
     on a hub that reported itself stopped. `ShutDownAsync` now runs from `StopCoreAsync`'s `finally`
     (`:440-447`), so the shutdown proper always runs.
  5. **A start racing a stop could abandon a just-bound listener.** `StartAsync` now claims the running
     slot with a `_starting` flag (`:329`) *before* the listener starts, and publishes `_cts` and
     `_acceptLoopTask` together (`:369-370`). A stop can no longer take a token source whose accept loop
     does not exist yet — which would have left the endpoint bound with nothing serving it.
  6. **Disposal ran its teardown once per caller.** `DisposeAsync` memoises in `_disposeTask` (`:549`)
     and sets `_disposed` first (`:548`); the listener is disposed exactly once and a start on a
     disposed hub throws `ObjectDisposedException`.
- **What to do:** keep the discipline — take state under the lock, work from locals, never await while
  holding it. Eight tests pin this (`MeshHubTests.cs:118-357`); two of them park a caller mid-lifecycle
  deterministically rather than relying on thread timing, see [testing.md](testing.md#parking-a-caller-mid-lifecycle).
- **What not to do:** do not make `StopAsync` `async` again — its decision is taken synchronously under
  the lock, and that is what makes the "join the existing shutdown" handover race-free. Do not move
  `ShutDownAsync` out of the `finally`, do not reintroduce a second read of a lifecycle field outside
  the lock, and do not clear `_disposed` — disposal is terminal.

### KI-24 — The shutdown's disconnect notification is sequential and has no send timeout
- **Where:** `MeshHub.cs:427-437` — the `foreach` over `_clients.Values` inside `StopCoreAsync`.
- **Severity:** medium (performance / availability). **Pre-existing and unchanged by PR #64** — the loop
  moved from `StopAsync` into `StopCoreAsync` but its behaviour is identical. Do not read it as a
  regression introduced by that change.
- **Why it bites:** each `SendAsync` is awaited before the next begins, and the only bound on any of
  them is the `cancellationToken` the caller passed in. A single registered peer that has stopped
  reading — a TCP peer whose window has closed, not one that has dropped — can therefore hold
  `StopAsync(default)` open **indefinitely**, and every peer behind it in the iteration is never
  notified at all. Shutdown latency is also linear in client count even when every peer is healthy.
- **What to do:** pass a cancellable token to `StopAsync` (or `DisposeAsync`-then-abandon is *not* an
  option — disposal awaits the same shutdown). A `CancellationTokenSource` with a modest timeout is the
  practical guard. Note that cancelling only abandons *your* wait if you joined someone else's shutdown;
  the owning caller's token is the one that bounds the sends.
- **What not to do:** do not assume `await using var hub = ...` bounds shutdown — it passes no token.

### KI-25 — A stopped hub is not restartable in general
- **Where:** `MeshHub.cs:382` (`StopAsync`), `Transport/ITransportListener.cs` (no stop operation).
- **Severity:** medium (maintainability). Pre-existing; made **explicit** by PR #64 rather than
  introduced by it — `IMeshHub.StopAsync`'s XML docs now state it outright.
- **Why it bites:** `StopAsync` releases the hub's own state and clears `_stopTask`, so the *hub* is
  willing to start again. The transport is not: `ITransportListener` has no stop, so the endpoint stays
  bound, and **both listeners in this library throw on a second `StartAsync`**. A restart therefore
  fails at the listener, not at the hub, and the error will not obviously point here.
- **What to do:** treat a stopped hub as spent and dispose it. Construct a new hub over a new listener
  if you need to serve again.
- **What not to do:** do not infer restartability from `StopAsync_AfterCompleting_ReleasesTheHubsRunningClaim`
  (`MeshHubTests.cs:282`). Its own `<remarks>` is explicit that it covers the hub's half only, and that
  the second start succeeds solely because the fixture's listener is a mock that permits it.

### KI-26 — `maxClients` was a soft cap; the claim that replaced it is load-bearing — **FIXED (PR #65, issue #13)**
- **Where:** `MeshHub.cs:75` (`private int _reservedClientSlots`), `:833-837` (the pre-authentication
  early-out), `:854-860` (the claim), `:1043-1046` (the release, in `HandleClientAsync`'s `finally`),
  `:1081` (`TryReserveClientSlot`), `:1110` (`ReleaseClientSlot`), `:1118` (`RefuseAtCapacityAsync`).
- **Severity:** high (correctness). Reachable from ordinary load — no adversary required, only
  simultaneous registrations.
- **What was wrong:** admission tested `_clients.Count >= _maxClients` and added to `_clients` some way
  further down. Between the two there is a window, and it is not small — it spans the authenticator
  await. Any number of concurrent registrations could read the same count, all pass, and all admit, so
  the cap could be overshot by as many clients as happened to be registering at once. PR #56 had added a
  *second* check after authentication to narrow the window; that made the race less likely but no less
  possible, because two non-atomic checks are still not an atomic decision.
- **What the fix guarantees.** Both count-based checks are gone. Admission now turns on one
  compare-and-swap against `_reservedClientSlots`, so exactly one of any number of concurrent
  registrations takes the last slot and the rest are refused. Three parts of the shape carry weight:
  1. **The claim is a CAS loop, not increment-then-test-and-undo** (`:1081`). An increment that overshoots
     is visible to every other registration until it is backed out, so a burst that all overshot and all
     retreated would refuse clients for slots nobody held. The loop only ever claims from a value that
     was still under the cap at the instant of the claim.
  2. **The claim sits *after* authentication** (`:854`, the authenticator at `:839-846`). Taking it
     earlier would let a peer that never authenticates — or authenticates slowly — hold capacity away
     from one that would.
  3. **The pre-authentication early-out is retained** (`:833-837`) and now reads `_reservedClientSlots`
     rather than `_clients.Count`. It decides nothing; it exists solely to preserve the property that a
     **full** hub never runs the integrator's authenticator, so a connection flood cannot drive
     credential work or pin handler tasks on a slow callback.
- **The pairing is the fragile part.** Every successful claim is owned by exactly one client handler and
  must be given back exactly once, by that handler's `finally`, guarded by the `slotReserved` flag
  (`:762`, `:860`, `:1043`). This covers the duplicate-name refusal as well as ordinary disconnection.
  The release deliberately runs **before** the transport is disposed, so a transport that blocks on close
  cannot hold capacity for as long as it hangs. A claim that escapes its release leaks capacity for the
  lifetime of the hub, and nothing will report it.
- **`ConnectedClientCount` can read *below* the number of claimed slots, and that is intended.** It is
  still `_clients.Count` (`:511`). A registration between its claim and its `_clients` insert, and a
  shutdown that has cleared the registries while a handler is still unwinding, both show fewer connected
  clients than there are outstanding claims — shutdown deliberately does **not** reset the counter,
  because the running handlers own those claims and return them themselves. The invariant is
  `reserved >= registered`, so the discrepancy only ever errs conservative: the hub may refuse very
  slightly early, but it cannot admit past the cap.
- **What to do:** enforce capacity through `TryReserveClientSlot` only. If you add a new refusal path
  between the claim and the `_clients` insert, verify it exits through the same `finally`.
- **What not to do:** do not write an admission or capacity check against `ConnectedClientCount` or
  `_clients.Count` — that is precisely the bug. Do not move the claim ahead of the authenticator. Do not
  delete the early-out on the grounds that it is redundant with the claim (it is redundant for
  *correctness*, and load-bearing for the flood property). Do not reset `_reservedClientSlots` on
  shutdown. Do not make `TryReserveClientSlot`/`ReleaseClientSlot` private — they are `internal` so
  `HandleClient_LastSlotClaimedButNotYetRegistered_RefusesRatherThanExceedingMaxClients`
  (`MeshHubTests.cs:824`) can create the claimed-but-unregistered window that a count-based check cannot
  see. Three tests pin this (`MeshHubTests.cs:824`, `:880`, `:923`).

### KI-27 — `GroupJoinRefused` carries no correlation identifier
- **Where:** hub builds `[0x10][name bytes]` with nothing else (`RefuseGroupJoin`, `MeshHub.cs:1567-1569`);
  client keys the removal on the decoded name alone (`MeshClient.cs:768-780`).
- **Severity:** low (correctness). **Accepted and deliberate** — this is a reporting inaccuracy, not an
  access-control hole. Do not "fix" it without a reason beyond tidiness; a correlation id is a wire
  change.
- **Why it bites:** joins are not correlated, so a refusal cannot be matched to the request that
  provoked it. Given a join for group `G` that is refused, and a *later* join for the same `G` that the
  hub allows, the refusal for the first can arrive after the second was recorded and clear it. The
  client then reports itself out of a group the hub still has it in.
- **Why that is safe:** the divergence is **fail-safe in one direction only**. The client
  *under*-reports — it drops a membership it actually holds — while the hub keeps the member. The client
  therefore continues to *receive* that group's traffic and remains entitled to *send* to it; what breaks
  is `JoinedGroups` and, through it, what `MeshClientReconnector` would restore after a drop. Nothing is
  granted that should not be; something is merely forgotten. The reverse — a refusal failing to remove a
  membership the hub denied — cannot happen, because the hub removes its own side before replying
  (`MeshHub.cs:1439`).
- **What to do:** if exact membership matters, treat `GroupJoinRefused` as "re-check", not as
  "definitely out of this group", and re-join if you still expect to be a member. Do not build a
  join/refuse retry loop on top of it without your own correlation.
- **What not to do:** do not add a correlation id to the frame in isolation — it changes an existing
  opcode's layout and so requires a `Protocol.Version` bump
  ([protocol.md](protocol.md#additive-opcodes-within-a-version)).

### KI-28 — The group authoriser has no concurrency cap, and the timeout does not stop the callback
- **Where:** the deliberate absence is commented at `MeshHub.cs:1482-1493`; the bounded wait is
  `:1500-1505`.
- **Severity:** medium (perf / availability). **Deliberate** — the hub bounds its own waiting and leaves
  the callback's resource use to the integrator, and says so on the delegate and in the README.
- **Why it bites, in two parts:**
  1. **The timeout bounds the hub's patience, not the callback's execution.** A callback that outruns
     `groupAuthorisationTimeout` is *abandoned* — `WaitAsync` stops waiting; the delegate carries on
     running. The client is refused and is free to ask again immediately, so a client that keeps
     re-joining after each refusal can leave invocations piling up behind it.
  2. **There is no semaphore.** Unlike `ClientAuthenticator`, which has `maxConcurrentAuthentications`
     because it runs on unauthenticated input, this callback has no cap. One *client* cannot have two
     decisions in flight — its receive loop is parked until the callback returns — but that bounds
     concurrency per client, not across clients. The ceiling is the connected client count, which is
     `maxClients` only if you configured one. A mass reconnect re-joins every group at once.
- **The related trap:** while the callback runs the client's receive loop reads nothing, so the client
  looks idle to the heartbeat monitor and can be **evicted rather than refused** if the decision outlasts
  `heartbeatInterval × maxMissedHeartbeats`. Behind a `MeshClientReconnector` that becomes a reconnect
  loop. The constructor warns on this combination (`MeshHub.cs:285-306`) but does not prevent it.
- **What to do:** an authoriser that holds a resource per invocation — a database connection, an
  outbound HTTP call — must **bound its own concurrency**, with its own semaphore or a shared pooled
  client. Prefer a synchronous decision against an in-memory policy table where you can; it takes the
  allocation-free fast path. Set `maxClients` if you want a hard ceiling on the fan-out. Keep
  `groupAuthorisationTimeout` below the heartbeat eviction budget.
- **What not to do:** do not assume the timeout releases whatever the callback was holding — it does
  not. Do not add a semaphore to the hub expecting it to help: it would queue joins behind each other
  and make the eviction trap worse, which is why there isn't one.

### KI-29 — `MeshHub` had unbounded resource-consumption defaults — **FIXED (PR #68, issue #16, draft)**
- **Where:** the new constants `MeshHub.cs:24-38` (`DefaultMaxClients`, `DefaultHeartbeatInterval`,
  `DefaultMaxConnectionsPerRemoteEndpoint`); the constructor's default resolution `:181-262`; the whole
  per-remote-endpoint cap machinery in `AcceptLoopAsync` and five new private helpers `:602-753`
  (`ExtractRemoteAddress`, `NormaliseForEndpointCap`, `DisposeRefusedTransportAsync`,
  `TryReserveEndpointSlot`, `ReleaseEndpointSlot`); the new `IRemoteEndPointTransport` interface
  (`Transport/IRemoteEndPointTransport.cs`) and its `TcpTransport` implementation (`TcpTransport.cs:70`).
- **Severity:** high (availability / security). Reachable from ordinary load, not just an adversary: any
  hub constructed with the defaults — which is to say, most integration code before this PR — had no
  ceiling on registered clients and never evicted an idle one.
- **What was wrong:** three independent gaps, all in the same direction:
  1. **`maxClients` defaulted to `int.MaxValue`.** A hub built with no arguments admitted an unlimited
     number of registered clients. An unauthenticated peer (KI-2) could open connections until the
     process ran out of sockets, threads or memory, and nothing in the constructor warned about it.
  2. **`heartbeatInterval` defaulted to `null` (disabled).** A registered connection that sent one frame
     and then went silent forever held its handler task, socket and outbound queue for the life of the
     hub. There was no idle eviction unless an integrator explicitly opted in.
  3. **Nothing capped connections per remote address.** Even with `maxClients` set, a flood of
     connections from one source that never completed the registration handshake sailed straight past
     it — `maxClients` only ever counted *registered* clients, never the pre-registration window.
- **What the fix guarantees, and why each default was chosen:**
  1. **`maxClients` now defaults to 1000.** The figure was already the README's worked example for what a
     "sensible" cap looks like, so an integrator who never touched the parameter now gets the behaviour
     the documentation always described as sensible, not silence. `int.MaxValue` still opts back into
     unlimited.
  2. **`heartbeatInterval` now defaults to 30 seconds**, and `null` now means "not configured" rather
     than "disabled" — a hub that never touches the parameter gets idle eviction at the default interval,
     exactly like an unconfigured `maxClients` still gets a cap. `Timeout.InfiniteTimeSpan` is the new,
     explicit sentinel that disables idle eviction — passing it is a deliberate act, distinct from simply
     omitting the parameter. The constructor's two existing warnings (`maxMissedHeartbeats == 1` with
     heartbeats; `groupAuthorisationTimeout` too close to the eviction budget, `:270-283`, `:285-306`) were
     updated to check the **resolved** `_heartbeatInterval` field rather than the raw parameter, since
     idle eviction now runs by default even when the parameter was never set — checking the raw
     parameter would have missed every hub relying on the new default.
  3. **`maxConnectionsPerRemoteEndpoint` (new, default 100)** caps connections accepted from one remote
     address at once, enforced in `AcceptLoopAsync` **before** any handler task is created or any
     registration frame is read — covering exactly the pre-registration window `maxClients` cannot see.
     It is keyed on the transport's reported remote address via the new, public
     `IRemoteEndPointTransport` capability — following the existing `IBatchSendTransport` pattern, but
     public rather than internal, since an external transport needs to be able to implement it too. A
     transport that does not implement it (`InMemoryTransport`, or a custom one) is simply never capped.
     **IPv6 addresses are masked to their `/64` network prefix before being used as the cap's key**
     (`NormaliseForEndpointCap`, `MeshHub.cs:663-678`) — without this, a single host with a routine `/64`
     allocation could defeat the cap by connecting from a different address within it each time, since a
     full-address key would see each one as a brand-new source.
- **This is a breaking behavioural change, not just a value change.** Any hub already running with the
  old defaults — unlimited clients, no idle eviction — will now refuse registrations beyond 1000
  concurrent clients and evict clients that go silent for 60 s (30 s × `maxMissedHeartbeats: 2`) by
  default. Tests or integrations that relied on either old behaviour, including ones that open many
  connections from `localhost` without configuring `maxConnectionsPerRemoteEndpoint`, need `int.MaxValue`
  / `Timeout.InfiniteTimeSpan` passed explicitly.
- **What to do:** accept the new defaults for any hub facing untrusted or unauthenticated input — that is
  the case they exist for. If a hub genuinely needs unlimited clients or no idle eviction (e.g. a fully
  trusted, closed deployment), pass `int.MaxValue` / `Timeout.InfiniteTimeSpan` explicitly and say why in
  a comment, since the new defaults are now the documented, expected posture.
- **What not to do:** do not "fix" a test that starts failing under the new caps by raising the cap
  globally — configure the specific test's `MeshHubFixture`/`MeshHub` instead
  (`maxConnectionsPerRemoteEndpoint: int.MaxValue`, etc.), the same way `MeshHubFixture` already does for
  the four per-remote-endpoint tests. Do not remove the IPv6 `/64` masking on the reasoning that "IPv6
  isn't used in tests" — it is the load-bearing part of the cap for any deployment that is reachable over
  IPv6 at all.

---

## Also worth knowing (not defects)

- **Broadcasts are indistinguishable from direct messages at the recipient** — both arrive as
  `DeliverMessage` → `MessageReceived` (`MeshHub.cs:1373`). If you need to tell them apart, encode it in
  your payload.
- **Two `.slnx` files** (root `Meshworx.slnx` vs `src/Meshworx.slnx`). CI and "done" use the root one.
- **`RegistrationRefusedException`'s extra ctors** (message / message+inner / default) exist only to
  satisfy analyser `CA1032`; the meaningful one is `RegistrationRefusedException(RegistrationErrorCode)`.
- **No `TODO`/`FIXME`/`HACK` markers** were found in the `main` library source — the comments are
  explanatory, not debt markers.
