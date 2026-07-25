# Known issues, foot-guns & load-bearing behaviour

[← back to index](../for-clanker.md)

The single complete register of what will bite a change in this codebase. Most-impactful first. Every
entry is grounded in the code. Many of these are **intentional design choices**, not bugs — but each is
a trap for code that assumes the obvious, so treat them as constraints to work *with*. "Severity" rates
the risk to a change, not a claim that the code is defective.

| ID | Title | Where | Severity | Status |
|---|---|---|---|---|
| KI-1 | Full outbound queue silently drops the frame | `MeshHub.cs:790`, `:816`, `:948` | high (correctness) | open — by design |
| KI-2 | Open admission by default; no authorisation model | system-wide | high (security) | **partly addressed** — authentication seam added (PR #56), transport TLS added (PR #59); open admission and cleartext remain the *defaults* |
| KI-3 | Client-name length checked in chars, not UTF-8 bytes | `MeshHub.cs:331`, `MeshClient.cs:102` | medium (correctness) | open |
| KI-4 | Unknown recipient drops the message silently | `MeshHub.cs:776-782` | medium (correctness) | open — by design |
| KI-5 | Delivery is unordered/unacked across the fan-out; no persistence | system-wide | medium (correctness) | open — by design |
| KI-6 | `StopAsync` writes `Disconnect` outside the send loop | `MeshHub.cs:156-166` | medium (correctness) | open |
| KI-7 | `InMemoryTransport` uses unbounded channels (no back-pressure) | `InMemoryTransport.cs:34-39` | medium (perf) | open — by design |
| KI-8 | Group-name length asymmetry (join unbounded, send ≤ 65 535) | `MeshHub.cs:440`, `MeshClient.cs:342-351` | low (correctness) | open |
| KI-9 | Malformed/short/unknown frames silently ignored | `MeshHub.cs:426-498`, `MeshClient.cs:513-643` | medium (maintainability) | open — by design |
| KI-10 | `JoinedGroups` can drift from the hub; no auto-rejoin on reconnect | `MeshClient.cs:310-330`, `MeshClientReconnector.cs:20-23` | medium (correctness) | open — by design |
| KI-11 | Heartbeat eviction is off-by-one vs "max missed" | `MeshHub.cs:750` | low (behaviour) | open |
| KI-12 | Only one client lookup in flight at a time | `MeshClient.cs:387-434` | low (perf) | open — by design |
| KI-13 | Event `Data` is a view over the received frame | `MeshClient.cs:551`, `:577` | low (correctness) | open |
| KI-14 | Protocol v3 is a hard break: v2 peers are refused, and `ConnectAsync` is source-breaking | `Messages/Protocol.cs:5`, `IMeshClient.cs:49` | high (compatibility) | open — by design |
| KI-15 | `AuthenticationFailed` conflates refusal, throw, timeout and slot starvation | `MeshHub.cs:560-638` | medium (maintainability) | open — by design |
| KI-16 | A reconnector's credential is fixed at construction and cannot be rotated | `MeshClientReconnector.cs:81`, `:137`, `:216` | medium (correctness) | open |
| KI-17 | Sender identity is hop-by-hop: a compromised hub can forge any sender | system-wide | medium (security) | open — by design |
| KI-18 | A failed TLS handshake is silent — the hub sees nothing at all | `TcpTransportListener.cs:446-460` | medium (maintainability) | open — by design |
| KI-19 | A queued reconnect signal means the connection *was* lost, not that it still is | `MeshClientReconnector.cs:233-236`, `:160-163` | high (correctness) | **load-bearing** — guard added by PR #60; do not remove |
| KI-20 | The caller owns the transport until `ConnectAsync` accepts it | `MeshClient.cs:163-177`, `MeshClientReconnector.cs:249-256`, `:131-145` | medium (resource correctness) | **partly addressed** — retry path fixed by PR #60; `StartAsync` still leaks |

---

### KI-1 — Full outbound queue silently drops the frame
- **Where:** `RouteMessage` `MeshHub.cs:790`, `BroadcastMessage` `:816`, `SendToGroup` `:948`. Queue is
  a bounded `Channel<byte[]>` of capacity **1024** per connection (`MeshHub.cs:971`, `:997`).
- **Why it bites:** the hub delivers with `TryWrite`. If a recipient's consumer (its transport) is slow
  enough to fill 1024 queued frames, every further frame for it is **dropped and logged at Warning** —
  no exception, no back-pressure to the sender. A `SendAsync` that "succeeds" guarantees only that the
  hub accepted the frame, never that it was queued for, let alone delivered to, the recipient.
- **What to do:** treat delivery as lossy. If you need reliability, build acks/retries at the
  application layer. If you raise the cap or change to blocking writes, understand you are trading drop
  for head-of-line blocking of the router — do it deliberately and test under a stalled consumer.

### KI-2 — Open admission by default; no authorisation model
- **Where:** system-wide; registration `MeshHub.cs:277-395`, authentication `MeshHub.cs:560-638`.
- **Status:** the *authentication* half now has a supported seam (PR #56, protocol version 3) and the
  *transport* half can now be secured with TLS (PR #59). The *authorisation* half does not exist, and
  both of the above are **opt-in** — the defaults are still open admission over a cleartext socket.
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
- **Why it still bites:**
  1. **No authenticator is the default.** A `MeshHub` constructed without one admits any peer that
     completes the handshake — exactly the pre-v3 behaviour. Nothing warns you.
  2. **Authentication is not authorisation.** Once admitted, *every* client can enumerate peers via
     `GetClientIdByNameAsync`, broadcast to everyone, join any group and send to any group. The
     authenticator's only lever is admit/refuse; there are no per-client capabilities and the hub does
     not pass identity into routing.
  3. **Names are not bound to identity.** The authenticator sees the requested name, but the hub's only
     name rule is uniqueness. Unless *your* callback ties credential to name, an authenticated client
     may register under any unused name, including one it has no right to.
  4. **Confidentiality and integrity are opt-in, and nothing warns you.** A listener constructed
     without `tlsOptions`, or a client using the three-argument `TcpTransport.ConnectAsync(host, port,
     ct)`, is **cleartext**: client names, assigned ids, group names and every message payload cross the
     wire in the clear and can be modified in flight. There is no log line, no property check and no
     handshake failure to tell you — only `TcpTransport.IsEncrypted` (`TcpTransport.cs:61`), which you
     have to assert yourself.
  5. **Even with TLS, sender identity is not end to end.** TLS secures each client–hub hop separately;
     a delivered message's sender id is still asserted by the hub rather than signed by the sending
     client. See KI-17.
- **What to do:** supply an authenticator for anything beyond a trusted network, and bind the credential
  to the requested `ClientName` inside it if names are meaningful to your application. Configure TLS on
  both ends where traffic crosses an untrusted segment — or run inside an already-encrypted channel
  (VPN, service-mesh mTLS, a TLS-terminating proxy) if that is how your deployment already works. Assert
  `IsEncrypted` in a start-up check so a mis-wired deployment fails loudly rather than quietly running
  in the clear. Note the `TcpTransportListener(int port)` convenience constructor binds loopback only
  ([transport.md](transport.md)), so remote exposure is a deliberate act.
- **What not to do:** do not treat a returned `true` as an authorisation decision, and do not put
  expensive or non-constant-time credential checks in the callback without reading
  [hub.md](hub.md#authentication) first — it runs on unauthenticated input.

### KI-3 — Client-name length checked in chars, not UTF-8 bytes
- **Where:** hub `clientName.Length > Protocol.MaxClientNameLength` (`MeshHub.cs:331`); client
  `clientName.Length > Protocol.MaxClientNameLength` (`MeshClient.cs:102`). `MaxClientNameLength = 256`.
- **Why it bites:** `.Length` counts UTF-16 code units, not encoded bytes. A 256-"character" name of
  multi-byte code points encodes to well over 256 bytes on the wire, and a name of astral characters
  (surrogate pairs) counts each pair as 2. Both sides use the same check so they agree, but any external
  reimplementation that validates bytes will disagree. Not a buffer risk (frames are 1 MiB-bounded), but
  a spec ambiguity.
- **What to do:** if you tighten this, decide bytes-vs-chars explicitly and change both sides + the wire
  docs together.

### KI-4 — Unknown recipient drops the message silently
- **Where:** `RouteMessage` `MeshHub.cs:776-782` (logs `Debug`, returns). Same effect for group sends to
  a non-existent/empty group (`SendToGroup` early-returns, `MeshHub.cs:903`, `:911`).
- **Why it bites:** sending to a stale/never-registered id, or to a group nobody has joined, is a no-op.
  The sender gets no signal. Combined with KI-1, "message sent" never implies "message delivered".
- **What to do:** resolve ids via `GetClientIdByNameAsync` immediately before sending if freshness
  matters, and design for silent loss.

### KI-5 — Unordered/unacked delivery, no persistence
- **Where:** system-wide. Fan-out is per-recipient queues; there is no sequence number, ack, or store.
- **Why it bites:** ordering holds only within a single connection's stream. Across a broadcast/group
  fan-out there is no global order, and nothing is retained if a client is offline.
- **What to do:** layer any ordering/durability guarantees on top; do not assume them.

### KI-6 — `StopAsync` writes `Disconnect` outside the send loop
- **Where:** `MeshHub.cs:156-166` — iterates `_clients` and calls `client.Transport.SendAsync` directly,
  concurrently with each connection's still-running send loop.
- **Why it bites:** two writers hit the same transport at once. This is **safe for `TcpTransport`**
  (internal `SemaphoreSlim` write lock, `TcpTransport.cs:32`) and any transport that honours the
  "`SendAsync` must be concurrency-safe" contract — but a custom transport that serialises incorrectly
  will interleave/corrupt framing during shutdown.
- **What to do:** if you write a custom transport, make `SendAsync` genuinely concurrency-safe. Don't
  "optimise" `TcpTransport`'s write lock away.

### KI-7 — `InMemoryTransport` unbounded channels
- **Where:** `InMemoryTransport.CreatePair` uses `Channel.CreateUnbounded` (`InMemoryTransport.cs:34-35`);
  `InMemoryTransportListener` pending-connections channel is also unbounded.
- **Why it bites:** no back-pressure — a fast producer against a stalled consumer grows memory without
  bound. Fine for tests and cooperative in-process use; not for adversarial/production load.
- **What to do:** use TCP (or a bounded custom transport) where back-pressure matters.

### KI-8 — Group-name length asymmetry
- **Where:** `Join`/`Leave` frames carry the name as the whole frame remainder (`MeshHub.cs:440`, `:445`)
  — effectively bounded only by the 1 MiB frame cap. `GroupMessage`/`DeliverGroupMessage` encode the
  name length as a `u16`, and the client rejects names over `ushort.MaxValue` (`MeshClient.cs:342-351`).
- **Why it bites:** a group name between 65 536 bytes and 1 MiB can be joined but never targeted by a
  group send from the stock client. An edge case, but a real inconsistency.
- **What to do:** keep group names short. If you unify the limit, apply it at join time too.

### KI-9 — Malformed/short/unknown frames silently ignored
- **Where:** dispatch ladders `MeshHub.cs:426-498` and `MeshClient.cs:513-643` — length-guarded
  `else if` chains with no terminal `else`.
- **Why it bites:** a frame that is too short for its opcode, or has an unknown opcode, is dropped with
  no error and no warning-level log. A framing/offset bug manifests as "nothing happens", which is hard
  to diagnose.
- **Also applies to registration** (`MeshHub.cs:299-327`): a frame under 2 bytes, under 4 bytes, with a
  zero name length, or with a name length running past the payload, drops the connection with **no error
  frame**. The client sees a closed connection rather than a `RegistrationRefusedException`, so a
  framing bug there looks like "the hub is down".
- **What to do:** when adding an opcode, add the guard *and* the branch on the correct side at the exact
  offsets in [protocol.md](protocol.md). When debugging missing messages, check framing first.

### KI-10 — `JoinedGroups` drift; no auto-rejoin on reconnect
- **Where:** client updates `_joinedGroups` optimistically **after** sending join/leave
  (`MeshClient.cs:314-318`, `:325-329`); membership is fire-and-forget. `MeshClientReconnector` does not
  restore it (`MeshClientReconnector.cs:20-23`).
- **Why it bites:** if a join/leave frame is lost, the client's `JoinedGroups` and the hub's actual
  membership diverge. After any reconnect the hub has **zero** membership for the client until the app
  re-joins.
- **What to do:** capture `Client.JoinedGroups` before a drop and re-`JoinGroupAsync` each on the
  reconnector's `Reconnected` event (see [client.md](client.md)).

### KI-11 — Heartbeat eviction off-by-one
- **Where:** `MeshHub.cs:750` — evicts when `missedHeartbeats > _maxMissedHeartbeats`.
- **Why it bites:** "max missed heartbeats = N" actually tolerates N idle intervals and evicts on the
  (N+1)th. With the default 2, a silent client is evicted after 3 idle intervals. Sizing a client
  `idleTimeout` or an SLA around the literal parameter value will be one interval short.
- **What to do:** account for the +1 when tuning; see [hub.md](hub.md).

### KI-12 — One client lookup in flight at a time
- **Where:** `GetClientIdByNameAsync` serialised by `_lookupLock` (`SemaphoreSlim(1,1)`) with a
  single-slot `_pendingLookup` (`MeshClient.cs:387-434`).
- **Why it bites:** concurrent lookups on the same client queue rather than pipeline — throughput of
  name resolution is one round-trip at a time. Correct (correlation ids prevent cross-talk), just not
  parallel.
- **What to do:** batch/caches name→id at the app layer if you resolve many names hot. Don't remove the
  correlation-id guard if you parallelise — it is what prevents a cancelled lookup resolving a later one.

### KI-13 — Event `Data` is a view over the received frame
- **Where:** `MessageReceived`/`GroupMessageReceived` args carry `ReadOnlyMemory<byte>` slices of the
  frame (`MeshClient.cs:551`, `:577`).
- **Why it bites:** the memory is only contractually valid during the handler. It happens to be backed
  by a fresh per-frame `byte[]` today (`TcpTransport.ReceiveAsync` allocates per frame), so retaining it
  works — but a future pooled-buffer transport would invalidate anything you kept.
- **What to do:** copy (`.ToArray()` / `Span.CopyTo`) if you retain the payload past the handler.

### KI-14 — Protocol v3 is a hard break, on the wire and in source
- **Where:** `Protocol.Version = 3` (`Messages/Protocol.cs:5`); the reshaped `RegistrationRequest`
  (`MeshHub.cs:314-329`, `MeshClient.cs:181-189`); the signature change on `IMeshClient.ConnectAsync`
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
- **Where:** `AuthenticateAsync` (`MeshHub.cs:560-638`) returns `false` for a refusal, a throw, a
  cancellation inside the callback, a callback that exceeds `registrationTimeout`, and a failure to
  acquire an authentication slot. The caller sends the same
  `Error(AuthenticationFailed)` in all cases (`MeshHub.cs:357-358`).
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
  (`MeshClientReconnector.cs:81`, `:99`) and replays it on every connect and reconnect (`:137`, `:247`).
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
  `RouteMessage` `MeshHub.cs:787`, `BroadcastMessage` `:805`, `SendToGroup` `:935` — from its own record
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
- **Where:** `TcpTransportListener.HandshakeAsync`'s catch-all (`TcpTransportListener.cs:446-460`) —
  disposes the connection and swallows the cause. The transport layer has no logger.
- **Why it bites:** an untrusted or absent client certificate, a protocol/cipher mismatch, a peer that
  exceeded `tlsHandshakeTimeout`, a cleartext client dialling a TLS listener, and a peer that reset all
  produce **exactly the same observable outcome: nothing**. The hub never sees a connection, logs no
  error, and the client gets a generic `AuthenticationException` or a closed socket. "The client cannot
  connect and neither side says why" is the expected symptom of a TLS misconfiguration here, and it will
  cost you time if you do not know that going in. The same silence covers the pending-bound shedding
  path (`:363-367`), where a connection is dropped before a handshake is even attempted.
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
  (`MeshClientReconnector.cs:233-236`); the signal sources are `OnDisconnected` (`:185`) and the
  post-subscription state re-check in `StartAsync` (`:160-163`).
- **Why it bites:** the reconnector's trigger is **level-based, not edge-based**. A signal sitting in
  the capacity-1 channel is a record that a drop *happened*, not a guarantee the client is still down by
  the time the loop services it. Three ways a signal goes stale:
  1. **One drop, two signals.** `Client.IsConnected` is false during `Disconnecting` as well as
     `Disconnected`, so a teardown that straddles the subscription line is seen *both* by `StartAsync`'s
     state re-check and, moments later, by `OnDisconnected` when the event finally fires. The channel is
     `DropWrite` capacity 1, so it coalesces only signals that overlap *in the queue* — these two do
     not, and the second survives as a duplicate for a drop already serviced.
  2. **An application handler reconnected from inside `Disconnected`.** `MeshClient` explicitly supports
     this (`MeshClient.cs:224-234`), so the connection can be live again before the loop wakes.
  3. **An earlier pass already recovered it.**
- **What it costs if the guard goes:** `MeshClient.ConnectAsync` **refuses** a connect unless the client
  is fully `Disconnected` (`MeshClient.cs:163-173`), so servicing a stale signal does not merely waste a
  round trip — it throws `InvalidOperationException` every time, which
  `ConnectWithRetryAsync`'s catch-all treats as a retryable failure. The loop then retries an
  already-connected client **for ever** at `retryDelay`, building and discarding a transport each pass.
  This is the regression PR #60's second commit exists to prevent;
  `StartAsync_DropSignalledTwice_SettlesWithoutRetryingConnectedClient`
  (`MeshClientReconnectorTests.cs:318`) is the test that pins it.
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
- **Where:** `MeshClient.ConnectAsync` adopts the transport at `MeshClient.cs:176`, *after* its argument
  and state validation (`:153-173`). A throw from that validation therefore leaves the transport
  unowned and unclosed; a throw after adoption is cleaned up by `CleanUpAsync` (`:238`, disposal at
  `:581-584`). The reconnector's retry path handles the gap (`MeshClientReconnector.cs:249-256`).
- **Why it bites:** the reachable case is not a programming error. A reconnect attempt racing a teardown
  hits the `ConnectionState.Disconnecting` guard (`MeshClient.cs:170`) and is rejected with
  `InvalidOperationException` before adoption — so before PR #60 every such attempt **abandoned a live
  transport**, one connected socket leaked per rejected retry, on a path that retries indefinitely.
- **Two things this leaves you with:**
  1. **`StartAsync` has no equivalent guard.** Its connect (`MeshClientReconnector.cs:131-145`) resets
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
- **What to do:** in any new code that hands a transport to `ConnectAsync`, dispose it yourself if the
  call throws. If you write a custom `ITransport`, make `DisposeAsync` idempotent, as both shipped
  implementations are. If you fix the `StartAsync` path, mirror the retry path's shape exactly rather
  than inventing a second convention.

---

## Also worth knowing (not defects)

- **Broadcasts are indistinguishable from direct messages at the recipient** — both arrive as
  `DeliverMessage` → `MessageReceived` (`MeshHub.cs:803`). If you need to tell them apart, encode it in
  your payload.
- **Two `.slnx` files** (root `Meshworx.slnx` vs `src/Meshworx.slnx`). CI and "done" use the root one.
- **`RegistrationRefusedException`'s extra ctors** (message / message+inner / default) exist only to
  satisfy analyser `CA1032`; the meaningful one is `RegistrationRefusedException(RegistrationErrorCode)`.
- **No `TODO`/`FIXME`/`HACK` markers** were found in the `main` library source — the comments are
  explanatory, not debt markers.
