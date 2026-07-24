# Known issues, foot-guns & load-bearing behaviour

[← back to index](../for-clanker.md)

The single complete register of what will bite a change in this codebase. Most-impactful first. Every
entry is grounded in the code. Many of these are **intentional design choices**, not bugs — but each is
a trap for code that assumes the obvious, so treat them as constraints to work *with*. "Severity" rates
the risk to a change, not a claim that the code is defective.

| ID | Title | Where | Severity | Status |
|---|---|---|---|---|
| KI-1 | Full outbound queue silently drops the frame | `MeshHub.cs:624`, `:650`, `:782` | high (correctness) | open — by design |
| KI-2 | No authentication or authorisation | system-wide | high (security) | open — by design |
| KI-3 | Client-name length checked in chars, not UTF-8 bytes | `MeshHub.cs:276`, `MeshClient.cs:102` | medium (correctness) | open |
| KI-4 | Unknown recipient drops the message silently | `MeshHub.cs:610-616` | medium (correctness) | open — by design |
| KI-5 | Delivery is unordered/unacked across the fan-out; no persistence | system-wide | medium (correctness) | open — by design |
| KI-6 | `StopAsync` writes `Disconnect` outside the send loop | `MeshHub.cs:118-128` | medium (correctness) | open |
| KI-7 | `InMemoryTransport` uses unbounded channels (no back-pressure) | `InMemoryTransport.cs:34-39` | medium (perf) | open — by design |
| KI-8 | Group-name length asymmetry (join unbounded, send ≤ 65 535) | `MeshHub.cs:354`, `MeshClient.cs:342-351` | low (correctness) | open |
| KI-9 | Malformed/short/unknown frames silently ignored | `MeshHub.cs:340-412`, `MeshClient.cs:513-643` | medium (maintainability) | open — by design |
| KI-10 | `JoinedGroups` can drift from the hub; no auto-rejoin on reconnect | `MeshClient.cs:310-330`, `MeshClientReconnector.cs:20-23` | medium (correctness) | open — by design |
| KI-11 | Heartbeat eviction is off-by-one vs "max missed" | `MeshHub.cs:584` | low (behaviour) | open |
| KI-12 | Only one client lookup in flight at a time | `MeshClient.cs:387-434` | low (perf) | open — by design |
| KI-13 | Event `Data` is a view over the received frame | `MeshClient.cs:551`, `:577` | low (correctness) | open |

---

### KI-1 — Full outbound queue silently drops the frame
- **Where:** `RouteMessage` `MeshHub.cs:624`, `BroadcastMessage` `:650`, `SendToGroup` `:782`. Queue is
  a bounded `Channel<byte[]>` of capacity **1024** per connection (`MeshHub.cs:805`, `:831`).
- **Why it bites:** the hub delivers with `TryWrite`. If a recipient's consumer (its transport) is slow
  enough to fill 1024 queued frames, every further frame for it is **dropped and logged at Warning** —
  no exception, no back-pressure to the sender. A `SendAsync` that "succeeds" guarantees only that the
  hub accepted the frame, never that it was queued for, let alone delivered to, the recipient.
- **What to do:** treat delivery as lossy. If you need reliability, build acks/retries at the
  application layer. If you raise the cap or change to blocking writes, understand you are trading drop
  for head-of-line blocking of the router — do it deliberately and test under a stalled consumer.

### KI-2 — No authentication or authorisation
- **Where:** system-wide; registration (`MeshHub.cs:238-306`) only checks name uniqueness/length/version/
  capacity.
- **Why it bites:** any peer that can open the transport can register under any unused name, enumerate
  peers via `GetClientIdByNameAsync`, broadcast to everyone, and join any group. There is no identity,
  no secrecy, no access control.
- **What to do:** treat the transport boundary as the trust boundary (bind to loopback/private networks,
  or wrap the transport in TLS/an authenticated channel). Do not expose a hub to untrusted networks as-is.
  If you add auth, it belongs in/around the handshake and/or a custom `ITransport`.

### KI-3 — Client-name length checked in chars, not UTF-8 bytes
- **Where:** hub `clientName.Length > Protocol.MaxClientNameLength` (`MeshHub.cs:276`); client
  `clientName.Length > Protocol.MaxClientNameLength` (`MeshClient.cs:102`). `MaxClientNameLength = 256`.
- **Why it bites:** `.Length` counts UTF-16 code units, not encoded bytes. A 256-"character" name of
  multi-byte code points encodes to well over 256 bytes on the wire, and a name of astral characters
  (surrogate pairs) counts each pair as 2. Both sides use the same check so they agree, but any external
  reimplementation that validates bytes will disagree. Not a buffer risk (frames are 1 MiB-bounded), but
  a spec ambiguity.
- **What to do:** if you tighten this, decide bytes-vs-chars explicitly and change both sides + the wire
  docs together.

### KI-4 — Unknown recipient drops the message silently
- **Where:** `RouteMessage` `MeshHub.cs:610-616` (logs `Debug`, returns). Same effect for group sends to
  a non-existent/empty group (`SendToGroup` early-returns, `MeshHub.cs:737`, `:745`).
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
- **Where:** `MeshHub.cs:118-128` — iterates `_clients` and calls `client.Transport.SendAsync` directly,
  concurrently with each connection's still-running send loop.
- **Why it bites:** two writers hit the same transport at once. This is **safe for `TcpTransport`**
  (internal `SemaphoreSlim` write lock, `TcpTransport.cs:20`) and any transport that honours the
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
- **Where:** `Join`/`Leave` frames carry the name as the whole frame remainder (`MeshHub.cs:354`, `:359`)
  — effectively bounded only by the 1 MiB frame cap. `GroupMessage`/`DeliverGroupMessage` encode the
  name length as a `u16`, and the client rejects names over `ushort.MaxValue` (`MeshClient.cs:342-351`).
- **Why it bites:** a group name between 65 536 bytes and 1 MiB can be joined but never targeted by a
  group send from the stock client. An edge case, but a real inconsistency.
- **What to do:** keep group names short. If you unify the limit, apply it at join time too.

### KI-9 — Malformed/short/unknown frames silently ignored
- **Where:** dispatch ladders `MeshHub.cs:340-412` and `MeshClient.cs:513-643` — length-guarded
  `else if` chains with no terminal `else`.
- **Why it bites:** a frame that is too short for its opcode, or has an unknown opcode, is dropped with
  no error and no warning-level log. A framing/offset bug manifests as "nothing happens", which is hard
  to diagnose.
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
- **Where:** `MeshHub.cs:584` — evicts when `missedHeartbeats > _maxMissedHeartbeats`.
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

---

## Also worth knowing (not defects)

- **Broadcasts are indistinguishable from direct messages at the recipient** — both arrive as
  `DeliverMessage` → `MessageReceived` (`MeshHub.cs:637`). If you need to tell them apart, encode it in
  your payload.
- **Two `.slnx` files** (root `Meshworx.slnx` vs `src/Meshworx.slnx`). CI and "done" use the root one.
- **`RegistrationRefusedException`'s extra ctors** (message / message+inner / default) exist only to
  satisfy analyser `CA1032`; the meaningful one is `RegistrationRefusedException(RegistrationErrorCode)`.
- **No `TODO`/`FIXME`/`HACK` markers** were found in the `main` library source — the comments are
  explanatory, not debt markers.
