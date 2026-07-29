# Known issues, foot-guns & load-bearing behaviour

[← back to index](../for-clanker.md)

The single complete register of what will bite a change in this codebase. Most-impactful first. Every
entry is grounded in the code. Many of these are **intentional design choices**, not bugs — but each is
a trap for code that assumes the obvious, so treat them as constraints to work *with*. "Severity" rates
the risk to a change, not a claim that the code is defective.

| ID | Title | Where | Severity | Status |
|---|---|---|---|---|
| KI-1 | Full outbound queue drops the frame by default | `MeshHub.cs:1892`, `:1958`, `:2049`, `:2392`, `:2476` | high (correctness) | **partly addressed** — PR #87 (issue #30) added an always-on in-process `QueueSaturated` event, an opt-in wire notification for direct sends only (`notifyOnQueueSaturation`), and an opt-in `DeliveryOptions.AwaitCapacity` that awaits room instead of dropping (direct sends only); the drop itself remains the default for anything that does not opt in, and broadcast/group drops are never disclosed to the sender at all, by deliberate design |
| KI-2 | Open admission by default; authorisation covers groups only | system-wide | high (security) | **partly addressed** — authentication seam (PR #56), transport TLS (PR #59), group authorisation seam + unconditional group-send membership (PR #66); open admission and cleartext remain the *defaults* |
| KI-3 | Client-name length checked in chars, not UTF-8 bytes | `MeshHub.cs:988`, `MeshClient.cs:185` | medium (correctness) | open |
| KI-4 | Unknown recipient drops the message silently | `MeshHub.cs:1877-1884` | medium (correctness) | open — by design |
| KI-5 | Delivery is unordered across the fan-out, and unacked for broadcast/group; no persistence | system-wide | medium (correctness) | **partly addressed** — PR #84 added an opt-in, end-to-end acknowledgement for a single direct send (`DeliveryOptions.RequireAck`); broadcast/group sends and cross-connection ordering remain unacknowledged and unordered by design |
| KI-6 | `StopAsync` writes `Disconnect` outside the send loop | `MeshHub.cs:562-572` | medium (correctness) | open |
| KI-7 | `InMemoryTransport` uses unbounded channels (no back-pressure) | `InMemoryTransport.cs:34-39` | medium (perf) | open — by design |
| KI-8 | Group-name length asymmetry (join unbounded, send ≤ 65 535) | `MeshHub.cs:1110`, `MeshClient.cs:658-661` | low (correctness) | open — now also reaches the group authoriser and the refusal frame |
| KI-9 | Malformed/short/unknown frames silently ignored | `MeshHub.cs:1081-1199`, `MeshClient.cs:1035-1240` | medium (maintainability) | open — by design, and now load-bearing for additive opcodes, including the four header-bearing ones added by PR #74; PR #83 and PR #84 each nested a check inside the same one branch without adding a branch, see history below |
| KI-10 | `JoinedGroups` can drift from the hub | `MeshClient.cs:580-618`, `MeshClientReconnector.cs:316-340` | low (correctness) | **largely addressed** — auto-rejoin landed in PR #52; PR #66 made a refusal correct the client's view. Residual drift only, see KI-27 |
| KI-11 | Heartbeat eviction was off-by-one vs "max missed" | `MeshHub.cs:1851` | low (behaviour) | **fixed** — corrected by PR #61 (issue #9); inclusive comparison, do not loosen |
| KI-12 | Only one client lookup in flight at a time | `MeshClient.cs:798-844` | low (perf) | open — by design |
| KI-13 | Event `Data` is a view over the received frame | `MeshClient.cs:1046`, `:1073` | low (correctness) | open |
| KI-14 | Version negotiation exists; the header envelope is the first thing to branch on it | `Messages/Protocol.cs:8,14,21`, `MeshHub.cs:1381-1403`, `:2299` | low (forward-compatibility) | **resolved for the header envelope** — PR #74 (issue #32) added the first real branch on `NegotiatedProtocolVersion`; see history below |
| KI-15 | `AuthenticationFailed` conflates refusal, throw, timeout and slot starvation | `MeshHub.cs:1405-1483` | medium (maintainability) | open — by design |
| KI-16 | A reconnector's credential is fixed at construction and cannot be rotated | `MeshClientReconnector.cs:94`, `:112`, `:177`, `:287` | medium (correctness) | open |
| KI-17 | Sender identity is hop-by-hop: a compromised hub can forge any sender | system-wide | medium (security) | open — by design |
| KI-18 | A failed TLS handshake is silent — the hub sees nothing at all | `TcpTransportListener.cs:583-597` | medium (maintainability) | open — by design |
| KI-19 | A queued reconnect signal means the connection *was* lost, not that it still is | `MeshClientReconnector.cs:273-276`, `:200-203` | high (correctness) | **load-bearing** — guard added by PR #60; do not remove |
| KI-20 | The caller owns the transport until `ConnectAsync` accepts it | `MeshClient.cs:180-204`, `MeshClientReconnector.cs:289-296`, `:171-185` | medium (resource correctness) | **partly addressed** — retry path fixed by PR #60; `StartAsync` still leaks |
| KI-21 | A `DisconnectAsync` arriving after the teardown publishes its state still raises `Disconnected` | `MeshClient.cs:1339-1361`, `:312-315` | low (correctness) | open — **accepted residual** of PR #62 (issue #10); the claim protocol around it is **load-bearing**, do not remove |
| KI-22 | A listener disposed under a pending accept must end it with `ObjectDisposedException` | `Transport/ITransportListener.cs:6-22`, `TcpTransportListener.cs:242-297`, `:307-380`, `InMemoryTransportListener.cs:57`, `:75-90` | high (correctness) | **fixed** — PR #63 (issue #11); the contract and both translations are **load-bearing**, do not remove |
| KI-23 | `MeshHub.StopAsync` was not safe under concurrent invocation | `MeshHub.cs:123`, `:455-484`, `:490-521`, `:527-575` | high (correctness) | **fixed** — PR #64 (issue #12); the lock discipline and the shared `_stopTask` are **load-bearing**, do not remove |
| KI-24 | The shutdown's disconnect notification is sequential and has no send timeout | `MeshHub.cs:562-572` | medium (perf / availability) | open — pre-existing, unchanged by PR #64 |
| KI-25 | A stopped hub is not restartable in general | `MeshHub.cs:517`, `Transport/ITransportListener.cs` | medium (maintainability) | open — by design, documented on `IMeshHub.StopAsync` |
| KI-26 | `maxClients` was a soft cap — non-atomic check-then-act let a burst overshoot it | `MeshHub.cs:109`, `:938-942`, `:959-965`, `:1192-1195`, `:1230`, `:1259` | high (correctness) | **fixed** — PR #65 (issue #13); the atomic claim, its position *after* authentication, and the claim/release pairing are **load-bearing**, do not remove |
| KI-27 | `GroupJoinRefused` carries no correlation identifier | `MeshHub.cs:2242-2244`, `MeshClient.cs:1173-1185` | low (correctness) | open — **accepted**, fail-safe by construction |
| KI-28 | The group authoriser has no concurrency cap, and the timeout does not stop the callback | `MeshHub.cs:2157-2187` | medium (perf / availability) | open — **deliberate**; bounding it is the integrator's job |
| KI-29 | `MeshHub` had unbounded resource-consumption defaults | `MeshHub.cs:26-40`, `:206-287`, `:707-858` | high (availability / security) | **fixed** — PR #68 (issue #16, merged as `76f9c89`); the new defaults are a **breaking behavioural change** for any hub relying on the old unlimited/disabled ones |
| KI-30 | `AddMeshClient` was not idempotent for its hosted service, unlike `AddMeshHub` | `MeshClientServiceCollectionExtensions.cs` | medium (correctness / availability) | **fixed** — caught in review and corrected before merge in PR #70; the keyed registration-marker guard is **load-bearing**, do not remove |
| KI-31 | Mapping the health checks to an unauthenticated HTTP endpoint leaks operational detail | `MeshHubHealthCheck.cs:37-42`, `MeshClientHealthCheckBuilderExtensions.cs:41` | medium (security / information disclosure) | open — **not a defect in this repo**, a caveat for how a consumer wires the checks up |
| KI-32 | `messages.routed` and `messages.dropped` are not complementary — for `broadcast`/`group` sends, and (since PR #85) for an expired `direct` send too | `MeshHub.cs:2064-2067`, `:2144-2145`, `:1824`, `:2162`, `:1460-1490` | low (observability / maintainability) | open — **by design**, a documentation nuance rather than a defect; PR #74's header-bearing routing methods inherit the original asymmetry unchanged, PR #85 adds a second, distinct one that reaches `direct` |
| KI-33 | An oversized outbound frame used to leak a client's registration slot permanently | `MeshHub.cs:1791-1807` | high (availability) | **fixed** — PR #74 (issue #32); `SendLoopAsync`'s catch now treats `ArgumentException` as a transport fault, do not narrow it back to `IOException`/`ObjectDisposedException` only |
| KI-34 | `MessageHeaders`'s constructor throws on a duplicate key instead of last-wins | `Messages/MessageHeaders.cs:41-45` | low (usability) | open — **by design, but undocumented on the type itself**; differs from a plain `Dictionary` object initializer |
| KI-35 | `WebSocketTransportListener` negotiates every connection off the accept path, including cleartext ones | `Transport/WebSocket/WebSocketTransportListener.cs:180-211`, `:317-373` | low (maintainability) | open — **by design**, but a real behavioural difference from `TcpTransportListener` |
| KI-36 | Constructor's `path` doc claimed `404` for a wrong upgrade path; the code always sends `400` | `Transport/WebSocket/WebSocketTransportListener.cs:80-83`, `:420-428`, `:505-555` | low (maintainability / doc accuracy) | **fixed** — PR #78; the XML doc now says `400 Bad Request` and notes the two causes are indistinguishable |
| KI-37 | A pipelined first WebSocket frame ahead of the `101` response has a dedicated regression test | `Transport/WebSocket/WebSocketTransportListener.cs:432-439`, `:575-613`, `:658-739` | low (test coverage) | **fixed** — PR #78; `WebSocketTransportLoopbackTests.ConnectAndAccept_ClientPipelinesFirstFrameAheadOfUpgradeResponse_LeftoverBytesAreNotLost` drives the `LeftoverPrefixedStream` path directly |
| KI-38 | `UnixSocketTransport`/`NamedPipeTransport` bypass the per-remote-endpoint connection cap entirely | `MeshHub.cs:772-782`, `:743-748`; `Transport/Unix/UnixSocketTransport.cs:22`; `Transport/NamedPipes/NamedPipeTransport.cs:23` | high (availability / security) | open — **deliberately deferred**, out of scope for PR #81 (issue #20) by the issue's own design; see full reasoning below |
| KI-39 | `UnixSocketTransportListener`'s `deleteExistingSocketFile` parameter silently also disables cleanup-on-dispose | `Transport/Unix/UnixSocketTransportListener.cs:59-67`, `:84-87`, `:167-170` | low (usability / test coverage) | open — **by design, correctly documented on the constructor, but untested for the `false` case** |
| KI-40 | `QuicTransportListener`'s per-source negotiation cap mitigates, but does not eliminate, a many-source flood | `Transport/Quic/QuicTransportListener.cs:421-516` | medium (availability) | open — **by design, a documented limitation of the mitigation, not a defect in it**; see full reasoning below |
| KI-41 | `QuicTransportListener.StartAsync` is not safe under concurrent invocation — unlike every other listener in this codebase | `Transport/Quic/QuicTransportListener.cs` | medium (correctness / resource leak) | **fixed** — same PR (#82), commit `d4de3b3`; a `_starting` flag now serialises concurrent `StartAsync` calls, mirroring `MeshHub.StartAsync`'s identical pattern; see `StartAsync_CalledConcurrently_OnlyOneSucceeds` |
| KI-42 | `SendAsync`'s headers overload now rejects six specific header keys | `MeshClient.cs:384`, `:543-555` | low (usability / back-compat) | open — **by design**; a caller already using `"mesh.request-id"`/`"mesh.reply"` (PR #83), `"mesh.ack-id"`/`"mesh.ack-request"`/`"mesh.ack"` (PR #84) or `"mesh.expires-at"` (PR #85) as its own header key now gets `ArgumentException` where it previously succeeded |
| KI-43 | Request/response is a client-side convention the hub cannot see or protect — any inbound `mesh.reply` header is intercepted, matched or not | `MeshClient.cs:1097`, `:1439-1484` | low (correctness) | open — **by design**; only a real risk for a non-`MeshClient` peer, or code that hand-builds a `SendMessageWithHeaders` frame outside `SendAsync`; PR #84 added the identical pattern for `mesh.ack`, see KI-46 |
| KI-44 | Delivery acknowledgement is being sent while the message hasn't necessarily been received | `MeshClient.cs:1100-1126` | low (correctness) | open — **by design**; "acknowledged" means "handed to the application", not "the handler succeeded" |
| KI-45 | A `SendAsync(..., DeliveryOptions.RequireAck(...))` timeout does not prove the message was not delivered | `MeshClient.cs:390-443`, `:1549-1596` | low (correctness) | open — **by design**; the acknowledgement is an ordinary routed message and can be lost the same way any other send can (KI-1, KI-5) |
| KI-46 | Delivery acknowledgement is also a client-side convention the hub cannot see or protect — any inbound `mesh.ack` header is intercepted, matched or not | `MeshClient.cs:1096`, `:1502-1547` | low (correctness) | open — **by design**; the acknowledgement mirror of KI-43, added by PR #84 |
| KI-47 | Message time-to-live is measured against the *sender's* clock, with no hub clock authority | `MeshClient.cs:446-466`, `:1407-1422`; `MeshHub.cs:1638-1668`; `Messages/MessageExpiryHeaderKeys.cs` | medium (correctness) | open — **by design**; a documented clock-skew caveat, not a bug, but a real source of surprising drops or non-drops under an unsynchronised clock |
| KI-48 | `DeliveryOptions.AwaitCapacity` parks the sender's whole connection, not just the one message | `MeshHub.cs:1587-1617`, `:1945-1956` | medium (performance / availability) | open — **by design**; every other message that sender addresses to any other recipient queues up behind the one being awaited, for up to `backpressureAwaitTimeout` |
| KI-49 | `RequireAck` + `AwaitCapacity` time out independently — an ack timeout can fail a send the hub still delivers | `DeliveryOptions.cs:91-108`, `MeshClient.cs:393-464` | medium (correctness) | open — **by design**; a retrying caller can duplicate a message that was reported failed but was in fact delivered late |

---

### KI-1 — Full outbound queue drops the frame by default
- **Where:** `RouteMessage` `MeshHub.cs:1892`, `RouteMessageWithHeaders` `:1958`, `BroadcastMessage`
  `:2049`, `SendToGroup` `:2392`, `SendToGroupWithHeaders` `:2476`. Queue is a bounded `Channel<byte[]>`
  of capacity **1024** per connection (`MeshHub.cs:2546`, `:2623`).
- **Why it bites:** the hub delivers with `TryWrite` (or, for `RouteMessageWithHeaders` since PR #87,
  an optional bounded await — see below). If a recipient's consumer (its transport) is slow enough to
  fill 1024 queued frames, every further frame for it is **dropped and logged at Warning**. Do nothing
  extra and this is still exactly as silent as before: no exception, no back-pressure to the sender, and
  a `SendAsync` that "succeeds" guarantees only that the hub accepted the frame, never that it was
  queued for, let alone delivered to, the recipient.
- **What changed (PR #87, issue #30):** three independent, separately opt-in signals now exist, none of
  which are on by default:
  1. **`MeshHub.QueueSaturated` — in-process, always raised, every send shape, no opt-in required.**
     Raised from all five sites above the moment a drop happens (`RaiseQueueSaturated`,
     `MeshHub.cs:1517-1534`), carrying both the sender's and the dropped recipient's id. Free to
     subscribe to; costs nothing if nobody does.
  2. **The `0x15 QueueSaturated` wire frame — opt-in (`notifyOnQueueSaturation` constructor parameter,
     default `false`), direct sends only.** `RouteMessage`/`RouteMessageWithHeaders` best-effort notify
     the sender's own client (`NotifySenderOfQueueSaturation`, `MeshHub.cs:1555-1576`), which surfaces
     it as `MeshClient.SendRejected`. **`BroadcastMessage`/`SendToGroup`/`SendToGroupWithHeaders` never
     send this frame, whatever `notifyOnQueueSaturation` is set to** — the dropped recipient's identity
     there comes from the hub's own registries, not the sender, so echoing it back would let a sender
     enumerate every connected client's id by broadcasting until somebody's queue filled. This asymmetry
     is deliberate and security-motivated, not a gap to "fix" by extending it to the fan-out paths — see
     [hub.md](hub.md#backpressure-signalling-and-awaiting-capacity).
  3. **`DeliveryOptions.AwaitCapacity` — opt-in per send, direct sends only, via
     `RouteMessageWithHeaders`.** Instead of dropping on a full queue, the hub parks the *sender's own*
     receive loop and awaits room (`TryAwaitCapacityAsync`, `MeshHub.cs:1587-1617`), bounded by the
     hub's `backpressureAwaitTimeout` (default 30 s). This is the only one of the three that can turn a
     would-be drop into a late delivery instead. See the head-of-line-blocking and `RequireAck`
     interaction notes below.
- **What did not change:** a hub built with none of the above set (the default) drops exactly as it
  always has. `BroadcastMessage`, `SendToGroup` and `SendToGroupWithHeaders` cannot be told to await
  capacity at all — only a direct send with headers can. See
  [known-issues.md](known-issues.md) KI-48 for the head-of-line-blocking consequence of using
  `AwaitCapacity`, and KI-49 for its interaction with `DeliveryOptions.RequireAck`.
- **What to do:** treat delivery as lossy by default. Subscribe to `QueueSaturated` (or the wire
  notification, for direct sends) if you need to observe drops; use `DeliveryOptions.AwaitCapacity` only
  for traffic that genuinely must not be lost, not as a blanket default, given the head-of-line-blocking
  trade-off in KI-48. If you raise the queue cap or change to blocking writes instead, understand you
  are trading drop for router-wide head-of-line blocking — do it deliberately and test under a stalled
  consumer.

### KI-2 — Open admission by default; authorisation covers groups only
- **Where:** system-wide; registration `MeshHub.cs:934-1050`, authentication `MeshHub.cs:1405-1483`,
  group authorisation `MeshHub.cs:2079-2226`, group-send membership `MeshHub.cs:2338-2357`.
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
      hub drops a `GroupMessage` from a non-member (`MeshHub.cs:2341`, `:2115-2121`), so a client cannot
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
     handshake failure to tell you — only `TcpTransport.IsEncrypted` (`TcpTransport.cs:58`), which you
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
- **Where:** hub `clientName.Length > Protocol.MaxClientNameLength` (`MeshHub.cs:988`); client
  `clientName.Length > Protocol.MaxClientNameLength` (`MeshClient.cs:185`). `MaxClientNameLength = 256`.
- **Why it bites:** `.Length` counts UTF-16 code units, not encoded bytes. A 256-"character" name of
  multi-byte code points encodes to well over 256 bytes on the wire, and a name of astral characters
  (surrogate pairs) counts each pair as 2. Both sides use the same check so they agree, but any external
  reimplementation that validates bytes will disagree. Not a buffer risk (frames are 1 MiB-bounded), but
  a spec ambiguity.
- **What to do:** if you tighten this, decide bytes-vs-chars explicitly and change both sides + the wire
  docs together.

### KI-4 — Unknown recipient drops the message silently
- **Where:** `RouteMessage` `MeshHub.cs:1877-1884` (logs `Debug`, returns; `RouteMessageWithHeaders`
  identically). `SendToGroup` drops silently on three paths: the group does not exist (`MeshHub.cs:2327`,
  plain `return`, **no log**), **the sender is not a member** (`:2098-2104`, logs `Debug`), and the sender
  is the group's only member (`:2107-2111`, no log).
- **Why it bites:** sending to a stale/never-registered id, or to a group nobody has joined, is a no-op.
  The sender gets no signal. Combined with KI-1, "message sent" never implies "message delivered".
  Since PR #66 the **commonest** cause of a silently-dropped group send is the sender not being a member
  — including the window after `JoinGroupAsync` returned but before the hub applied the join, and the
  case where the join was refused outright. The old "empty group" early return is gone; an empty group is
  simply one you cannot be a member of.
- **What to do:** resolve ids via `GetClientIdByNameAsync` immediately before sending if freshness
  matters, and design for silent loss. For groups, join before you send, and subscribe to
  `GroupJoinRefused` so a refusal is not mistaken for a delivery problem.

### KI-5 — Unordered delivery, no persistence, and (mostly) unacked — **PARTLY ADDRESSED (PR #84)**
- **Where:** system-wide. Fan-out is per-recipient queues; there is no sequence number or store, and no
  ordering guarantee across the fan-out.
- **Status:** the "unacked" half of this entry is no longer true for a single direct send. PR #84 added
  `SendAsync(Guid recipientId, ReadOnlyMemory<byte>, DeliveryOptions.RequireAck(TimeSpan), CancellationToken)`
  (`MeshClient.cs:390-443`, see [client.md](client.md#delivery-acknowledgement)) — the call completes only
  once the recipient's client has raised `MessageReceived` for the message and sent back an
  acknowledgement, or throws `TimeoutException`. This is entirely **opt-in and client-to-client**: the hub
  is not involved (see KI-46) and gains nothing from it, and every other send path — the plain
  `SendAsync` overload, `BroadcastAsync`, `SendToGroupAsync` — remains exactly as unacked as before.
- **Why it still bites:** ordering holds only within a single connection's stream, unaffected by PR #84.
  Across a broadcast/group fan-out there is still no global order and no acknowledgement mechanism at all,
  and nothing is retained if a client is offline, acknowledgement or not.
- **What to do:** use `DeliveryOptions.RequireAck` where a single direct send's delivery genuinely needs
  confirming, but read KI-44/KI-45 first — "acknowledged" is a narrower guarantee than it sounds. For
  broadcast/group sends, or anything needing durability or a global order, continue to layer your own
  guarantees on top; this PR does not provide them.

### KI-6 — `StopAsync` writes `Disconnect` outside the send loop
- **Where:** `MeshHub.cs:562-572` (in `StopCoreAsync` since PR #64) — iterates `_clients` and calls
  `client.Transport.SendAsync` directly, concurrently with each connection's still-running send loop.
- **Why it bites:** two writers hit the same transport at once. This is **safe for `TcpTransport`**
  (internal `SemaphoreSlim` write lock, `TcpTransport.cs:29`) and any transport that honours the
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
- **Where:** `Join`/`Leave` frames carry the name as the whole frame remainder (`MeshHub.cs:1110`, `:1052`)
  — effectively bounded only by the 1 MiB frame cap. `GroupMessage`/`DeliverGroupMessage` encode the
  name length as a `u16`, and the client rejects names over `ushort.MaxValue` (`MeshClient.cs:658-661`).
- **Why it bites:** a group name between 65 536 bytes and 1 MiB can be joined but never targeted by a
  group send from the stock client. An edge case, but a real inconsistency.
- **Two consequences PR #66 added, both already mitigated in the code** — know them before you touch
  either path:
  - **The unbounded name reaches your `GroupAuthoriser`** as `GroupJoinContext.GroupName`. Do not use it
    as a dictionary key, a log field or a filesystem path without bounding it yourself.
  - **The unbounded name reaches the hub's log lines.** The refusal paths log at `Warning`/`Error` and
    are reachable at will by any admitted client, so an unclipped name would let one client choose how
    much the hub writes. `ForLog` clips to 64 characters (`MeshHub.cs:2128`, `:1898-1903`); run any new
    log line on this path through it. Note `MeshClient` does **not** clip (`MeshClient.cs:1187`) — the
    name there came from your own hub.
- **What to do:** keep group names short. If you unify the limit, apply it at join time too.

### KI-9 — Malformed/short/unknown frames silently ignored
- **Where:** dispatch ladders `MeshHub.cs:1081-1199` and `MeshClient.cs:1035-1240` — length-guarded
  `else if` chains with no terminal `else`. PR #74 (issue #32) added one more `else if` to each side for
  the two header-bearing opcodes it introduced, growing the client's ladder from 121 to 188 lines and the
  hub's from 77 to 118, without changing the shape. **PR #83, PR #84 and PR #85 each grew the client's
  ladder further without adding a branch** — all three nested a check inside the same existing
  `DeliverMessageWithHeaders` branch, now a three-condition `if` (`MeshClient.cs:1096-1098`): PR #83's
  `TryCompletePendingRequest`, PR #84's `TryCompletePendingAck` ahead of it, then PR #85's `!IsExpired`
  appended as the third and final condition. **PR #85 additionally added a check to the
  `DeliverGroupMessageWithHeaders` branch** (`:1150`), which previously had none — the first time that
  branch has been nested rather than left as a plain `if (headers is not null)`. The ladder still has the
  same number of `else if`s after all three changes, so the "add a branch at the exact offset" guidance
  below is unaffected.
- **Why it bites:** a frame that is too short for its opcode, or has an unknown opcode, is dropped with
  no error and no warning-level log. A framing/offset bug manifests as "nothing happens", which is hard
  to diagnose.
- **Also applies to registration** (`MeshHub.cs:956-984`): a frame under 3 bytes, under 5 bytes, with a
  zero name length, or with a name length running past the payload, drops the connection with **no error
  frame**. The client sees a closed connection rather than a `RegistrationRefusedException`, so a
  framing bug there looks like "the hub is down".
- **It is also load-bearing, not merely tolerated.** The fall-through is what let `GroupJoinRefused`
  (`0x10`) be added **within** protocol version 3: an older client that receives one ignores it, which is
  indistinguishable from the opcode never having existed. Any future hub → client opcode may rely on the
  same property — but a *client → hub* opcode may not, and neither may a change to an existing frame's
  layout. See [protocol.md](protocol.md#additive-opcodes-within-a-version). PR #74's header-bearing
  opcodes are the counter-example: two of the four travel client → hub, so they took the version-bump
  route instead — see [protocol.md](protocol.md#message-headers).
- **What to do:** when adding an opcode, add the guard *and* the branch on the correct side at the exact
  offsets in [protocol.md](protocol.md). When debugging missing messages, check framing first.

### KI-10 — `JoinedGroups` drift — **LARGELY ADDRESSED (PR #52, then PR #66)**
- **Where:** `JoinGroupAsync` `MeshClient.cs:580-618`, `LeaveGroupAsync` `:621-634`, refusal handling
  `:1139-1164`; reconnector restore `MeshClientReconnector.cs:316-340`.
- **Status:** the two claims this entry originally made are **both now false** and are corrected here
  rather than deleted, because older notes still repeat them:
  - *"No auto-rejoin on reconnect"* — wrong since **PR #52**. `restoreGroupMembership` defaults to
    `true` and `RestoreGroupMembershipAsync` re-joins each group over the wire
    (`MeshClientReconnector.cs:335`).
  - *"The client records membership after sending"* — wrong since **PR #66** for joins. `JoinGroupAsync`
    now records **before** sending (`MeshClient.cs:590-594`) precisely so a refusal that arrives first is
    not undone, and rolls the record back on send failure only when that call is what added it
    (`:608-614`). `LeaveGroupAsync` still sends first, then removes (`:627-633`).
- **What still bites (the residual):**
  1. **A lost or refused frame can still diverge the two views.** Membership is fire-and-forget; there is
     no ack for a *successful* join. A refusal now corrects the client (`MeshClient.cs:1173-1185` removes
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
- **Where:** `MonitorHeartbeatAsync`, `MeshHub.cs:1851`.
- **Status:** resolved by PR #61. The comparison is now `missedHeartbeats >= _maxMissedHeartbeats`
  (it was `>`), so eviction fires on the **Nth** consecutive silent interval, which is what the
  parameter name and the XML docs always claimed. Retained here because the behaviour of any hub built
  before that commit differs, and because the fix is easy to regress.
- **What it was:** "max missed heartbeats = N" tolerated N idle intervals and evicted on the (N+1)th.
  With the default 2, a silent client was pinged twice and evicted after 3 idle intervals, so sizing a
  client `idleTimeout` or an SLA around the literal parameter value came out one interval short.
- **What it is now:** a silent client is evicted on the Nth silent interval and probed **N − 1** times
  on the way there, because the threshold check precedes the `Ping` enqueue (`MeshHub.cs:1851-1863`). The
  ping cadence for a live client is unchanged — only the eviction point moved. **N = 1 therefore never
  probes at all**; the constructor logs a `Warning` for that combination (`MeshHub.cs:341-354`). Full
  schedule table in [hub.md](hub.md#heartbeat-schedule).
- **What to do:** if you had compensated for the old +1 in a deployment's `idleTimeout`, hub-side
  eviction now happens one interval sooner than it used to — re-check that the client's `idleTimeout`
  is still comfortably above `heartbeatInterval`. Do not loosen the comparison back to `>`; three tests
  pin the schedule (`MeshHubTests.cs:2211`, `:2311`, `:2355`), and they assert the **ping count** at
  eviction precisely because an "was it evicted?" assertion cannot distinguish N from N+1.
- **Note:** since PR #68 (issue #16) `heartbeatInterval` defaults to 30 seconds rather than `null`, so
  idle eviction — and this schedule — now applies to every hub that does not explicitly pass
  `Timeout.InfiniteTimeSpan`. See [hub.md](hub.md#using-it-efficiently) and the constructor row in
  [for-clanker.md](../for-clanker.md#5-configuration--environment).

### KI-12 — One client lookup in flight at a time
- **Where:** `GetClientIdByNameAsync` serialised by `_lookupLock` (`SemaphoreSlim(1,1)`) with a
  single-slot `_pendingLookup` (`MeshClient.cs:798-844`).
- **Why it bites:** concurrent lookups on the same client queue rather than pipeline — throughput of
  name resolution is one round-trip at a time. Correct (correlation ids prevent cross-talk), just not
  parallel. **Not** the same limitation as `RequestAsync` (PR #83), which has no such serialisation — see
  [client.md](client.md#request-response).
- **What to do:** batch/caches name→id at the app layer if you resolve many names hot. Don't remove the
  correlation-id guard if you parallelise — it is what prevents a cancelled lookup resolving a later one.

### KI-13 — Event `Data` is a view over the received frame
- **Where:** `MessageReceived`/`GroupMessageReceived` args carry `ReadOnlyMemory<byte>` slices of the
  frame (`MeshClient.cs:1046`, `:1073`).
- **Why it bites:** the memory is only contractually valid during the handler. It happens to be backed
  by a fresh per-frame `byte[]` today (`TcpTransport.ReceiveAsync` allocates per frame), so retaining it
  works — but a future pooled-buffer transport would invalidate anything you kept.
- **What to do:** copy (`.ToArray()` / `Span.CopyTo`) if you retain the payload past the handler. This
  applies equally to the `Headers` a header-bearing frame carries (PR #74) — `MessageHeaders` itself is
  immutable and safe to retain, but the strings it holds are already independent copies decoded off the
  frame, so no special handling is needed there.

### KI-14 — Version negotiation exists; the header envelope is the first thing to branch on it
- **Where:** `Protocol.MinSupportedVersion` / `Protocol.MaxSupportedVersion` (`Messages/Protocol.cs:8`,
  `:14`, `4`/`5` as of PR #74) and `Protocol.HeaderEnvelopeMinVersion` (`:21`, added by PR #74);
  `MeshHub.TryNegotiateProtocolVersion` (`MeshHub.cs:1381-1403`, called at `:898`);
  `IMeshClient.NegotiatedProtocolVersion` (`IMeshClient.cs:28`) and its implementation
  (`MeshClient.cs:137`, set at `:240`, reset to `0` on disconnect at `:362` and `:1343`);
  `MeshHub.ClientConnection.NegotiatedProtocolVersion` (`MeshHub.cs:2560`, captured once as a constructor
  parameter, constructed at `:974`).
- **History.** This entry previously described protocol v3 as a hard break on both the wire and in
  source. PR #73 (issue #47) replaced the single-byte `Protocol.Version` equality check with min/max
  range negotiation, but at the time **nothing downstream read the negotiated version except the
  logger** — `MinSupportedVersion == MaxSupportedVersion == 4` made the gap inert by construction, and the
  entry was narrowed to record that as an open, deliberate gap.
- **Resolved (for this one capability) by PR #74 (issue #32).** `MaxSupportedVersion` was raised to `5`
  and `HeaderEnvelopeMinVersion` added to mark where the structured message-header envelope
  (`MessageHeaders`) becomes available. Both sides now actually read `NegotiatedProtocolVersion` before
  doing something the other side might not understand:
  - **`MeshClient`** refuses to *send* a non-empty `MessageHeaders` on a connection negotiated below `5`,
    throwing `NotSupportedException` (`RequireHeaderEnvelopeSupport`, `MeshClient.cs:696-705`) — headers
    are never silently dropped on the wire. Since PR #83 this check is also reached from `RequestAsync`/
    `ReplyAsync`, and since PR #84 from `SendAsync(..., DeliveryOptions.RequireAck(...), ...)`, all of
    which share the same internal `SendCoreAsync` (`:468-521`) as `SendAsync`'s headers overload.
  - **`MeshHub`** decides, **per recipient**, whether to forward a header-bearing frame unchanged or
    strip it to the plain equivalent (`RouteMessageWithHeaders`/`SendToGroupWithHeaders`,
    `MeshHub.cs:1924`/`:2177`) — a group with members on mixed negotiated versions gets mixed frame
    shapes for the same send. See [protocol.md](protocol.md#message-headers) and
    [hub.md](hub.md#routing-helpers).
- **What remains open:** this closes the gap **for the header envelope specifically**, not in general.
  The next capability gated the same way still needs its own `Protocol.XyzMinVersion` constant and its
  own explicit check on both sides — negotiating a version number is still not sufficient by itself; the
  pattern PR #74 established is the one to imitate (see [protocol.md](protocol.md#versioning)).
- **What to do:** for a genuinely optional, hub → client-only capability, still prefer the additive-opcode
  route ([protocol.md](protocol.md#additive-opcodes-within-a-version)) over a version bump — PR #74's
  header opcodes needed a bump specifically because two of the four travel client → hub, which that route
  excludes. Do not remove `TryNegotiateProtocolVersion`'s inverted-range or non-overlap checks, or
  `ClientConnection.NegotiatedProtocolVersion`'s immutability (it is captured once at registration and
  never revised) — both are load-bearing for the frame-shape decision now built on top of them.

### KI-15 — `AuthenticationFailed` conflates every non-success outcome
- **Where:** `AuthenticateAsync` (`MeshHub.cs:1405-1483`) returns `false` for a refusal, a throw, a
  cancellation inside the callback, a callback that exceeds `registrationTimeout`, and a failure to
  acquire an authentication slot. The caller sends the same
  `Error(AuthenticationFailed)` in all cases (`MeshHub.cs:1013-1014`).
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
- **Where:** `MeshClientReconnector` takes `credential` as a constructor parameter
  (`MeshClientReconnector.cs:94`) and stores it into the `readonly _credential` field (declared `:39`,
  assigned `:112`), then replays it on every connect and reconnect (`:177`, `:287`).
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
  `RouteMessage` `MeshHub.cs:1889`, `BroadcastMessage` `:1796`, `SendToGroup` `:2136` (and identically the
  header-bearing variants) — from its own record of the connection, and nothing in `Messages/` carries a
  signature. TLS, where configured, secures the client↔hub connection only.
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
  (`MeshClientReconnector.cs:273-276`); the signal sources are `OnDisconnected` (`:225`) and the
  post-subscription state re-check in `StartAsync` (`:200-203`).
- **Why it bites:** the reconnector's trigger is **level-based, not edge-based**. A signal sitting in
  the capacity-1 channel is a record that a drop *happened*, not a guarantee the client is still down by
  the time the loop services it. Three ways a signal goes stale:
  1. **One drop, two signals.** `Client.IsConnected` is false during `Disconnecting` as well as
     `Disconnected`, so a teardown that straddles the subscription line is seen *both* by `StartAsync`'s
     state re-check and, moments later, by `OnDisconnected` when the event finally fires. The channel is
     `DropWrite` capacity 1, so it coalesces only signals that overlap *in the queue* — these two do
     not, and the second survives as a duplicate for a drop already serviced.
  2. **An application handler reconnected from inside `Disconnected`.** `MeshClient` explicitly supports
     this (`MeshClient.cs:245-255`), so the connection can be live again before the loop wakes.
  3. **An earlier pass already recovered it.**
- **What it costs if the guard goes:** `MeshClient.ConnectAsync` **refuses** a connect unless the client
  is fully `Disconnected` (`MeshClient.cs:185-193`), so servicing a stale signal does not merely waste a
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
- **Where:** `MeshClient.ConnectAsync` adopts the transport at `MeshClient.cs:206`, *after* its argument
  and state validation (`:182-203`). A throw from that validation therefore leaves the transport
  unowned and unclosed; a throw after adoption is cleaned up by `CleanUpAsync` (`:273`, disposal at
  `:855-858`). The reconnector's retry path handles the gap (`MeshClientReconnector.cs:289-296`).
- **Why it bites:** the reachable case is not a programming error. A reconnect attempt racing a teardown
  hits the `ConnectionState.Disconnecting` guard (`MeshClient.cs:199`) and is rejected with
  `InvalidOperationException` before adoption — so before PR #60 every such attempt **abandoned a live
  transport**, one connected socket leaked per rejected retry, on a path that retries indefinitely.
- **Two things this leaves you with:**
  1. **`StartAsync` has no equivalent guard.** Its connect (`MeshClientReconnector.cs:171-185`) resets
     the started flag and rethrows without disposing the transport. Bounded to one per call, but
     `StartAsync` is documented as retryable, so a caller looping it leaks one transport per attempt.
     *(Inference from reading both paths; no test covers it.)*
  2. **`ITransport.DisposeAsync` must now be idempotent.** The reconnector disposes on *any*
     `ConnectAsync` throw, including the post-adoption ones the client already cleaned up — the code
     relies on the second disposal being harmless. `ITransport`'s `<remarks>` documents a concurrency
     contract but says **nothing** about idempotent disposal (`Transport/ITransport.cs:3-15`). Both
     in-tree implementations happen to satisfy it — `InMemoryTransport` guards explicitly
     (`InMemoryTransport.cs:68-77`), `TcpTransport` inherits it from `Stream`/`TcpClient`/`SemaphoreSlim`
     (`TcpTransport.cs:253-258`) — but a custom transport that throws on second disposal will fail here.
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
- **Where:** `HandleReceiveLoopTerminationAsync` releases `_stateLock` at `MeshClient.cs:1350` and
  invokes the event at `:1361`. The claim `DisconnectAsync` would need to lay is at `:312-315`.
- **Status:** **open, and deliberately so.** This is the residual window that PR #62 (issue #10)
  knowingly did not close, not an oversight. The code says as much in the XML docs on
  `HandleReceiveLoopTerminationAsync` (`MeshClient.cs:1303-1311`), `IMeshClient.DisconnectAsync`
  (`IMeshClient.cs:70-83`) and `IMeshClient.Disconnected` (`:367-377` — this citation was found pointing
  at `RequestAsync`'s declaration and corrected this pass, see [client.md](client.md)'s coordinate
  caveat), and in `README.md`.
- **Why it bites:** PR #62 made a local disconnect racing a remote drop silent whichever side wins, and
  it is tempting to read that as an absolute guarantee. It is not. The guarantee holds only up to the
  moment the teardown takes its raise decision. Concretely, `HandleReceiveLoopTerminationAsync` reads
  `_localDisconnectRequested` into `raiseDisconnected` inside the same locked block that sets
  `_state = ConnectionState.Disconnected` (`MeshClient.cs:1339-1350`), then **releases the lock** before
  invoking the delegate (`:1359-1361`). A `DisconnectAsync` entering in that gap finds the state is
  already `Disconnected`, not `Disconnecting`, so the `if (_state is ConnectionState.Disconnecting)`
  claim at `:312` does not fire. It lays no claim, returns as a genuine no-op — and the event the
  application was trying to suppress fires anyway, with `DisconnectReason.ConnectionLost`.
  The window is a handful of instructions wide, so it is rare, but it is real and it is not testable by
  the seam the PR's own tests use (those pin the *earlier* interleaving; see
  [testing.md](testing.md#testing-conventions-follow-these)).
- **Why it is not closed:** closing it would mean holding `_stateLock` across the
  `Disconnected?.Invoke` so that no `DisconnectAsync` could interleave between the decision and the
  raise. That directly contradicts a documented, supported pattern — **a handler may reconnect
  synchronously via `ConnectAsync` from inside `Disconnected`** (`IMeshClient.cs:367-377`), which is
  exactly how `MeshClientReconnector` behaves. `ConnectAsync` takes `_stateLock` itself
  (`MeshClient.cs:192`), so invoking the event under the lock would deadlock every such handler. The
  trade is deliberate: a rare spurious `Disconnected` is preferable to a guaranteed deadlock on a
  supported path.
- **What to do:**
  - Treat "no `Disconnected` after `DisconnectAsync`" as **overwhelmingly reliable, not guaranteed**.
    If your handler must be exactly-once, make it idempotent, or gate it on your own
    "I asked for this" flag set before you call `DisconnectAsync` — do not rely solely on the client's
    suppression.
  - **Do not remove the claim protocol** (`MeshClient.cs:312-315`, `:1337-1357`, `:205`) on the reasoning
    that it "does not fully work". It closes the wide, easily-hit window; only the narrow one remains.
    This is load-bearing in the same sense as KI-19's revalidation guard.
  - If you do attempt to close the residual window, the constraint to design against is the synchronous
    reconnect-from-handler pattern, not the lock itself. Anything that ends with the event being raised
    under `_stateLock` is wrong. Prove any change with a test that reconnects from inside `Disconnected`.

### KI-22 — A listener disposed under a pending accept must end it with `ObjectDisposedException` — **FIXED (PR #63, issue #11)**
- **Where:** the contract on `ITransportListener`'s `<remarks>` (`Transport/ITransportListener.cs:6-22`);
  the `TcpTransportListener` implementation (`:242-297` for accept, `:307-380` for disposal); the
  `InMemoryTransportListener` implementation (`InMemoryTransportListener.cs:57`, `:75-90`); the consumer
  that depends on it, `MeshHub.AcceptLoopAsync` (`MeshHub.cs:740-765`).
- **Status:** resolved by PR #63. Retained because the resulting behaviour is **load-bearing** in three
  separate places, each easy to regress, and because a custom `ITransportListener` has to satisfy the
  same contract.
- **Why the exception type is the whole issue:** `MeshHub.AcceptLoopAsync` breaks on
  `OperationCanceledException`/`ObjectDisposedException` and treats **everything else** as one bad
  connection — logged at Warning and retried, with `continue` and **no delay** (`MeshHub.cs:757-765`).
  Against a listener that is never coming back, "anything else" is therefore an unbounded hot spin that
  floods the log and pins a core. A listener that fails to report its own disposal correctly does not
  merely confuse a caller; it takes the hub with it.
- **What was wrong, and what each part now guarantees:**
  1. **The data race the issue was raised for.** `TcpTransportListener.AcceptAsync` null-checked
     `_listener` and then dereferenced it, while `DisposeAsync` set it to `null`. A dispose landing
     between the two produced a `NullReferenceException` — which the accept loop then logged and
     retried. All mutable state is now guarded by a `Lock _stateLock` (`:74`) and every entry point
     captures what it needs **once** into locals (`:291-297`). *The hub itself never triggered this — it
     cancels the accept token and awaits the loop before disposing — so the reachable case was
     standalone use that ignored the interface's "cancel first" remark. The interface now says
     implementations may not rely on callers doing so.*
  2. **The cleartext path did not translate.** `ObjectDisposedException` translation existed only on the
     TLS branch (via `ChannelClosedException`). A **cleartext** listener disposed under a pending accept
     surfaced the raw `SocketException`/`InvalidOperationException` that a stopped `TcpListener` throws —
     straight into the retry-without-delay branch. The new `internal static
     IsStoppedListenerFailure(Exception)` (`:632`) plus a `when (_disposed && …)` filter (`:504-512`)
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
  complete on return. Use the shipped listeners as worked examples — by the time of PR #81 (issue #20)
  there are four socket/pipe-backed ones (`Tcp`, `WebSocket`, `Unix`, `NamedPipe`) plus
  `InMemoryTransportListener`, all satisfying the same contract.
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
- **Where:** `MeshHub.cs:123` (`Lock _stateLock`), `:455-484` (`StopAsync`), `:490-521`
  (`StopCoreAsync`), `:527-575` (`ShutDownAsync`), `:384-445` (`StartAsync`), `:644-672`
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
     (`:513-520`), so the shutdown proper always runs.
  5. **A start racing a stop could abandon a just-bound listener.** `StartAsync` now claims the running
     slot with a `_starting` flag (`:402`) *before* the listener starts, and publishes `_cts` and
     `_acceptLoopTask` together (`:442-443`). A stop can no longer take a token source whose accept loop
     does not exist yet — which would have left the endpoint bound with nothing serving it.
  6. **Disposal ran its teardown once per caller.** `DisposeAsync` memoises in `_disposeTask` (`:653`)
     and sets `_disposed` first (`:652`); the listener is disposed exactly once and a start on a
     disposed hub throws `ObjectDisposedException`.
- **What to do:** keep the discipline — take state under the lock, work from locals, never await while
  holding it. Eight tests pin this (`MeshHubTests.cs:118-357`); two of them park a caller mid-lifecycle
  deterministically rather than relying on thread timing, see [testing.md](testing.md#parking-a-caller-mid-lifecycle).
- **What not to do:** do not make `StopAsync` `async` again — its decision is taken synchronously under
  the lock, and that is what makes the "join the existing shutdown" handover race-free. Do not move
  `ShutDownAsync` out of the `finally`, do not reintroduce a second read of a lifecycle field outside
  the lock, and do not clear `_disposed` — disposal is terminal.

### KI-24 — The shutdown's disconnect notification is sequential and has no send timeout
- **Where:** `MeshHub.cs:562-572` — the `foreach` over `_clients.Values` inside `StopCoreAsync`.
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
- **Where:** `MeshHub.cs:517` (`StopAsync`), `Transport/ITransportListener.cs` (no stop operation).
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
- **Where:** `MeshHub.cs:109` (`private int _reservedClientSlots`), `:938-942` (the pre-authentication
  early-out), `:959-965` (the claim), `:1192-1195` (the release, in `HandleClientAsync`'s `finally`),
  `:1230` (`TryReserveClientSlot`), `:1259` (`ReleaseClientSlot`), `:1292` (`RefuseAtCapacityAsync`).
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
  1. **The claim is a CAS loop, not increment-then-test-and-undo** (`:1230`). An increment that overshoots
     is visible to every other registration until it is backed out, so a burst that all overshot and all
     retreated would refuse clients for slots nobody held. The loop only ever claims from a value that
     was still under the cap at the instant of the claim.
  2. **The claim sits *after* authentication** (`:959`, the authenticator at `:944-951`). Taking it
     earlier would let a peer that never authenticates — or authenticates slowly — hold capacity away
     from one that would.
  3. **The pre-authentication early-out is retained** (`:938-942`) and now reads `_reservedClientSlots`
     rather than `_clients.Count`. It decides nothing; it exists solely to preserve the property that a
     **full** hub never runs the integrator's authenticator, so a connection flood cannot drive
     credential work or pin handler tasks on a slow callback.
- **The pairing is the fragile part.** Every successful claim is owned by exactly one client handler and
  must be given back exactly once, by that handler's `finally`, guarded by the `slotReserved` flag
  (`:867`, `:965`, `:1192`). This covers the duplicate-name refusal as well as ordinary disconnection.
  The release deliberately runs **before** the transport is disposed, so a transport that blocks on close
  cannot hold capacity for as long as it hangs. A claim that escapes its release leaks capacity for the
  lifetime of the hub, and nothing will report it.
- **`ConnectedClientCount` can read *below* the number of claimed slots, and that is intended.** It is
  still `_clients.Count` (`:584`). A registration between its claim and its `_clients` insert, and a
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
  (`MeshHubTests.cs:871`) can create the claimed-but-unregistered window that a count-based check cannot
  see. Three tests pin this (`MeshHubTests.cs:871`, `:927`, `:970`).

### KI-27 — `GroupJoinRefused` carries no correlation identifier
- **Where:** hub builds `[0x10][name bytes]` with nothing else (`RefuseGroupJoin`, `MeshHub.cs:2242-2244`);
  client keys the removal on the decoded name alone (`MeshClient.cs:1173-1185`).
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
  (`MeshHub.cs:2114`).
- **What to do:** if exact membership matters, treat `GroupJoinRefused` as "re-check", not as
  "definitely out of this group", and re-join if you still expect to be a member. Do not build a
  join/refuse retry loop on top of it without your own correlation.
- **What not to do:** do not add a correlation id to the frame in isolation — it changes an existing
  opcode's layout and so requires a `Protocol.MaxSupportedVersion` bump
  ([protocol.md](protocol.md#additive-opcodes-within-a-version)).

### KI-28 — The group authoriser has no concurrency cap, and the timeout does not stop the callback
- **Where:** the deliberate absence is commented at `MeshHub.cs:2157-2168`; the bounded wait is
  `:1939-1944`.
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
  loop. The constructor warns on this combination (`MeshHub.cs:356-421`) but does not prevent it.
- **What to do:** an authoriser that holds a resource per invocation — a database connection, an
  outbound HTTP call — must **bound its own concurrency**, with its own semaphore or a shared pooled
  client. Prefer a synchronous decision against an in-memory policy table where you can; it takes the
  allocation-free fast path. Set `maxClients` if you want a hard ceiling on the fan-out. Keep
  `groupAuthorisationTimeout` below the heartbeat eviction budget.
- **What not to do:** do not assume the timeout releases whatever the callback was holding — it does
  not. Do not add a semaphore to the hub expecting it to help: it would queue joins behind each other
  and make the eviction trap worse, which is why there isn't one.

### KI-29 — `MeshHub` had unbounded resource-consumption defaults — **FIXED (PR #68, issue #16, draft)**
- **Where:** the new constants `MeshHub.cs:26-40` (`DefaultMaxClients`, `DefaultHeartbeatInterval`,
  `DefaultMaxConnectionsPerRemoteEndpoint`); the constructor's default resolution `:206-287`; the whole
  per-remote-endpoint cap machinery in `AcceptLoopAsync` and five new private helpers `:707-858`
  (`ExtractRemoteAddress`, `NormaliseForEndpointCap`, `DisposeRefusedTransportAsync`,
  `TryReserveEndpointSlot`, `ReleaseEndpointSlot`); the new `IRemoteEndPointTransport` interface
  (`Transport/IRemoteEndPointTransport.cs`) and its `TcpTransport` implementation (`TcpTransport.cs:66`).
  `WebSocketTransport` implements it too (added by PR #78). **`UnixSocketTransport` and
  `NamedPipeTransport` (PR #81, issue #20) do not implement it at all** — see KI-38 below for what that
  costs a hub reached only over one of those transports.
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
     (`NormaliseForEndpointCap`, `MeshHub.cs:833-848`) — without this, a single host with a routine `/64`
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

### KI-30 — `AddMeshClient` was not idempotent for its hosted service, unlike `AddMeshHub`
- **Where:** `MeshClientServiceCollectionExtensions.AddMeshClientCore`, the hosted-service registration
  guard immediately before `services.AddHostedService(serviceProvider => new MeshClientHostedService(...))`.
  Contrast `MeshHubServiceCollectionExtensions.cs`, `services.AddHostedService<MeshHubHostedService>();`,
  whose equivalent idempotency remark was already accurate.
- **Status:** **fixed** before merge, in the same PR (#70) that introduced it — caught by documentation
  review rather than by the analyser passes, then confirmed against the code and corrected.
- **What the defect was:** `Microsoft.Extensions.Hosting`'s `AddHostedService<THostedService>()` (the
  type-parameter overload, used by `AddMeshHub`) registers via `TryAddEnumerable`, so calling it twice for
  the same closed type is a no-op the second time. `AddMeshClient` uses
  `AddHostedService(Func<IServiceProvider, THostedService>)` — the **factory** overload — which is a
  plain, undeduplicated `AddSingleton` under the hood, because a factory delegate carries no identity the
  framework can compare. `AddMeshClient("Alice", ...)` called twice would therefore register **two**
  `MeshClientHostedService` instances, both keyed to `"Alice"`, both resolving the *same* keyed
  `IMeshClient`/`MeshClientReconnector` singleton. At host start the generic host runs each
  `IHostedService.StartAsync` in registration order; the first call connects the client (or starts the
  reconnector) successfully, and the second would throw `InvalidOperationException` from the same
  singleton (`MeshClient.ConnectAsync` refuses a second connect; `MeshClientReconnector.StartAsync` refuses
  a second start), which the host propagates as a fatal startup failure.
- **The fix:** a private `MeshClientHostedServiceRegistrationMarker` type is registered as a keyed
  singleton per `clientName` the first time `AddMeshClient` runs for that name; a later call for the same
  name finds the marker already present and skips re-registering the hosted service (the options/keyed-
  client registrations above it still run and still layer, unchanged). This is the moral equivalent, per
  client name, of what `TryAddEnumerable` already gives `AddMeshHub` for free — **load-bearing**, do not
  remove. Covered by
  `MeshClientServiceCollectionExtensionsTests.AddMeshClient_CalledTwiceForTheSameName_RegistersOnlyOneHostedService`.
- **What not to do:** do not "fix" a regression here by switching to the type-parameter
  `AddHostedService<T>()` overload for `MeshClientHostedService` — it takes no constructor arguments
  beyond what DI can supply, but `MeshClientHostedService` needs `clientName` captured per call, which
  that overload has no way to thread through.

### KI-31 — Mapping the health checks to an unauthenticated HTTP endpoint leaks operational detail

- **Where:** `MeshHubHealthCheck.CheckHealthAsync`'s `data` dictionary
  (`MeshHubHealthCheck.cs:37-42`, populated with `connectedClientCount`, `claimedClientSlots` and
  `maxClients`); `MeshClientHealthCheckBuilderExtensions.AddMeshClient`'s default check name
  (`MeshClientHealthCheckBuilderExtensions.cs:41`, `$"meshclient:{clientName}"`), which embeds the
  client's own name.
- **Status:** open — **this is not a defect in either health check or in this repo.** Neither
  `MeshHubHealthCheck` nor `MeshClientHealthCheck` maps itself to any endpoint; that is entirely the
  consuming application's choice, typically ASP.NET Core's `MapHealthChecks(...)`. This entry exists so
  the caveat is on record where an integrator will look for it, not to flag a bug to fix here.
- **What the exposure is:** the standard ASP.NET Core `HealthCheckOptions.ResponseWriter` (or a custom one
  a consumer wires up) commonly serialises each `HealthReport` entry's `Status`, `Description` and `Data`
  to the HTTP response body. For `AddMeshHub`, that includes the hub's connected and claimed client counts
  and its configured `MaxClients` — an approximate measure of load and configured capacity. For
  `AddMeshClient`, the check's own name (`meshclient:{clientName}` by default) discloses the client's
  registered name to anyone who can enumerate the health endpoint's entries, even without reading `Data`.
  Neither is a credential or a secret, but both are the kind of operational fingerprinting detail that
  should not be handed to an unauthenticated caller on the public internet.
- **What to do about it (consumer-side, not this repo's job):** put the health endpoint behind
  authentication/authorisation, restrict it to an internal network or a management port, or use
  `HealthCheckOptions.ResponseWriter` to return only the overall `Status` (no per-check `Data`) on a
  publicly reachable path, reserving the detailed view for an internal one. This mirrors the standard
  advice for `Microsoft.Extensions.Diagnostics.HealthChecks` generally — it is not specific to Meshworx —
  but is worth restating here because `AddMeshHub`/`AddMeshClient` are the first health checks this
  codebase ships, and nothing here documents the endpoint-mapping half of the picture.

---

### KI-32 — `messages.routed` and `messages.dropped` are not complementary — **EXTENDED (PR #85)**
- **Where:** `BroadcastMessage` records `messages.routed`/`bytes.routed` once after its delivery loop, if
  `hasRecipient` is set (`MeshHub.cs:2064-2067`), and records `messages.dropped` (`reason=queue-full`)
  separately, once per recipient whose queue was full, inside the same loop (`:1824`). `SendToGroup`
  follows the identical shape: `messages.routed`/`bytes.routed` once, before the delivery loop
  (`:2144-2145`), and `messages.dropped` once per full queue inside it (`:2162`). **PR #74's
  `SendToGroupWithHeaders` inherits this shape unchanged** (routed at `:2214`, dropped at `:2243`) — it is
  not a new variant of the asymmetry, just the same one applied to the header-bearing path. Full write-up
  of the routed-side semantics in [hub.md](hub.md#metrics).
- **Severity:** low. This is an accurate description of intentional metric semantics, not a bug — but it
  is a trap for anyone who builds a dashboard or alert on top of these instruments without reading the
  code, because the two counters look like they should partition a fan-out send into "delivered" and
  "not delivered" and they do not.
- **Why it bites:** for a **direct** send (`RouteMessage`, and identically `RouteMessageWithHeaders`),
  `messages.routed` only increments after the frame is actually written to the recipient's queue (`:1716`,
  gated by the `TryWrite` check at `:1703`) — so for `direction=direct`, `routed` and
  `dropped(reason=queue-full)`/`dropped(reason=unknown-recipient)` are mutually exclusive outcomes of the
  same call, and "routed − dropped" was a genuine delivered-message count **until PR #85 (issue #29)**.
  For **`broadcast`** and **`group`**, `routed` fires once per call that had at least one candidate
  recipient, **regardless of whether delivery to that recipient (or any of several) actually succeeded**
  — a full outbound queue on every single recipient still leaves the call counted as one `routed` message,
  with N separate `dropped` increments alongside it. A consumer computing "messages successfully delivered
  = routed − dropped" will silently under-count drops (or double-count a call as both routed and entirely
  undelivered) for any hub using broadcast or group traffic under back-pressure.
- **PR #85 introduced a third asymmetry, and this one *does* reach `direction=direct`.**
  `dropped(reason=expired)` (`IsExpiredFrame`, `MeshHub.cs:1638-1668`) is recorded by `SendLoopAsync` at
  **dequeue** time, strictly after `RouteMessage`/`RouteMessageWithHeaders` already incremented `routed`
  at **enqueue** time — the two are different tasks reading the same `OutboundQueue.Channel` at different
  instants, not one gated check. A direct message whose time-to-live (`SendAsync(..., TimeSpan, ...)`,
  see [client.md](client.md#message-expiry-time-to-live)) expires while it sits queued is therefore
  counted **both** `routed` **and** `dropped(reason=expired)` — the "routed and dropped are mutually
  exclusive for direct" claim above no longer holds unconditionally; it holds only for
  `reason=unknown-recipient`/`reason=queue-full`. `broadcast`/`group` sends carrying a time-to-live would
  have the identical double-count issue, but no overload attaches one to them — there is no
  `SendToGroupAsync(..., TimeSpan, ...)` — so this is currently reachable only via `direction=direct`.
  See [hub.md](hub.md#metrics) and [hub.md](hub.md#dropping-expired-frames).
- **What to do:** treat `messages.routed`/`bytes.routed` as "the hub attempted to fan this message out to
  somebody", not "this message was delivered", for `broadcast` and `group`. If you need a true
  delivered-count for those directions, derive it as `routed` fan-out attempts weighed against
  `messages.dropped(reason=queue-full)` per call, which these instruments do not expose — the dropped
  counter has no correlation back to the routed call that produced it beyond direction, since a `Counter`
  carries no per-call structure. Do not "fix" the asymmetry by making `broadcast`/`group` require
  universal delivery before counting `routed`: that would conflate "was this message handed to the
  router" with "did every recipient's queue accept it", which are different questions and both worth
  observing, and would regress the zero-recipient exclusion the metric already gets right (see the
  `BroadcastMessage` comment at `MeshHub.cs:2030-2036` and `SendToGroup`'s early return at
  `:2124-2128`). **For `direction=direct` specifically, treat `dropped(reason=expired)` as its own
  category, not as evidence against `routed`** — a message can legitimately be both `routed` (accepted
  onto the queue) and later `dropped(reason=expired)` (discarded before it left the queue); computing
  "delivered = routed − dropped" for direct sends now needs to exclude the `expired` reason specifically
  to stay accurate, since it is the one `dropped` reason that is not mutually exclusive with `routed`.

### KI-33 — An oversized outbound frame used to leak a client's registration slot permanently — **FIXED (PR #74, issue #32)**
- **Where:** `SendLoopAsync`'s `catch` clause (`MeshHub.cs:1791-1807`), which awaits
  `transport.SendAsync` for every queued delivery frame.
- **Severity:** was high (availability). Reachable without an adversary: any legitimate sender whose
  message, combined with a recipient/group name and (since PR #74) a header block, produces a frame
  larger than the transport's cap (`TcpTransport`'s 1 MiB payload limit, notably) triggers it.
- **What was wrong:** a transport rejecting an oversized frame throws `ArgumentException`. Before this
  fix, `SendLoopAsync` only caught `IOException`/`ObjectDisposedException`, so that exception propagated
  out of the send-loop task. `HandleClientAsync`'s `finally` block awaits that task as part of its own
  teardown — an unhandled fault there aborts the `finally` **partway through**, skipping whatever cleanup
  had not yet run: the client-slot release, the name removal, the group removal, and the connection
  disposal. The one client that triggered it left its capacity slot permanently claimed, invisible to
  `ClaimedClientSlots` reconciling itself, for the lifetime of the hub.
- **The fix:** `SendLoopAsync`'s `catch` now also matches `ArgumentException` and treats it exactly like
  a transport fault — log a warning and cancel the client's `clientCts`, letting `HandleClientAsync`'s
  `finally` run to completion normally. Only the one oversized message is lost; the connection's
  bookkeeping is unaffected.
- **What to do:** do not narrow this catch back to `IOException`/`ObjectDisposedException` only — the
  three-way `when` clause is load-bearing. If you add a new header-bearing or otherwise size-inflating
  frame type, verify it can still trip a transport's cap and that this catch still covers the failure
  mode; do not assume the sender-side length checks (`HeaderEnvelope.GetEncodedLength`'s 65 535-byte
  cap, for instance) make an oversized *outbound* (hub → recipient) frame impossible — they bound one
  component, not the combined frame the hub reassembles for delivery.

### KI-34 — `MessageHeaders`'s constructor throws on a duplicate key instead of last-wins
- **Where:** the public constructor (`Messages/MessageHeaders.cs:41-45`), which copies its input via
  `new Dictionary<string, string>(values, StringComparer.Ordinal)`.
- **Severity:** low (usability). Not a correctness bug in the library — it is a foot-gun for a caller
  building `MessageHeaders` from a source that can itself contain duplicate keys (e.g. mirroring an HTTP
  header collection, or merging two dictionaries by concatenating their pairs).
- **Why it bites:** a plain `Dictionary<TKey,TValue>` built via an object initializer or repeated indexer
  assignment (`d[key] = value`) silently keeps the last value on a duplicate key. `MessageHeaders`'s
  constructor instead passes the enumerable straight to `Dictionary`'s `IEnumerable<KeyValuePair<,>>`
  constructor, which behaves like calling `Add` for each pair and **throws `ArgumentException`** ("An
  item with the same key has already been added") the moment a duplicate appears. A caller who assumes
  `MessageHeaders` behaves like an ordinary dictionary will see this as a surprising crash rather than
  the merge they expected. Confirmed by direct testing against `System.Collections.Generic.Dictionary`'s
  documented behaviour for this constructor overload; not covered by `MessageHeadersTests.cs`, which has
  no duplicate-key case.
- **What to do:** de-duplicate (e.g. `.GroupBy(p => p.Key).Select(g => g.Last())`, or build a
  `Dictionary` yourself and pass it via a future `FromOwnedDictionary`-style entry point if one is ever
  made public) before constructing a `MessageHeaders` from a source that might repeat a key. Do not
  assume the constructor is forgiving of duplicates the way a collection initializer is. The wire-format
  decoder (`HeaderEnvelope.Read`) does **not** have this problem — it builds its `Dictionary` with plain
  indexer assignment (`values[key] = value;`), so a malformed or adversarial header block with a
  duplicate key is fine on receipt; the throw is specific to the public constructor's copy path.

### KI-35 — `WebSocketTransportListener` negotiates every connection off the accept path, including cleartext ones
- **Where:** `StartAsync` launches `NegotiationPumpAsync` unconditionally (`Transport/WebSocket/WebSocketTransportListener.cs:180-211`);
  the pump itself and its per-connection negotiation, `NegotiateAsync` (`:317-373`, `:375-482`). Contrast
  `TcpTransportListener`, whose equivalent background pump (`HandshakePumpAsync`) is created **only when
  `tlsOptions` is configured** — a cleartext `TcpTransportListener` accepts inline in `AcceptAsync` itself,
  with no background task at all.
- **Why it bites:** the hardening shape the pump provides — the accept never gated on a negotiation
  slot, the zero-byte-read-before-slot-acquisition, the polled pending bound, the retry-with-delay on a
  transient accept failure — is otherwise a close match for `TcpTransportListener`'s TLS pump (see
  [transport.md](transport.md#the-negotiation-pump--read-this-before-touching-it) for the point-by-point
  comparison), so it is easy to assume the two listeners differ only in whether TLS is layered on top.
  They do not: `WebSocketTransportListener` always negotiates off-path because the HTTP upgrade parse has
  to happen there regardless of encryption, so `maxConcurrentHandshakes` bounds concurrent **plain HTTP
  header parsing** for a cleartext deployment just as much as it bounds concurrent TLS handshakes for a
  secured one. Someone porting "cleartext is basically free, the pump only matters with TLS" from the TCP
  listener to this one will size the knob wrong.
- **What to do:** size `maxConcurrentHandshakes` for the busiest configuration you actually run, cleartext
  or TLS — do not leave it at the default on the reasoning that "we don't use TLS here so it doesn't
  matter". If you need cleartext WebSocket connections to bypass the pump for latency, that is a design
  change to the listener, not a configuration tweak.

### KI-36 — Constructor's `path` doc claims `404` for a wrong upgrade path; the code always sends `400` (fixed)
- **Status: fixed before merge (PR #78).** The constructor's XML doc on the `path` parameter
  (`Transport/WebSocket/WebSocketTransportListener.cs:80-83`) wrongly claimed a mismatched path was
  refused with `404 Not Found`; the implementation has only ever sent `400 Bad Request`
  (`WriteResponseAsync(stream, "400 Bad Request", ...)`, `:422-428`) whenever `ReadUpgradeRequestAsync`
  returns a `null` key — there is no 404 anywhere in the code. The doc comment now says `400 Bad Request`
  and states plainly that a wrong path and a malformed/missing upgrade request are indistinguishable to
  the caller (`ReadUpgradeRequestAsync`, `:505-555`, returns `null` for both, with nothing to tell the two
  causes apart).
- **Why it mattered:** a genuine mismatch between the source's own documentation and its behaviour, not a
  wire-protocol or correctness defect — `WebSocketTransportLoopbackTests.ConnectAsync_WrongPath_ThrowsWebSocketException`
  only ever asserted that `ConnectAsync` throws, not which HTTP status produced it. An integrator who read
  the old doc and built something that expects `404` specifically for "wrong upgrade path" — a
  reverse-proxy routing rule, a health probe distinguishing "misconfigured client" from "server trouble"
  — would have seen `400` instead, indistinguishable from a malformed request.
- **What to do:** treat any non-`101` response from this listener as "rejected", full stop — do not build
  tooling that infers the cause from the status code. The behaviour (always `400`, no distinction) was
  kept as-is; only the doc was corrected to match it.

### KI-37 — A pipelined first WebSocket frame ahead of the `101` response has no dedicated regression test
- **Where:** the leftover-handling in `NegotiateAsync` (`Transport/WebSocket/WebSocketTransportListener.cs:432-439`);
  the buffered header reader that produces it, `ReadHeaderLinesAsync` (`:575-613`); the wrapper that
  serves it back, the private nested `LeftoverPrefixedStream` (`:658-739`).
- **Why it matters, not "bites":** `ReadHeaderLinesAsync` reads the HTTP upgrade request in 16 KiB-bounded
  chunks looking for the terminating blank line. A peer is not required by RFC 6455 to wait for the
  `101 Switching Protocols` response before sending its first WebSocket frame, so that frame's bytes can
  legitimately land in the same chunk as the tail of the header block. Whatever comes after the
  terminator in that chunk is not header data at all; `ReadHeaderLinesAsync` returns it as `leftover`
  rather than discarding it, and `NegotiateAsync` wraps the negotiated stream in `LeftoverPrefixedStream`
  whenever `leftover.Length > 0` so `SystemWebSocket.CreateFromStream` sees those bytes served back before
  anything further is read from the socket. On inspection this looks correct.
- **The gap:** *(inference)* no test in `WebSocketTransportLoopbackTests.cs` or elsewhere in the suite
  drives a client that writes a WebSocket frame immediately after its upgrade request without waiting for
  the `101` response — every test's `WebSocketTransport.ConnectAsync` call implicitly waits for
  `ClientWebSocket` to complete the handshake before the test sends anything. This path's correctness
  therefore rests on code inspection and RFC 6455 conformance, not on a regression test that would catch a
  future change silently dropping those bytes (which would manifest as an intermittently missing or
  truncated first message on a fast client, and only under a specific chunk-boundary timing — a very hard
  bug to reproduce after the fact).
- **What to do:** if you touch the header reader or the leftover plumbing, keep returning and re-serving
  those bytes rather than discarding them. If you want this properly regression-tested, the shape to add
  is a test that opens a raw `TcpClient` against the listener, writes the upgrade request **and** the
  first WebSocket frame's bytes back-to-back in one `Send` before reading the response, and asserts the
  frame is still received correctly.

### KI-38 — `UnixSocketTransport`/`NamedPipeTransport` bypass the per-remote-endpoint connection cap entirely
- **Where:** `MeshHub.AcceptLoopAsync`'s cap check (`MeshHub.cs:772-782`) and the predicate it runs,
  `ExtractRemoteAddress` (`MeshHub.cs:809-814`), which only recognises a transport implementing
  `IRemoteEndPointTransport` **and** reporting an `IPEndPoint`. Neither `UnixSocketTransport`
  (`Transport/Unix/UnixSocketTransport.cs:22`) nor `NamedPipeTransport`
  (`Transport/NamedPipes/NamedPipeTransport.cs:23`) implements `IRemoteEndPointTransport` at all — added
  by PR #81 (issue #20). **`QuicTransport` (PR #82, issue #21, not yet merged to `main`) is *not* in this
  bucket** — it implements `IRemoteEndPointTransport` and reports the real `QuicConnection.RemoteEndPoint`
  on both sides, so it participates in `maxConnectionsPerRemoteEndpoint` exactly as `TcpTransport` does;
  this was a deliberate correctness point confirmed during that PR's review specifically to avoid growing
  this entry's affected-transport list further. See [transport.md](transport.md#quictransport--transportquicquictransportcs33).
- **Severity:** high (availability / security). Not hypothetical: it is reachable from ordinary use of
  either new transport, not just an adversary.
- **Why it bites:** `maxConnectionsPerRemoteEndpoint` (KI-29) exists specifically to cap the
  pre-registration connection flood `maxClients` cannot see — but it is enforced only when
  `ExtractRemoteAddress` gets back a non-null `IPAddress`, which requires the accepted transport to
  implement `IRemoteEndPointTransport` and report an `IPEndPoint`. `UnixSocketTransport` and
  `NamedPipeTransport` report neither (there is no interface to query), so `ExtractRemoteAddress` returns
  `null` for every connection accepted over either transport, the cap check is skipped entirely
  (`MeshHub.cs:773`, `remoteAddress is not null && !TryReserveEndpointSlot(remoteAddress)` — the
  short-circuit means `TryReserveEndpointSlot` is never even called), and `TcpTransport`/
  `WebSocketTransport`'s per-source ceiling simply does not exist for these two transports. **A single
  local process (or, on a shared host, a single local account) with filesystem access to the Unix socket
  path or the named pipe can open connections up to the hub's full `MaxClients` budget** — the *only*
  remaining ceiling — rather than the intended `maxConnectionsPerRemoteEndpoint` (default 100) per
  source. On a hub also reachable over TCP/WebSocket, this can also be used to exhaust the client budget
  those remote peers would otherwise share, since `MaxClients` is one pool across every listener/transport
  a given `MeshHub` accepts from.
- **Why this was left unfixed rather than closed before merge (read before "fixing" it unilaterally):**
  closing this gap properly means changing `MeshHub.cs` itself — either widening
  `_connectionsByRemoteAddress`'s key type beyond `IPAddress` to admit some transport-agnostic notion of
  "connection identity" (a socket path, a pipe name, a process id — none of which `MeshHub` currently has
  a concept for), or inventing a parallel cap keyed some other way for non-`IPEndPoint` transports. Issue
  #20's own accepted design explicitly scopes the work as **"new transport only; hub/client
  untouched"** — extending `MeshHub.cs` was a deliberate non-goal of this PR, not an oversight the author
  missed. The gap was surfaced during review (a `pr-analyser-performance` pass) and recorded here rather
  than patched in a hurry inside an otherwise-scoped PR.
- **What to do:** if you deploy either new transport on a host where more than one untrusted local
  principal can reach the socket path/pipe name, **do not rely on `maxConnectionsPerRemoteEndpoint` for
  protection** — it does nothing for you. Either restrict filesystem/ACL access to the path so only a
  single trusted principal can reach it at all (which both listeners already hint at strongly — see
  their permission-hardening defaults in [transport.md](transport.md)), or lower `maxClients` to a value
  you are comfortable with a single untrusted local source claiming in full. A hub reachable over a mix
  of TCP/WebSocket and one of the new local transports still shares one `maxClients` pool, so a flood
  over the local transport can starve remote peers of capacity too.
- **What not to do:** do not add an ad hoc, transport-specific cap bolted onto `AcceptLoopAsync` as a
  quick fix without first deciding the general shape `MeshHub.cs` should take for a non-`IPEndPoint`
  connection identity — that is exactly the design question issue #20 deferred, and a narrow special case
  for these two transports would likely need redoing again for the next non-network transport.

### KI-39 — `UnixSocketTransportListener`'s `deleteExistingSocketFile` parameter silently also disables cleanup-on-dispose
- **Where:** the single `_deleteExistingSocketFile` field, set once from the constructor parameter of the
  same name (`Transport/Unix/UnixSocketTransportListener.cs:59-67`) and read in two places:
  `StartAsync`'s stale-file deletion before bind (`:84-87`) and `DisposeAsync`'s cleanup after teardown
  (`:167-170`, via `TryDeleteSocketFile`). Added by PR #81 (issue #20).
- **Severity:** low (usability / test coverage). Not a defect — the constructor's own XML doc states
  plainly that "the same file is also deleted on `DisposeAsync`" — but the parameter name and its
  placement (documented primarily as a *startup* concern: "the usual cause is a previous instance that
  exited without cleaning up") make the dispose-time effect easy to miss on a skim.
- **Why it bites:** a caller who passes `deleteExistingSocketFile: false` because they specifically do
  not want *pre-existing* files touched — for instance, wanting to fail loudly on "address already in
  use" rather than silently clobbering a file that might belong to something else — also, as a side
  effect, opts their **own** instance out of deleting its **own** socket file on a clean shutdown. That
  combination (fail-fast on collision, but still clean up after yourself) is not expressible with this
  constructor as it stands; the two behaviours share one flag.
- **Test coverage gap:** *(inference, confirmed by reading the test file)*
  `UnixSocketTransportListenerTests.cs` has no test constructing a listener with
  `deleteExistingSocketFile: false` at all — both the stale-file and the dispose-cleanup tests
  (`StartAsync_StaleSocketFileExists_DeletesItAndBindsSuccessfully`, `DisposeAsync_DeletesTheSocketFile`)
  exercise only the default (`true`) path. The `false` path's behaviour is exactly as the code implies,
  but nothing regression-tests it.
- **What to do:** if you need "leave a stale file alone at startup" and "delete my own file at shutdown"
  as independent choices, this constructor cannot express that today — you would need to add a second
  parameter rather than reusing this one, and should add the missing test coverage for whichever
  combination you rely on at the same time.
- **What not to do:** do not assume `deleteExistingSocketFile: false` only affects startup behaviour
  when reading or reviewing code that passes it — check both call sites in the source, since the
  constructor's own doc is the only place both effects are stated together.

### KI-40 — `QuicTransportListener`'s per-source negotiation cap mitigates, but does not eliminate, a many-source flood
- **Where:** `NegotiationPumpAsync` (`Transport/Quic/QuicTransportListener.cs:421-516`) — the global
  `negotiationSlots` semaphore (`maxConcurrentNegotiations`, default 64, `:428`) and the per-source cap
  layered in front of it (`TryAdmitSource`/`ReleaseSource`/`NormaliseForSourceCap`,
  `maxConcurrentNegotiationsPerSource`, default one eighth of `maxConcurrentNegotiations`, `:572-643`).
  Added by PR #82 (issue #21), **not yet merged to `main`**.
- **Severity:** medium (availability). Not hypothetical against a genuinely distributed source — it is
  the residual half of a gap the per-source cap was added specifically to narrow, not close.
- **Why it bites:** QUIC's `AcceptConnectionAsync` completes the full TLS 1.3 handshake internally
  before ever returning a connection, so — unlike `TcpTransportListener`'s handshake pump or
  `WebSocketTransportListener`'s negotiation pump, both of which gate admission on a cheap zero-byte-read
  pre-check that costs nothing and needs no slot — there is no way to tell a QUIC peer that will
  eventually open a stream apart from one that never will, before actually waiting for it. The per-source
  cap bounds how much of the global `maxConcurrentNegotiations` pool **one** source can occupy; it does
  **not** bound how much a flood spread across **many distinct sources** can occupy between them. A
  distributed flood of genuine (not spoofed — a spoofed source cannot complete the handshake at all)
  QUIC handshakes from `maxConcurrentNegotiations` or more distinct source addresses, each opening no
  stream, can still exhaust the global pool for `streamOpenTimeout` at a time, exactly as it could before
  the per-source cap existed. The cap changes the *shape* of the attack a single source can mount — from
  "hold the whole pool alone" to "hold at most one eighth of it" — not the existence of the underlying
  "no cheap pre-check" gap that makes the pool exhaustible at all.
- **Why this is recorded as a known limitation rather than fixed here:** closing it fully would need
  either a genuinely cheap admission signal QUIC does not offer (there is no equivalent of TCP's
  zero-byte pre-connect-handshake read once msquic has already completed the crypto), or an
  IP-reputation/rate-limiting layer outside what `QuicTransportListener` can reasonably own on its own —
  a materially larger design than a transport-level listener. The per-source cap is the mitigation that
  fits the transport's own scope; it was added specifically during a security-review pass on PR #82,
  which is itself evidence this trade-off was made deliberately, not missed.
- **What to do:** if you deploy `QuicTransportListener` on a network reachable by an adversary who can
  mount a distributed flood, size `maxConcurrentNegotiations` and `streamOpenTimeout` for that threat
  model specifically — a smaller `streamOpenTimeout` shortens how long a hostile connection can hold its
  slot, at the cost of being less forgiving to a genuinely slow legitimate peer. Do not rely on
  `maxConcurrentNegotiationsPerSource` alone as a defence against a distributed flood; it was never
  designed to be one.
- **What not to do:** do not read "the per-source cap was added during a security review" as "this gap is
  now closed" — the cap's own constructor XML doc and the pump's `<remarks>` are explicit that it
  addresses the single-source case only.

### KI-41 — `QuicTransportListener.StartAsync` is not safe under concurrent invocation (fixed)
- **Status: fixed, same PR (#82), commit `d4de3b3`.** Found by this documentation pass while reconciling
  the disposal-race handling already present in `StartAsync`; fixed immediately afterwards rather than
  shipped as an open item, since the fix pattern already exists in this same codebase
  (`MeshHub.StartAsync`'s `_starting` flag) and the change was small and low-risk.
- **Where it was:** `StartAsync` (`Transport/Quic/QuicTransportListener.cs`). The "already running" guard
  was checked under the lock **before** the asynchronous bind (`QuicListener.ListenAsync`); the only
  check made in the **second** lock block, after the bind completed, was `!_disposed` — there was no
  re-check that another `StartAsync` call had since published state.
- **Severity:** medium (correctness / resource leak). Not reachable through ordinary `MeshHub` usage —
  `MeshHub.StartAsync` calls `listener.StartAsync()` exactly once, itself guarded by the hub's own
  lifecycle lock (KI-23) — but reachable from any caller that invokes `QuicTransportListener.StartAsync`
  more than once without awaiting the first call first, which nothing on this type prevents or documents
  against.
- **Why it bites:** every other listener in this codebase (`TcpTransportListener`,
  `WebSocketTransportListener`, `UnixSocketTransportListener`, `NamedPipeTransportListener`) binds
  synchronously, so its entire bind runs *inside* `_stateLock` and a second concurrent `StartAsync` call
  simply cannot observe the "not yet running" state the first call already claimed — the lock serialises
  them completely, and the second reliably throws `InvalidOperationException`. `QuicTransportListener`
  cannot do this, because `QuicListener.ListenAsync` is itself the asynchronous bind — there is no
  synchronous constructor step to lock around, as the type's own doc comments acknowledge (`:239-243`,
  "a concurrent `DisposeAsync` is handled by rechecking the `_disposed` flag under lock once
  `ListenAsync`'s await completes"). That handles the **`StartAsync`-vs-`DisposeAsync`** race — it is
  exactly what `DisposeAsync_RacedAgainstStartAsync_NeverLeavesAnUnpublishedListenerRunning`
  (`QuicTransportListenerTests.cs:286`) proves — but nothing handles the **`StartAsync`-vs-`StartAsync`**
  race: two overlapping calls both pass the initial guard (neither has published `_negotiationCts` yet
  when the other checks it), both genuinely call `QuicListener.ListenAsync` and successfully bind (two
  real binds — if the endpoint uses an ephemeral port, `0`, both succeed on two different ports; if it
  names a fixed port, one bind will fail with a `QuicException`/`SocketException` the caller sees
  directly, which happens to mask the race on that path only), and both reach the second lock block.
  Whichever finishes second **silently overwrites** `_listener`, `_negotiatedTransports`,
  `_negotiationCts` and `_negotiationPumpTask` with its own instances — there is no check for "somebody
  else already published". The loser's `QuicListener` is never disposed by anything: its background
  `NegotiationPumpAsync` keeps running, keeps accepting real connections, and keeps writing negotiated
  transports into a `Channel` that only the *orphaned* pump's own closure still references — nothing will
  ever call `AcceptAsync` against it, because the listener's `_negotiatedTransports` field now points at
  the winner's channel instead. The result is a leaked bound UDP socket and a background task that runs
  forever (or until the channel fills and its writes start blocking), invisible to `DisposeAsync`, which
  only ever tears down whatever the fields currently reference.
- **What was tested and what wasn't:** `StartAsync_AlreadyRunning_ThrowsInvalidOperationException`
  (`QuicTransportListenerTests.cs:82`) only exercises the **sequential** case — it fully `await`s the
  first `StartAsync` before issuing the second, which the existing guard handles correctly. Nothing in
  `QuicTransportListenerTests.cs` dispatches two `StartAsync` calls concurrently and releases them
  together the way `AcceptAsync_RacedAgainstDispose_OnlyEverReportsDisposal`
  (`QuicTransportListenerTests.cs:230`) does for the accept/dispose race — that shape exists in the file
  for exactly this kind of race and was not applied to this one.
- **Why this was missed by the PR's earlier hardening passes:** the security-review and performance-review
  passes recorded in this branch's history (KI-40's per-source cap, the non-blocking shed) both targeted
  the negotiation pump's handling of *many connections*, not the listener's own `StartAsync` entry point.
  This gap sat one level up, in code the other hardening passes had no reason to look at.
- **What was fixed:** a `_starting` flag, checked and claimed under `_stateLock` alongside the existing
  "already running" check, exactly mirroring `MeshHub.StartAsync`'s pattern (`MeshHub.cs:464`, described
  in [known-issues.md](known-issues.md) KI-23's fifth point). A second concurrent `StartAsync` now sees
  `_starting` already `true` and throws `InvalidOperationException` immediately, before ever calling
  `QuicListener.ListenAsync` — so the double-bind this entry describes can no longer happen. The flag is
  released (in a `catch`) if `QuicListener.IsSupported` is `false` or `ListenAsync` itself throws, so a
  listener that failed to start remains startable rather than permanently reporting itself as running.
  `StartAsync_CalledConcurrently_OnlyOneSucceeds` (`QuicTransportListenerTests.cs`) dispatches two
  `StartAsync` calls to separate threads, releases them together, asserts exactly one succeeds, and then
  proves the winner's state was genuinely published by connecting and exchanging a payload through the
  listener afterwards — closing the test gap this entry originally recorded.
- **What not to do:** do not treat `StartAsync_AlreadyRunning_ThrowsInvalidOperationException` alone as
  proof this is safe — it only ever exercised the sequential case; the race coverage above is what closes
  the gap it left open.

### KI-42 — `SendAsync`'s headers overload now rejects six specific header keys — **EXTENDED (PR #84, PR #85)**
- **Where:** `MeshClient.SendAsync(Guid, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken)`
  (`MeshClient.cs:377-387`) calls `ThrowIfReservedHeaderKeyPresent(headers)` at `:384` (method at
  `:543-555`, guarding the `ReservedHeaderKeys` array at `:527-535`) before doing anything else with the
  caller's `headers`.
- **Severity:** low (usability / backward compatibility). Introduced by PR #83 for two keys, extended by
  PR #84 to five, extended again by PR #85 to six. PR #83 added the request/response helper
  (`RequestAsync`/`ReplyAsync`, see [client.md](client.md#request-response)) and needed two header keys —
  `"mesh.request-id"` and `"mesh.reply"` (`Messages/RequestReplyHeaderKeys.cs`) — reserved so an
  application header could never be silently swallowed by the receive loop's reply matcher (KI-43 below).
  PR #84 added delivery acknowledgement (see [client.md](client.md#delivery-acknowledgement)) and needed
  three more — `"mesh.ack-id"`, `"mesh.ack-request"` and `"mesh.ack"`
  (`Messages/DeliveryAcknowledgementHeaderKeys.cs`) — for the identical reason (KI-46 below). PR #85 added
  message time-to-live (see [client.md](client.md#message-expiry-time-to-live)) and needed one more —
  `"mesh.expires-at"` (`Messages/MessageExpiryHeaderKeys.cs`) — so an application header could not be
  silently mistaken for a real expiry value by the receive loop's expiry check.
- **Why it bites:** before PR #83, `MessageHeaders` was an open, uninterpreted-by-the-client namespace —
  an application could use any string as a key, and `SendAsync` would send it unremarked. Since PR #83
  (two keys), PR #84 (three more) and PR #85 (one more), doing so with any of these six exact strings
  throws `ArgumentException`. Any application that happened to use one of them as its own header key
  before the relevant PR shipped will see `SendAsync` start failing where it previously succeeded — a
  genuine (if narrow-probability) behavioural break introduced without a protocol version bump each time,
  because it is enforced entirely client-side and has no wire-format consequence at all.
- **What to do:** if you hit this, rename your header key. Do not work around it by constructing the
  frame yourself to bypass `SendAsync` — see KI-43/KI-46 for why that produces a worse outcome on the
  receiving end than a thrown exception on the sending end.
- **What not to do:** do not widen the guard to reject a larger namespace "for safety" — it deliberately
  matches only the six keys `RequestReplyHeaderKeys`/`DeliveryAcknowledgementHeaderKeys`/
  `MessageExpiryHeaderKeys` actually define, so it does not surprise an application using an unrelated
  `"mesh.*"`-prefixed key of its own.

### KI-43 — Request/response is a client-side convention the hub cannot see or protect
- **Where:** `MeshClient.TryCompletePendingRequest` (`:1439-1484`), called from the receive loop's
  `DeliverMessageWithHeaders` branch (`:1097`, second of three nested checks since PR #85 — `:1096` is
  `TryCompletePendingAck`, `:1098` is the PR #85 expiry check, see KI-46 and KI-47) **before**
  `MessageReceived` is ever raised for that frame.
- **Severity:** low (correctness), by design. The scenario below requires either a peer not built on this
  library, or application code that deliberately bypasses `SendAsync` to hand-build a frame — neither is
  something an ordinary consumer of `MeshClient` can do by accident through the public API (KI-42 already
  stops that route).
- **Why it bites:** `SendMessageWithHeaders`/`DeliverMessageWithHeaders` (`0x11`/`0x12`) are ordinary,
  hub-routed direct-message opcodes; the hub never decodes header *content*, only the header block's
  *length*, on this or any other path (see [protocol.md](protocol.md#message-headers)). Request/response
  correlation, matching, and the sender-identity check that stops a hostile peer from forging a reply to
  someone else's request (see [client.md](client.md#request-response)) are entirely enforced by the two
  `MeshClient` instances involved — never by the hub. Consequently, **any** frame that reaches a
  `MeshClient`'s receive loop carrying header `mesh.reply=1` is intercepted by `TryCompletePendingRequest`
  and **never** raised through `MessageReceived` — regardless of whether it matches a real pending
  request, and regardless of whether the sender is a `MeshClient` that actually called `RequestAsync` in
  the first place. A non-`MeshClient` peer, a differently-versioned client library, or application code
  that constructs a `SendMessageWithHeaders` frame outside `SendAsync` and happens to set that header for
  its own unrelated reasons would have that message silently dropped by every receiving `MeshClient`, with
  only a `Debug`/`Warning`-level log (KI-43 shares its symptom with KI-9: a framing-adjacent bug here also
  manifests as "the message never arrives"). **PR #84 introduced the identical pattern a second time** for
  `mesh.ack` — see KI-46, which is this entry's acknowledgement mirror rather than a duplicate.
- **What to do:** never construct a `MessageHeaders` containing `RequestReplyHeaderKeys.Reply` (`"mesh.
  reply"`) outside `ReplyAsync` itself. If you are implementing a Meshworx-compatible peer in another
  language or library, treat these two header keys as reserved wire vocabulary even though the hub itself
  enforces nothing about them.
- **What not to do:** do not read `SendAsync`'s guard (KI-42) as a complete solution to this — it only
  protects callers going through this library's own public API on the *sending* side. It cannot protect
  the *receiving* side against a peer that was never subject to it.

### KI-44 — Delivery acknowledgement means "handed to the application", not "the handler succeeded"
- **Where:** `MeshClient.ReceiveLoopAsync`'s `DeliverMessageWithHeaders` branch, `MeshClient.cs:1100-1126`.
  The acknowledgement fire-and-forget dispatch (`:1117-1126`) sits immediately **after** the
  `MessageReceived?.Invoke(...)` call (`:1100-1115`), outside its `try/catch`.
- **Severity:** low (correctness), by design.
- **Why it bites:** `TrySendAcknowledgementAsync` is called once `MessageReceived` has been raised for the
  incoming message — the code does not distinguish a subscriber that processed the message from one that
  threw (the throw is already caught and logged one line earlier, per the callback-boundary convention).
  A caller reading `SendAsync(..., DeliveryOptions.RequireAck(...), ...)`'s successful completion as "the
  recipient's application processed this message" is reading more into it than the guarantee actually
  gives: it only means the frame reached `MessageReceived` and a `DeliverMessageWithHeaders`-branch
  acknowledgement was sent back. A throwing, no-op, or logically-failing handler still results in the
  sender's `RequireAck` call completing successfully.
- **What to do:** if the caller needs proof the recipient's application *logic* succeeded — not merely
  that the frame arrived — that is a different, stronger guarantee than this feature provides. Build an
  explicit application-level reply (e.g. `RequestAsync`/`ReplyAsync`, see KI-43) carrying a
  success/failure result, rather than reading `RequireAck`'s completion as one.
- **What not to do:** do not "fix" this by moving the acknowledgement dispatch inside the
  `MessageReceived` `try` block so it only fires on success — that would change the acknowledgement's
  meaning to "the handler didn't throw", which is still not "succeeded" for a handler that swallows its
  own errors, and would newly conflate a transport-level delivery signal with an application outcome. If
  you need the stronger guarantee, build it explicitly (see above) rather than overloading this one.

### KI-45 — A `RequireAck` timeout does not prove the message was not delivered
- **Where:** `MeshClient.SendAsync(..., DeliveryOptions, ...)`, `MeshClient.cs:390-443`;
  `TrySendAcknowledgementAsync`, `:1549-1596`.
- **Severity:** low (correctness), by design.
- **Why it bites:** the acknowledgement itself is an ordinary routed message, sent back through the same
  hub and subject to every silent-drop path any other message is — a full outbound queue on the hub
  (KI-1), an unknown/since-disconnected recipient becoming the sender's target (KI-4, though here it is
  the *acknowledger's* send that would be affected), or the connection dropping between the original
  message being delivered and the acknowledgement being dispatched. `TrySendAcknowledgementAsync` swallows
  every failure it can observe (`:1584-1596`) and logs at `Debug` — the sender simply never receives an
  acknowledgement and times out, with no way to distinguish "the message never arrived" from "the message
  arrived but the acknowledgement was lost".
- **What to do:** treat a `TimeoutException` from `RequireAck` as "no acknowledgement was confirmed", not
  as authoritative proof of non-delivery. If your application cannot tolerate that ambiguity, an
  idempotent operation plus a retry is the standard way to make an at-least-once guarantee usable; this
  feature does not provide exactly-once or delivery-with-certainty semantics.
- **What not to do:** do not build retry-on-timeout logic that assumes the original send never arrived —
  a slow acknowledgement path can still complete after the caller has already given up and retried,
  producing a duplicate delivery the application must be prepared to handle.

### KI-46 — Delivery acknowledgement is also a client-side convention the hub cannot see or protect
- **Where:** `MeshClient.TryCompletePendingAck` (`:1502-1547`), called from the receive loop's
  `DeliverMessageWithHeaders` branch (`:1096`, first of three nested checks since PR #85 — see KI-43 and
  KI-47) **before** `MessageReceived` is ever raised for that frame.
- **Severity:** low (correctness), by design. The acknowledgement mirror of KI-43, added by PR #84; the
  reasoning is identical, substituting `mesh.ack`/`TryCompletePendingAck`/`_pendingAcks` for
  `mesh.reply`/`TryCompletePendingRequest`/`_pendingRequests` throughout.
- **Why it bites:** the hub never decodes header *content* on the direct-message path — only the header
  block's *length* — so acknowledgement correlation, matching, and the sender-identity check that stops a
  hostile peer from forging an acknowledgement to someone else's message (see
  [client.md](client.md#delivery-acknowledgement)) are entirely enforced by the two `MeshClient` instances
  involved. **Any** frame that reaches a `MeshClient`'s receive loop carrying header `mesh.ack=1` is
  intercepted by `TryCompletePendingAck` and **never** raised through `MessageReceived` — regardless of
  whether it matches a still-pending `RequireAck` call, and regardless of whether the sender is a
  `MeshClient` that actually received a message requesting one. A non-`MeshClient` peer, or application
  code that hand-builds a `SendMessageWithHeaders` frame outside `SendAsync` and happens to set that
  header, would have that message silently dropped by every receiving `MeshClient`.
- **What to do:** never construct a `MessageHeaders` containing
  `DeliveryAcknowledgementHeaderKeys.Ack` (`"mesh.ack"`) outside the automatic acknowledgement dispatch
  itself. If you are implementing a Meshworx-compatible peer, treat all three delivery-acknowledgement
  header keys as reserved wire vocabulary even though the hub itself enforces nothing about them.
- **What not to do:** do not read `SendAsync`'s guard (KI-42) as a complete solution to this — it only
  protects callers going through this library's own public API on the *sending* side, exactly as noted for
  KI-43.

### KI-47 — Message time-to-live is measured against the sender's clock, with no hub clock authority
- **Where:** `MeshClient.SendAsync(Guid, ReadOnlyMemory<byte>, TimeSpan, CancellationToken)`
  (`MeshClient.cs:446-466`) computes the absolute expiry from `DateTimeOffset.UtcNow` at the moment of the
  call (`:457`); `MeshClient.IsExpired` (`:1407-1422`) and `MeshHub.IsExpiredFrame` (`MeshHub.cs:1638-1668`)
  each independently compare that value against **their own** `DateTimeOffset.UtcNow` when they check it;
  see `Messages/MessageExpiryHeaderKeys.cs` for the shared parsing helper both call through.
- **Severity:** medium (correctness). By design — there is no protocol mechanism for clock synchronisation
  anywhere in Meshworx, and adding one was explicitly out of scope for PR #85 (issue #29) — but real,
  because the feature's entire purpose (dropping a message once it is "too old to be useful") only behaves
  as a caller expects if the three clocks involved (sender, hub, recipient) agree closely enough for the
  chosen `timeToLive` to be meaningful.
- **Why it bites:** a sender whose system clock runs fast computes an expiry instant that is, in absolute
  terms, later than the sender intended relative to true wall-clock time — messages survive longer than
  the caller's mental model of "expires in `timeToLive`" suggests. A sender whose clock runs slow produces
  the opposite: messages that the hub or recipient consider expired well before the `timeToLive` the
  caller specified has "really" elapsed. Because the hub and the recipient each compare independently
  against their *own* clock (not against each other's, and not against the sender's), the hub and the
  recipient can also disagree with each other about whether a given message has expired if their own
  clocks have drifted apart — a message could be dropped at the hub but would have been accepted by the
  recipient, or vice versa, purely as a function of which machine's clock is more accurate at that moment.
  This is most acute for a short `timeToLive` (seconds), where even a few hundred milliseconds of skew is a
  meaningful fraction of the window; a `timeToLive` of minutes or hours is comparatively insensitive to
  ordinary NTP-class drift.
- **What to do:** run NTP (or an equivalent clock-sync service) on every machine hosting a `MeshClient` or
  `MeshHub` that uses this feature, and do not choose a `timeToLive` shorter than the clock-skew budget you
  are actually willing to tolerate across your fleet. Treat "the message expired" and "the message did not
  expire" as both being approximate, not exact, cutoffs.
- **What not to do:** do not assume a hub-side or recipient-side drop proves the message was "genuinely"
  older than `timeToLive` by the sender's clock, and do not build logic that depends on the hub and the
  recipient agreeing on whether a specific borderline message has expired — they are not guaranteed to.

### KI-48 — `DeliveryOptions.AwaitCapacity` parks the sender's whole connection, not just the one message
- **Where:** `TryAwaitCapacityAsync` (`MeshHub.cs:1587-1617`) is awaited from
  `RouteMessageWithHeaders` (`MeshHub.cs:1945-1956`) before that method returns to
  `HandleClientAsync`'s dispatch loop.
- **Severity:** medium (performance / availability). By design — added by PR #87 (issue #30) as the
  documented trade-off of opting into `DeliveryOptions.AwaitCapacity` — but easy to miss because nothing
  in the caller's own code looks like it should block anything beyond the one send.
- **Why it bites:** frames from a single connection are read and routed strictly in order. While
  `RouteMessageWithHeaders` awaits room on one saturated recipient's queue, the sending client's receive
  loop is parked and reads nothing else — so every other message that same sender addresses to **any
  other recipient**, however healthy, queues up behind the one being awaited, for up to
  `backpressureAwaitTimeout` (default 30 s, `Timeout.InfiniteTimeSpan` to wait forever — which the
  constructor logs a warning for, see [hub.md](hub.md#backpressure-signalling-and-awaiting-capacity)).
  This is head-of-line blocking at the **sender's** connection, not the hub or the router as a whole:
  other clients' traffic is unaffected, but a chatty sender using `AwaitCapacity` against one slow
  recipient can stall its own unrelated traffic for the full timeout.
- **What to do:** reserve `AwaitCapacity` for the specific message that must not be lost, not as a
  blanket default on every send from a connection that also sends latency-sensitive traffic elsewhere.
  If a sender genuinely needs independent delivery guarantees per recipient, consider separate
  `MeshClient` connections rather than relying on one connection's ordering to interleave fairly under
  backpressure — it will not.

### KI-49 — `RequireAck` + `AwaitCapacity` time out independently — an ack timeout can fail a send the hub still delivers
- **Where:** `DeliveryOptions.WithAwaitCapacity` (`DeliveryOptions.cs:91-108`, remarks) and the
  `SendAsync(Guid, ReadOnlyMemory<byte>, DeliveryOptions, CancellationToken)` overload
  (`MeshClient.cs:393-464`), specifically the `AcknowledgementTimeout` wait at `:443-455`.
- **Severity:** medium (correctness). By design — added by PR #87 (issue #30) as a documented
  consequence of combining two features that were each independently designed (PR #84's
  `RequireAck`, PR #87's `AwaitCapacity`) — but a real trap for a caller that reads only one of the two
  features' own documentation.
- **Why it bites:** `DeliveryOptions.WithAwaitCapacity()` lets a single send both require an
  acknowledgement and await capacity, but the two waits are timed by different parties and are not
  reconciled automatically. `AcknowledgementTimeout` is measured by the sending `MeshClient`; the
  capacity wait is bounded by the **hub's own** `backpressureAwaitTimeout` (default 30 s). If the
  acknowledgement timeout is shorter, `SendAsync` throws `TimeoutException` while the hub may still be
  waiting for room — and if room then frees up, the hub queues and delivers the message anyway, after
  the caller has already been told it failed. A caller that follows the `RequireAck`-timeout-means-retry
  pattern `RequireAck` exists to support would then send the message a second time, producing a
  duplicate the recipient's application sees twice.
- **What to do:** when combining `RequireAck` with `AwaitCapacity`, set the acknowledgement timeout
  comfortably **longer** than the hub's `backpressureAwaitTimeout` — the reverse of the ordering a
  caller might reach for instinctively. If you do not control the hub's `backpressureAwaitTimeout`
  (e.g. it is a library you don't own), treat a `TimeoutException` from this combination as *not* proof
  of non-delivery — the same caveat KI-45 already makes for `RequireAck` alone — and design retries to
  be idempotent rather than relying on the timeout to mean "definitely not delivered".

### KI-50 — A held message's recipient id stops resolving as soon as its name reconnects, so store-and-forward only bridges the gap once — **MITIGATED (issue #43)**
- **Where:** `MeshHub.TryStoreForOfflineDeliveryAsync` / `ForgetOfflineIdentity`, called from
  `HandleClientAsync`'s registration path and from the unknown-recipient branch of both direct-routing
  methods.
- **Severity:** medium (correctness, by design). Issue #28 keys the store by client **name**, but
  senders address by the per-connection `Guid` they looked up, and a reconnecting client is minted a
  brand new one.
- **Why it bites:** while the recipient is away, a peer's cached id resolves to its name and messages
  are held. The moment that name registers again — under an id the peer does not know — the hub forgets
  the old id, and every further message the peer sends to it is dropped as `unknown-recipient` rather
  than held. So a peer that never re-runs `GetClientIdByNameAsync` gets its traffic bridged across
  exactly one absence and then silently stops reaching the recipient, even though the recipient is
  connected. There is no signal to the sender that its id has gone stale.
- **What to do:** **turn session resumption on** (`sessionResumptionWindow`) — issue #43 landed exactly
  for this. A resumed client keeps its `Guid`, so a peer's cached id stays correct across the reconnect
  and the "held once, then dropped" cliff never arrives. It is not a total fix: resumption is opt-in,
  needs protocol version 6 at both ends, and lapses once the window closes, and in any of those cases
  the cliff is still there. Where it applies, also treat a looked-up id as valid only while the peer
  stays connected — re-resolve on the hub's `ClientDisconnected`, on a `SendRejected`, or before each
  send if the cost is acceptable.

### KI-51 — The offline store runs on a live connection's path, and a slow one is felt by clients
- **Where:** `MeshHub.TryStoreForOfflineDeliveryAsync` (called from the sending client's receive loop)
  and `MeshHub.DeliverStoredMessagesAsync` (called during the returning client's registration), both
  bounded by `offlineStoreTimeout` (default 10 s).
- **Severity:** low–medium (availability), only for a hub with a custom, non-in-memory store.
- **Why it bites:** this is the same hazard class as a slow `GroupAuthoriser` (KI-28). A durable store
  that does I/O adds that latency to the *sender's* receive loop for every message it holds — and,
  because one connection's frames are routed in order, everything else that sender is sending queues
  behind it. On the drain side it delays the returning client's transition into its receive loop.
  Unlike the group authoriser, there is no concurrency cap: every connection may have one store call in
  flight at once, so a store must expect `maxClients`-way concurrency.
- **What to do:** keep the implementation fast and bound your own concurrency inside it. Note the
  timeout bounds how long the *hub waits*, not how long your call *runs* — an abandoned call carries on
  executing. `InMemoryOfflineStore` never blocks, so a hub using the default is unaffected.

### KI-52 — A resumption token is a bearer credential for an identity, and the name behind it is only as strong as your authenticator
- **Where:** `MeshHub.IssueSessionToken` / `ResumeSessionAsync`, and the token appended to
  `RegistrationComplete` (issue #43).
- **Severity:** medium (security, by design), and only for a hub with `sessionResumptionWindow` set.
- **Why it bites:** anyone holding the token can reclaim the `Guid` and group memberships of the name it
  was issued to. Four things bound that, and it is worth knowing exactly what each does *not* cover:
  - It is **32 bytes of `RandomNumberGenerator` output**, so guessing is not an attack.
  - Only its **SHA-256 hash** is retained by the hub, so the session table is not a bag of live secrets —
    but the token itself is on the wire once, in the registration reply, in the clear on a cleartext
    transport. **Use TLS if the network is not trusted** — the same advice the credential your
    `ClientAuthenticator` checks already needs.
  - It is **single-use** — a successful resume issues a fresh token and invalidates the old — so a token
    captured off the wire cannot be replayed *after* its owner has next reconnected. It can be replayed
    before that.
  - It only reclaims **its own name's** session. But on a hub with no `ClientAuthenticator` the name is
    self-asserted (KI-2), so an attacker holding a token can simply register under that name and then
    present it. **Session resumption does not add an authentication boundary and must not be read as
    one**; it preserves an identity, it does not prove one.
- **What to do:** treat the token exactly as you treat the registration credential. Use an encrypted
  transport, configure a `ClientAuthenticator` if the name is meant to mean anything, and keep the
  window as short as your reconnect behaviour tolerates — it bounds how long a stolen token is worth
  anything.

### KI-53 — A resumed client's restored groups are re-authorised, which can double a mass-reconnect's authoriser load
- **Where:** `MeshHub.RestoreGroupMembershipAsync` (issue #43), and `MeshClientReconnector`'s own group
  restoration.
- **Severity:** low (performance), only with a `GroupAuthoriser` configured.
- **Why it bites:** resumption restores membership by asking the authoriser for each group (correctly —
  see [hub.md](hub.md#session-resumption) for why it must). `MeshClientReconnector` then re-joins the
  groups it snapshotted before the drop, which asks the authoriser *again* for the same groups. The
  result is correct — joins are idempotent and each decision is honoured — but a mass reconnect of
  resuming clients drives roughly twice the authoriser calls it used to. KI-28's advice about bounding
  your own concurrency inside the callback applies with double the force.
- **What to do:** nothing is broken, but if authoriser load matters, either pass
  `restoreGroupMembership: false` to `MeshClientReconnector` when the hub has resumption enabled and let
  the hub's restore do the work, or make the authoriser cheap for a repeat decision on the same
  (client, group) pair.

---

## Also worth knowing (not defects)

- **Broadcasts are indistinguishable from direct messages at the recipient** — both arrive as
  `DeliverMessage` → `MessageReceived` (`MeshHub.cs:2026`). If you need to tell them apart, encode it in
  your payload.
- **Two `.slnx` files** (root `Meshworx.slnx` vs `src/Meshworx.slnx`). CI and "done" use the root one.
- **`RegistrationRefusedException`'s extra ctors** (message / message+inner / default) exist only to
  satisfy analyser `CA1032`; the meaningful one is `RegistrationRefusedException(RegistrationErrorCode)`.
- **No `TODO`/`FIXME`/`HACK` markers** were found in the `main` library source — the comments are
  explanatory, not debt markers.
