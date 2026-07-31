# Wire protocol & message framing

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [transport.md](transport.md)

This is the contract on the wire. Get it wrong and frames are silently dropped rather than rejected, so
document/verify carefully when you touch it. Everything here is read from
`src/AdamSalisbury.Meshworx/Messages/MessageType.cs`, `Messages/Protocol.cs`, and the encode/decode
sites in `MeshHub.cs` / `MeshClient.cs`.

- **Protocol version is a negotiated range** (`Protocol.MinSupportedVersion` = `4`,
  `Protocol.MaxSupportedVersion` = `10`, `Messages/Protocol.cs:8`, `:14`). The client advertises the range
  it can speak; the hub picks the highest version common to both sides — see
  [Registration handshake](#registration-handshake). Negotiation itself was introduced by PR #73
  (issue #47); PR #74 (issue #32) is the **first thing to actually widen the range and branch on the
  result** — it raised `MaxSupportedVersion` from `4` to `5` to gate the structured message-header
  envelope (`Protocol.HeaderEnvelopeMinVersion = 5`, `Messages/Protocol.cs:21`): a connection negotiated
  at `5` can carry headers, one negotiated at `4` cannot, and both the client (refusing to send headers
  a peer wouldn't understand) and the hub (choosing the outgoing frame shape per recipient) now read
  `NegotiatedProtocolVersion` to decide. See [Message headers](#message-headers),
  [Versioning](#versioning) and [known-issues.md](known-issues.md) KI-14 (now resolved).
  `GroupJoinRefused` (`0x10`) was added **within** version 3, before negotiation existed at all, and did
  **not** bump the version — see [Additive opcodes](#additive-opcodes-within-a-version) for why that is
  sound and when it is not (the four header-bearing opcodes below did **not** qualify for that route and
  bumped the version instead — see the note after the opcode table). `QueueSaturated` (`0x15`, PR #87,
  issue #30) is a second opcode added the same way, within version 5, no bump needed. **Issue #43 is the
  second widening**, raising `MaxSupportedVersion` from `5` to `6` to gate session resumption
  (`Protocol.SessionResumptionMinVersion = 6`): one of its three opcodes travels client → hub, which
  rules out the additive route — see [Session resumption](#session-resumption) below. **PR #135/issue
  #109 is the third widening**, raising `MaxSupportedVersion` from `6` to `7`
  (`Protocol.SessionResumedGroupsMinVersion = 7`) so a `SessionResumed` reply can report which group
  memberships the hub actually restored — see [Session resumption](#session-resumption) below for the
  wire layout this adds. **Commit `fb2f9a0` is the fourth widening**, raising `MaxSupportedVersion` from
  `7` to `8` and adding `Protocol.TopicPubSubMinVersion = 8` — this closes a gap topic pub/sub (issue #37)
  shipped with one commit earlier: its four client → hub opcodes
  (`SubscribeTopic`/`UnsubscribeTopic`/`PublishTopicMessage`/`PublishTopicMessageWithHeaders`) were
  reachable at any negotiated version for that one commit, breaking this file's own additive-opcode rule —
  see [Topic pub/sub frames](#topic-pubsub-frames-issue-37) below and
  [known-issues.md](known-issues.md) KI-61 (now fixed). **Issue #38 is the fifth widening**, raising
  `MaxSupportedVersion` from `8` to `9` and adding `Protocol.ClientAttributesMinVersion = 9` to gate
  `SetClientAttributes`/`FindClientsRequest` — this time the gate shipped with the feature itself from the
  start, rather than needing a follow-up fix — see
  [Client attribute frames](#client-attribute-frames-issue-38) below. **Issue #39 is the sixth widening**,
  raising `MaxSupportedVersion` from `9` to `10` and adding `Protocol.PresenceMinVersion = 10` to gate
  `SubscribePresence`/`UnsubscribePresence` — again gated from the outset — see
  [Presence frames](#presence-frames-issue-39) below.
- **`MessageType` and `Protocol` are `internal`** — opcodes are not visible outside the assembly.
- **Byte order:** big-endian for all multi-byte integers (`BinaryPrimitives.*BigEndian`). Ids are
  16-byte `Guid`s written with `Guid.TryWriteBytes` / read with `new Guid(span)`.

---

## Two layers: framing vs message

1. **Transport framing** (owned by the transport). For `TcpTransport`: `[4-byte big-endian length N][N
   payload bytes]`, `N ≤ 1 MiB`. `UnixSocketTransport`/`NamedPipeTransport` (PR #81, issue #20) and, since
   PR #82 (issue #21), `QuicTransport` frame **identically** — all three
   (plus TCP) wrap a stream-oriented channel (a socket stream, a pipe stream, or a single QUIC stream)
   exactly the same way, and all four now share one internal `StreamFramer` helper
   (`Transport/Framing/StreamFramer.cs`) rather than each reimplementing the length prefix and its bounds
   checking. `WebSocketTransport` (PR #78, issue #18) is the exception and needs no length prefix at all —
   one WebSocket binary message carries exactly one frame, since the WebSocket protocol already delimits
   messages; it enforces the identical 1 MiB cap independently, on both send and receive, via its own
   separate constant. `InMemoryTransport` likewise uses channel boundaries — no length prefix. Whatever
   the transport, the hub/client never see its framing; they receive one **message payload** per
   `ReceiveAsync`, and everything below this line is that payload, unchanged by which transport carried
   it. See [transport.md](transport.md) for each transport's framing in full.
2. **Message** (owned by hub/client). The **first payload byte is the opcode** (`MessageType`); the rest
   is opcode-specific. Empty frames (length 0) are ignored, not decoded (`MeshHub.cs:1075`,
   `MeshClient.cs:1051` — re-pointed this pass for PR #87's shift).

Everything in the tables below is the **message payload** (i.e. after the transport's framing header).

---

## Opcodes (`MessageType`, `Messages/MessageType.cs`)

| Name | Byte | Direction | Payload after the opcode byte |
|---|---|---|---|
| `RegistrationComplete` | `0x01` | hub → client | assigned client id (16), negotiated protocol version (1), and — version 6+ with resumption enabled only — token length (2, BE) and the resumption token |
| `SendMessage` | `0x02` | client → hub | recipient id (16), message bytes |
| `DeliverMessage` | `0x03` | hub → client | sender id (16), message bytes |
| `RegistrationRequest` | `0x04` | client → hub | version min (1), version max (1), name length (2, BE), UTF-8 name, opaque credential (rest of frame) |
| `Error` | `0x05` | hub → client | registration error code (1) |
| `ClientLookupRequest` | `0x06` | client → hub | correlation id (4, BE), UTF-8 name |
| `ClientLookupResponse` | `0x07` | hub → client | correlation id (4), found flag (1), id (16 if found) |
| `Disconnect` | `0x08` | either | none |
| `Ping` | `0x09` | hub → client | none |
| `Pong` | `0x0A` | client → hub | none |
| `BroadcastMessage` | `0x0B` | client → hub | message bytes |
| `JoinGroup` | `0x0C` | client → hub | UTF-8 group name (rest of frame) |
| `LeaveGroup` | `0x0D` | client → hub | UTF-8 group name (rest of frame) |
| `GroupMessage` | `0x0E` | client → hub | name length (2, BE), UTF-8 group name, message bytes |
| `DeliverGroupMessage` | `0x0F` | hub → client | sender id (16), name length (2, BE), UTF-8 group name, message bytes |
| `GroupJoinRefused` | `0x10` | hub → client | UTF-8 group name (rest of frame) |
| `SendMessageWithHeaders` | `0x11` | client → hub | recipient id (16), header-block length (2, BE), header block, message bytes |
| `DeliverMessageWithHeaders` | `0x12` | hub → client | sender id (16), header-block length (2, BE), header block, message bytes |
| `GroupMessageWithHeaders` | `0x13` | client → hub | name length (2, BE), UTF-8 group name, header-block length (2, BE), header block, message bytes |
| `DeliverGroupMessageWithHeaders` | `0x14` | hub → client | sender id (16), name length (2, BE), UTF-8 group name, header-block length (2, BE), header block, message bytes |
| `QueueSaturated` | `0x15` | hub → client | recipient id (16) — the direct-send recipient whose queue was full |
| `ResumeSession` | `0x16` | client → hub | resumption token (rest of frame; 32 bytes in practice) |
| `SessionResumed` | `0x17` | hub → client | reclaimed client id (16), token length (2, BE), renewed token, and — version 7+ only, PR #135/issue #109 — group count (2, BE) followed by that many `[nameLength (2, BE)][utf8 name]` restored-group entries |
| `SessionResumeRefused` | `0x18` | hub → client | none |
| `SubscribeTopic` | `0x19` | client → hub | UTF-8 topic pattern (rest of frame) |
| `UnsubscribeTopic` | `0x1A` | client → hub | UTF-8 topic pattern (rest of frame) |
| `PublishTopicMessage` | `0x1B` | client → hub | topic length (2, BE), UTF-8 topic, message bytes |
| `PublishTopicMessageWithHeaders` | `0x1C` | client → hub | topic length (2, BE), UTF-8 topic, header-block length (2, BE), header block, message bytes |
| `DeliverTopicMessage` | `0x1D` | hub → client | sender id (16), topic length (2, BE), UTF-8 topic, message bytes |
| `DeliverTopicMessageWithHeaders` | `0x1E` | hub → client | sender id (16), topic length (2, BE), UTF-8 topic, header-block length (2, BE), header block, message bytes |
| `SetClientAttributes` | `0x1F` | client → hub | attribute block (rest of frame; `HeaderEnvelope`-encoded key/value pairs, no separate length prefix) |
| `FindClientsRequest` | `0x20` | client → hub | correlation id (4, BE), criteria block (rest of frame; `HeaderEnvelope`-encoded key/value pairs) |
| `FindClientsResponse` | `0x21` | hub → client | correlation id (4, BE), result count (2, BE), that many `[id (16)][nameLength (2, BE)][utf8 name]` entries |
| `SubscribePresence` | `0x22` | client → hub | none |
| `UnsubscribePresence` | `0x23` | client → hub | none |
| `PresenceChanged` | `0x24` | hub → client | change type (1; `0x01` joined, `0x02` left), client id (16), name length (2, BE), UTF-8 name |

`0x24` is the highest opcode in use; the next new one is `0x25`. Topic-based publish/subscribe (issue
#37) is covered in full in [Topic pub/sub frames](#topic-pubsub-frames-issue-37) below and
[hub.md](hub.md#topic-based-publishsubscribe)/[client.md](client.md#topic-based-publishsubscribe). **Two
of its six opcodes (`DeliverTopicMessage`/`DeliverTopicMessageWithHeaders`) are a further confirming
example of the additive-opcode route — hub → client only, no version bump needed. The other four
(`SubscribeTopic`/`UnsubscribeTopic`/`PublishTopicMessage`/`PublishTopicMessageWithHeaders`) are client →
hub and, by this section's own rule, needed one — for one commit they shipped without it, then commit
`fb2f9a0` added `Protocol.TopicPubSubMinVersion = 8` and gated all four on both ends** — see
[Additive opcodes within a version](#additive-opcodes-within-a-version) below and
[known-issues.md](known-issues.md) KI-61 (fixed) for the full write-up of that history.

Client attributes (issue #38) — `SetClientAttributes`/`FindClientsRequest` are client → hub and gated
behind `Protocol.ClientAttributesMinVersion = 9` **from the outset**, learning KI-61's lesson rather than
repeating it; `FindClientsResponse` is hub → client and additive, no gate needed. See [Client attribute
frames](#client-attribute-frames-issue-38) below.

Presence (issue #39) — `SubscribePresence`/`UnsubscribePresence` are client → hub and gated behind
`Protocol.PresenceMinVersion = 10` from the outset, same discipline as issue #38; `PresenceChanged` is
hub → client and additive. `GetClientsAsync` needed no new opcode at all — it reuses
`FindClientsRequest`/`FindClientsResponse` (issue #38) with an empty criteria block. See [Presence
frames](#presence-frames-issue-39) below.

The four header-bearing opcodes
(`0x11`–`0x14`, PR #74, issue #32) are each the existing opcode's frame with one extra
`[headerBlockLength(2, BE)][headerBlock]` pair spliced in right after the fields that address the
message (recipient id / group name) and before the opaque message body — see
[Message headers](#message-headers) below for why these needed a version bump rather than the
additive-opcode route. `QueueSaturated` (`0x15`, PR #87, issue #30) is a genuine new opcode, not a
fourth-route header-only capability — but it follows the additive-opcode route `GroupJoinRefused`
established, needing no version bump; see
[Additive opcodes within a version](#additive-opcodes-within-a-version) below.

**PR #83's request/response helper (`RequestAsync`/`ReplyAsync`) added no new opcode and no protocol
version.** A request and its reply are both ordinary `SendMessageWithHeaders`/`DeliverMessageWithHeaders`
(`0x11`/`0x12`) frames — the same two opcodes any `SendAsync(recipientId, message, headers)` call already
produces — distinguished only by two new well-known keys inside the existing header block. See
[Request/response headers](#request-response-headers) below.

**PR #84's delivery acknowledgement (`SendAsync(..., DeliveryOptions.RequireAck(...), ...)`) takes the
identical route.** The original message and the recipient's automatic acknowledgement are both ordinary
`SendMessageWithHeaders`/`DeliverMessageWithHeaders` frames too — no new opcode, no protocol version bump
— distinguished from an application's own headers, and from a request/reply pair, by three more new
well-known keys. See [Delivery acknowledgement headers](#delivery-acknowledgement-headers) below.

**`RegistrationErrorCode`** (`RegistrationErrorCode.cs`, sent as the byte after `Error`):
`DuplicateClientName=0x01`, `UnsupportedProtocolVersion=0x02`, `ClientNameTooLong=0x03`,
`HubAtCapacity=0x04`, `AuthenticationFailed=0x05` (`RegistrationErrorCode.cs:31`).

---

## Registration handshake

```
client → hub : [0x04 RegistrationRequest][versionMin][versionMax][nameLen u16 BE][utf8 clientName][credential...]
hub → client : [0x01 RegistrationComplete][clientId (16 bytes)][negotiatedVersion]  # success, 18 bytes
             | [0x01 RegistrationComplete][clientId (16)][negotiatedVersion][tokenLen u16 BE][token]
                                                                                   # success, version 6+ with resumption on
             | [0x05 Error][errorCode]                                             # refused
```

**The registration frame itself has never changed shape, and issue #43 deliberately did not change it
either** — see [Session resumption](#session-resumption) for why a resumption token could not be spliced
into it. The *reply* grew a conditional tail, which is safe in the one direction it matters: the hub
knows the negotiated version before it builds the reply, and the client reads that version from a fixed
offset (byte 17) before deciding whether to read anything after it. A reply is exactly 18 bytes whenever
the negotiated version is below 6 or the hub has resumption switched off, which is every reply any
earlier build produced.

`versionMin`/`versionMax` is the client's supported range; `MeshClient` always sends
`Protocol.MinSupportedVersion`/`Protocol.MaxSupportedVersion` (`4`/`8` as of commit `fb2f9a0`, which
widened the range from `4`/`7` to gate topic pub/sub — see [Topic pub/sub frames](#topic-pubsub-frames-issue-37)
below), so a hub and client both built from this codebase negotiate `8`, but the wire and the hub's
negotiation both treat it as a real range — a client built against an older copy of the library
(advertising `4`/`4`) still interoperates, negotiating down to `4` and losing only the header envelope —
see
[Versioning](#versioning). The **credential is everything after the name** — its length is implied by
the frame length, so it can be empty (the default). The hub does not interpret those bytes; it hands
them to the configured `ClientAuthenticator` and nothing else reads them. See
[hub.md](hub.md#authentication) and [types.md](types.md#authentication-types).

Hub-side validation order (`MeshHub.cs:956-1050`), each failure sends the error (if applicable) and drops
the connection:
1. Frame ≥ **3** bytes and opcode `0x04` — else drop silently (no error frame) (`:956-961`).
2. **Negotiate a version** via `TryNegotiateProtocolVersion(versionMin, versionMax, out negotiatedVersion)`
   (`MeshHub.cs:1381-1403`, called at `:963`) — else `Error(UnsupportedProtocolVersion)` (`:963-969`).
   Negotiation fails if `versionMin > versionMax` (an inverted range) or if `[versionMin, versionMax]`
   does not overlap `[Protocol.MinSupportedVersion, Protocol.MaxSupportedVersion]`; otherwise it picks the
   **highest version common to both ranges**. **This is checked before the length checks below**, so a
   3-byte frame carrying an unsupported range still gets an error reply.
3. Frame ≥ 5 bytes, i.e. long enough to carry the name length — else drop silently (`:971-975`).
4. `nameLen != 0` **and** frame ≥ `5 + nameLen` — else drop silently (`:977-984`). A declared length of
   zero, or one running past the payload, is treated as malformed: **no error frame, connection
   dropped**. The empty name is refused here rather than admitted so it cannot reserve the empty string
   in the name registry.
5. Decode the name from bytes `[5, 5+nameLen)`; `clientName.Length ≤ 256` **chars** — else
   `Error(ClientNameTooLong)` (`:988-994`).
6. **Authentication**, only when an authenticator was configured (`:996-1017`). Two parts, in order: an
   at-capacity **early-out** — already-claimed slots `>= maxClients` → `Error(HubAtCapacity)` without the
   callback running (`:1003-1007`) — then the callback itself, given the name and credential
   (`:1009-1016`). Refusal, throw, cancellation or timeout → `Error(AuthenticationFailed)`.
7. **Capacity claim** (`:1024-1028`): one atomic compare-and-swap takes a client slot if and only if fewer
   than `maxClients` are claimed. Failure → `Error(HubAtCapacity)`. This, not the early-out in step 6 and
   not the registered client count, is the decision that admits or refuses on capacity, so concurrent
   registrations cannot all pass and overshoot the cap.
8. Name not already claimed (`_clientNames.TryAdd`) — else `Error(DuplicateClientName)` (`:1032-1037`).
9. Send `RegistrationComplete` carrying the assigned id and the `negotiatedVersion` byte from step 2
   (`:1043-1047`).

Note that the **binding** capacity decision happens **after** authentication — an unauthenticated peer
cannot hold a slot away from one that would authenticate — and **before** the name is reserved, so a
client refused on either count never claims a name. The early-out in step 6 preserves the separate
property that a full hub never runs the callback.

Client-side (`MeshClient.cs:221-237`): an `Error` reply → `RegistrationRefusedException(errorCode)`; any
reply that isn't exactly an 18-byte `RegistrationComplete` → `InvalidOperationException`. On success the
trailing byte is read into `IMeshClient.NegotiatedProtocolVersion` (`MeshClient.cs:241`,
`IMeshClient.cs:28`) — `0` whenever the client is not connected.

A connection that never sends a valid registration within `registrationTimeout` (default 10 s) is
dropped without an error frame (`MeshHub.cs:944-952`).

---

## Message frames (exact byte offsets)

Direct send / deliver:
```
SendMessage       : [0x02][recipientId 16][body...]              # client→hub, needs len ≥ 17
DeliverMessage    : [0x03][senderId 16][body...]                 # hub→client, needs len ≥ 17
SendMessageWithHeaders    : [0x11][recipientId 16][headerLen u16 BE][headerBlock][body...]  # client→hub, needs len ≥ 19
DeliverMessageWithHeaders : [0x12][senderId 16][headerLen u16 BE][headerBlock][body...]     # hub→client, needs len ≥ 19
```
Broadcast is sent as `BroadcastMessage` but **delivered as `DeliverMessage`** — recipients cannot tell a
broadcast from a direct message (`MeshHub.BroadcastMessage` builds a `0x03` frame, `MeshHub.cs:2021-2026`
— corrected this pass; the previous citation, `:1674`, had pointed at unrelated `MonitorHeartbeatAsync`
code):
```
BroadcastMessage  : [0x0B][body...]                              # client→hub
```

Groups:
```
JoinGroup         : [0x0C][utf8 groupName...]                    # whole remainder is the name
LeaveGroup        : [0x0D][utf8 groupName...]
GroupMessage      : [0x0E][nameLen u16 BE][utf8 groupName][body...]   # client→hub, needs len ≥ 3
DeliverGroupMessage: [0x0F][senderId 16][nameLen u16 BE][utf8 groupName][body...]  # hub→client, needs len ≥ 19
GroupJoinRefused  : [0x10][utf8 groupName...]                    # hub→client, client needs len > 1
GroupMessageWithHeaders    : [0x13][nameLen u16 BE][utf8 groupName][headerLen u16 BE][headerBlock][body...]              # client→hub, needs len ≥ 5
DeliverGroupMessageWithHeaders : [0x14][senderId 16][nameLen u16 BE][utf8 groupName][headerLen u16 BE][headerBlock][body...]  # hub→client, needs len ≥ 21
```
The hub passes the original name bytes straight through from the inbound `GroupMessage` into the
outbound `DeliverGroupMessage` rather than re-encoding the decoded string (`MeshHub.cs:1127-1134`,
`:2369-2374`). The header-bearing group frames do the same, and additionally pass the header block
through as an opaque `ReadOnlyMemory<byte>` — the hub reads only its **length**, never its content, on
both the direct and group paths (`RouteMessageWithHeaders`/`SendToGroupWithHeaders`, see
[hub.md](hub.md#routing-helpers)).

<a id="message-headers"></a>

### Message headers

`MessageHeaders` (`Messages/MessageHeaders.cs`) is a small, immutable, string-keyed
`IReadOnlyDictionary<string, string>` that travels alongside a message body without the hub ever decoding
it into a `MessageHeaders` object — the hub only ever reads a header block's declared *length*, to route
or strip it, with **one narrow exception since PR #85**: it scans (without fully decoding) for a single
well-known expiry key, see [Message expiry headers](#message-expiry-headers) below.
`MessageHeaders.Empty` is the shared no-headers instance; `SendAsync`/`SendToGroupAsync`
overloads taking a `MessageHeaders` fall back to the plain, header-less frame and cost nothing extra on
the wire when it is empty (`headers.Count == 0`) — see [client.md](client.md#sending-headers).

**Wire format of the header block** (`Messages/HeaderEnvelope.cs`): a flat, back-to-back run of entries,
`[keyLength(1)][UTF-8 key][valueLength(2, BE)][UTF-8 value]`, read until exactly as many bytes as the
preceding block-length field declared have been consumed — there is no entry count. A key longer than
255 bytes once UTF-8-encoded, or a value longer than 65 535 bytes, cannot be represented and is rejected
at encode time (`ArgumentException`), as is a header set whose total encoded length would not fit the
2-byte block-length prefix (`HeaderEnvelope.GetEncodedLength`, throws past `ushort.MaxValue`).

**Decoding is defensive on both sides.** `HeaderEnvelope.Read` bounds-checks every internal key/value
length against the block's own declared length and throws `FormatException` rather than letting a
span-slice exception escape on a malformed block; the hub never calls **this** method (it only reads the
block's length to route/strip), but `MeshClient` does, on receipt of `DeliverMessageWithHeaders`/
`DeliverGroupMessageWithHeaders`. There, `TryReadHeaderBlock` (`MeshClient.cs:1419-1431`) catches the
`FormatException`, logs a warning, and drops **only that one frame** rather than tearing down the
connection — the same "one bad frame must not kill the loop" principle as the rest of the receive loop
(see [Length-guard behaviour](#length-guard-behaviour-why-malformed-frames-do-nothing)). **Since PR #85
the hub does call a sibling method, `HeaderEnvelope.TryReadValue`**, which applies the identical
bounds-checking and throws the identical `FormatException` on a malformed block, but returns one value
instead of decoding every entry — see [Message expiry headers](#message-expiry-headers) below and
[hub.md](hub.md#dropping-expired-frames) for how the hub's own call site treats that exception.

**Headers require both ends to have negotiated at least protocol version 5**
(`Protocol.HeaderEnvelopeMinVersion`). `MeshClient.SendAsync`/`SendToGroupAsync` throw
`NotSupportedException` if called with a non-empty `MessageHeaders` on a connection negotiated below
that — headers are never silently dropped on the sending side. On the **hub** side, each recipient's own
negotiated version decides what it receives, independently of the sender's: `RouteMessageWithHeaders`
forwards the header-bearing frame unchanged to a version-5+ recipient, or strips the header block
entirely and falls back to the plain `DeliverMessage`/`DeliverGroupMessage` frame for a recipient
negotiated below 5, since that recipient would not recognise the header-bearing opcode at all. For a
group, this means members can receive **different frame shapes for the same send** depending on what
each negotiated — see [hub.md](hub.md#routing-helpers). At most one frame of each shape is built per
call regardless of group size.

**Why this needed a version bump instead of the additive-opcode route.** Unlike `GroupJoinRefused`, two
of the four new opcodes (`SendMessageWithHeaders`, `GroupMessageWithHeaders`) travel **client → hub**,
which the additive-opcode reasoning below explicitly excludes — an older hub receiving one would not
recognise it and would silently drop it, losing the message rather than degrading gracefully. Gating all
four behind `HeaderEnvelopeMinVersion` and having both hub and client check
`NegotiatedProtocolVersion` before using them is the correct route for a capability either peer can
*originate*, not just receive.

**`GroupJoinRefused` echoes the same bytes, and that is load-bearing rather than tidy.** The hub copies
the inbound `JoinGroup` name bytes and replies with exactly those (`RefuseGroupJoin`, `MeshHub.cs:2232`,
copy at `:2104`, echo at `:2242-2244`). Re-encoding the *decoded* string is not size-preserving: every
byte that is not valid UTF-8 decodes to `U+FFFD` and re-encodes as three, so a name of invalid bytes
would **triple**. Join frames carry no length cap of their own (KI-8), so a re-encoded refusal could
exceed the transport's 1 MiB payload limit and throw on send — which faults that connection's send loop,
and the faulted send loop is awaited during teardown, abandoning the rest of it including the release of
the client's capacity slot. Echoing keeps the refusal no larger than the frame that provoked it, which
the transport has already bounded. If you touch this path, keep the echo.

**Group sends require membership.** The hub silently drops a `GroupMessage` from a client that has not
joined the target group (`MeshHub.cs:2341`, `:2351-2357`). There is no error frame for this — a correct
client only sends to groups it has joined, and it learns that a join did *not* take effect from
`GroupJoinRefused`. See [hub.md](hub.md#group-authorisation) and
[known-issues.md](known-issues.md) KI-2.

<a id="request-response-headers"></a>

### Request/response headers (PR #83)

`RequestAsync`/`ReplyAsync` (`IMeshClient`, see [client.md](client.md#request-response)) are built
entirely on the header block above — **no new opcode, no new frame shape, and no protocol version was
added for this feature.** A request and its reply are both ordinary `SendMessageWithHeaders`/
`DeliverMessageWithHeaders` (`0x11`/`0x12`) frames, gated by the same `HeaderEnvelopeMinVersion` (`5`)
check every other non-empty `MessageHeaders` send already goes through. What distinguishes them from an
application's own headers is two new well-known keys, `Messages/RequestReplyHeaderKeys.cs` (`internal`):

| Key | Wire string | Present on |
|---|---|---|
| `RequestReplyHeaderKeys.CorrelationId` | `"mesh.request-id"` | both the request and its reply — an invariant-culture integer, the sender's own correlation id |
| `RequestReplyHeaderKeys.Reply` | `"mesh.reply"` | the reply only, value `"1"` — its absence is what distinguishes an incoming request from an incoming reply that both carry `CorrelationId` |

**The hub does not know these keys exist.** It never decodes header *content* on either the direct or
group path (see above) — it only reads the header block's *length* to route or strip it. Request/response
correlation, matching and the sender-identity check that prevents a hostile peer from forging a reply
(see [client.md](client.md#request-response)) are entirely the two `MeshClient` instances' own
responsibility. A consequence: `MessageHeaders`'s own constructor guard (below) only stops an
*application* calling `SendAsync`/`RequestAsync` on **this** library's `MeshClient` from colliding with
these keys — it cannot stop a differently-implemented peer, or a hand-built frame, from sending a
`SendMessageWithHeaders` frame carrying `mesh.reply=1` to a real `MeshClient`, which will intercept and
silently drop it before `MessageReceived` regardless of who sent it. See
[known-issues.md](known-issues.md) KI-42 and KI-43.

<a id="delivery-acknowledgement-headers"></a>

### Delivery acknowledgement headers (PR #84)

`SendAsync(..., DeliveryOptions.RequireAck(...), ...)` (`IMeshClient`, see
[client.md](client.md#delivery-acknowledgement)) is built on the identical route PR #83 established: the
message and its acknowledgement are both ordinary `SendMessageWithHeaders`/`DeliverMessageWithHeaders`
(`0x11`/`0x12`) frames, gated by the same `HeaderEnvelopeMinVersion` (`5`) check. What distinguishes them
is three new well-known keys, `Messages/DeliveryAcknowledgementHeaderKeys.cs` (`internal`):

| Key | Value | Wire string | Present on |
|---|---|---|---|
| `DeliveryAcknowledgementHeaderKeys.CorrelationId` | the sending client's own acknowledgement correlation id, invariant-culture integer | `"mesh.ack-id"` | both the original message and its acknowledgement |
| `DeliveryAcknowledgementHeaderKeys.Request` | `"1"` | `"mesh.ack-request"` | the original message only — marks it as wanting an acknowledgement |
| `DeliveryAcknowledgementHeaderKeys.Ack` | `"1"` | `"mesh.ack"` | the acknowledgement frame only — its absence is what distinguishes an incoming message that happens to carry `CorrelationId` from the acknowledgement answering it |

**The acknowledgement is sent by the recipient's `MeshClient` automatically, not by application code**,
once `MessageReceived` has been raised for the message (successfully or not) — see
[client.md](client.md#delivery-acknowledgement) for the exact receive-loop sequencing, including why the
send is deliberately fire-and-forget rather than awaited.

**The hub is exactly as blind to these three keys as it is to the request/response pair above**, for the
identical reason (it only reads the header block's length, never its content), with the identical
consequence: `ThrowIfReservedHeaderKeyPresent` only stops an application on **this** library's
`MeshClient` from colliding with them on the sending side; it cannot stop a hand-built frame, or a
differently-implemented peer, from sending one carrying `mesh.ack=1`, which any receiving `MeshClient`
will intercept and drop before `MessageReceived` regardless of who sent it. See
[known-issues.md](known-issues.md) KI-42, KI-44, KI-45 and KI-46.

<a id="message-expiry-headers"></a>

### Message expiry headers (PR #85)

`SendAsync(..., TimeSpan, ...)` (`IMeshClient`, see
[client.md](client.md#message-expiry-time-to-live)) rides the same route again: an ordinary
`SendMessageWithHeaders`/`DeliverMessageWithHeaders` (`0x11`/`0x12`) frame, gated by the same
`HeaderEnvelopeMinVersion` (`5`) check, carrying one new well-known key,
`Messages/MessageExpiryHeaderKeys.cs` (`internal`):

| Key | Value | Wire string | Present on |
|---|---|---|---|
| `MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds` | the absolute expiry instant, Unix milliseconds, invariant-culture integer, computed from the **sender's own clock** | `"mesh.expires-at"` | the message only — there is no reply/acknowledgement frame for this feature |

**This was the first exception to "the hub only ever reads a header block's length, never its content" —
PR #87 (below) added a second.** Every other header-bearing feature above (request/response, delivery
acknowledgement) is invisible to the hub — it forwards or strips the whole block based on the recipient's
negotiated version alone. Message expiry is different: `MeshHub.SendLoopAsync` calls a new,
narrowly-scoped `HeaderEnvelope.TryReadValue` (`Messages/HeaderEnvelope.cs:175-233`) to search a queued
frame's header block for exactly this one key, so it can drop an already-expired frame before writing it
to the transport — see [hub.md](hub.md#dropping-expired-frames). `TryReadValue` is a linear scan of the
block's raw entries, not the general `HeaderEnvelope.Read` decode: it never allocates the
`Dictionary<string, string>`/`MessageHeaders` a full decode would, and it still never touches the sender
id, group name or message body. The distinction that survives is "the hub never decodes the full header
set, and never reads the body" — not "the hub never reads header content at all", which was true before
this PR and is no longer accurate as a blanket statement even before PR #87 extended it further. See
[known-issues.md](known-issues.md) KI-47 for the clock-skew consequence of the check this enables.

<a id="backpressure-header"></a>

### Backpressure header (PR #87)

`SendAsync(..., DeliveryOptions, ...)` with `AwaitCapacity` set (`IMeshClient`, see
[client.md](client.md#backpressure-signalling)) also rides the header-envelope route: an ordinary
`SendMessageWithHeaders` (`0x11`) frame, gated by the same `HeaderEnvelopeMinVersion` (`5`) check, carrying
one new well-known key, `Messages/BackpressureHeaderKeys.cs` (`internal`):

| Key | Value | Wire string | Present on |
|---|---|---|---|
| `BackpressureHeaderKeys.AwaitCapacity` | `"1"` | `"mesh.await-capacity"` | the original message only — there is no reply/acknowledgement frame for this feature |

**This is the second exception to "the hub only ever reads a header block's length, never its content"**,
and reads at a different point in the pipeline from message expiry's: `WantsAwaitCapacity`
(`MeshHub.cs:1979-1990`) is called from `RouteMessageWithHeaders`, at **enqueue** time, on the frame just
received *from the sender* — not, like `IsExpiredFrame`, from `SendLoopAsync` at **dequeue** time on a
frame already queued for a recipient. It uses the identical `HeaderEnvelope.TryReadValue` single-key scan
as message expiry, so it never allocates a full `MessageHeaders` decode either, and a malformed header
block is tolerated as "not requested" rather than faulted (mirroring `IsExpiredFrame`'s own malformed-block
handling). **Only `RouteMessageWithHeaders` calls it** — `RouteMessage` (the header-less direct overload)
and the three fan-out routing methods never do, since only a direct send with headers can have its
capacity wait honoured; see [hub.md](hub.md#backpressure-signalling-and-awaiting-capacity).

<a id="priority-header"></a>

### Priority header (`ab16567`)

Message priority (see [client.md](client.md#message-priority) and [hub.md](hub.md#priority-lanes)) rides
the same header-envelope route: an ordinary `SendMessageWithHeaders`/`GroupMessageWithHeaders`
(`0x11`/`0x13`) frame carrying one new well-known key, `Messages/MessagePriorityHeaderKeys.cs` (`internal`):

| Key | Value | Wire string | Present on |
|---|---|---|---|
| `MessagePriorityHeaderKeys.Priority` | `"high"` / `"low"` / `"normal"` | `"mesh.priority"` | only written when the priority is not `Normal` — a `Normal`-priority send costs nothing extra on the wire |

**This is a third exception to "the hub only ever reads a header block's length, never its content"** —
`ReadPriority` (`MeshHub.cs:2970-2981`) uses the identical `HeaderEnvelope.TryReadValue` single-key scan
as expiry and backpressure. Parsing is total: an absent, malformed, or unrecognised value resolves to
`Normal`, never throws. Read at **enqueue** time by `RouteMessageWithHeaders`/`SendToGroupWithHeaders`
only — the plain/headerless direct and group paths and `BroadcastMessage` (no header-bearing broadcast
opcode exists) always enqueue at `Normal`, regardless of anything the sender does. See
[known-issues.md](known-issues.md) KI-54.

<a id="trace-context-headers"></a>

### Trace context headers (feat #92)

Opt-in distributed tracing (see [client.md](client.md#distributed-tracing)) rides the same route, carrying
two new well-known keys, `Messages/TraceContextHeaderKeys.cs` (`internal`) — deliberately **not**
`mesh.`-prefixed, so a bridge into HTTP/gRPC/a message broker sees the standard W3C names:

| Key | Wire string | Present on |
|---|---|---|
| `TraceContextHeaderKeys.TraceParent` | `"traceparent"` | any header-bearing send made inside an active trace (a library-owned `Meshworx.Send` span with a listener attached, or ambient `Activity.Current`) |
| `TraceContextHeaderKeys.TraceState` | `"tracestate"` | the same sends, only when the W3C `tracestate` value is non-empty |

**The hub never reads these** — unlike priority/expiry/backpressure, trace context is passed through
completely opaque, exactly like an application's own header. Both keys are still reserved (an application
cannot set them directly) purely so a traced send's own values are never overwritten by, or confused with,
a caller-supplied header of the same name.

<a id="chunk-headers"></a>

### Chunk headers (feat #93, reserved by PR #134/issue #107)

Large-message chunking (see [client.md](client.md#large-message-chunking)) splits a payload across
multiple ordinary `SendMessageWithHeaders` (`0x11`) frames, each carrying three well-known keys,
`Messages/ChunkHeaderKeys.cs` (`internal`):

| Key | Value | Wire string |
|---|---|---|
| `ChunkHeaderKeys.Id` | a `Guid` ("D" format), fresh per `SendLargeAsync` call | `"mesh.chunk.id"` |
| `ChunkHeaderKeys.Index` | zero-based chunk index | `"mesh.chunk.index"` |
| `ChunkHeaderKeys.Count` | total chunk count for this transfer (≤ `ChunkHeaderKeys.MaxChunksPerMessage`, 4096) | `"mesh.chunk.count"` |

**The hub has no awareness of chunking at all — each chunk is routed as an ordinary opaque header-bearing
frame**, unlike priority/expiry/backpressure; reassembly is purely a `MeshClient`-side concern
(`ChunkReassembler`). **These three keys were not originally reserved** — PR #93's first landing let an
application set them directly, and a completed reassembly left them on the delivered message's headers
unstripped. Both were fixed by the follow-up `144ab0b` (issue #107): the three keys were added to
`ReservedHeaderKeys`, and the reassembled message's headers now have them stripped
(`ChunkHeaderKeys.WithoutChunkHeaders`) before `MessageReceived`/`GroupMessageReceived` fires.

<a id="session-resumption"></a>

### Session resumption (issue #43, extended by PR #135/issue #109)

Three opcodes and one conditional field, gated on `Protocol.SessionResumptionMinVersion` (`6`). This is
the second capability to widen `MaxSupportedVersion`, and the shape of the exchange is the interesting
part. A fourth widening, `Protocol.SessionResumedGroupsMinVersion` (`7`, PR #135/issue #109), appends a
conditional group-membership block to the accepted reply — see below.

```
hub → client : [0x01 RegistrationComplete][clientId 16][negotiatedVersion][tokenLen u16 BE][token]
client → hub : [0x16 ResumeSession][token...]
hub → client : [0x17 SessionResumed][reclaimedClientId 16][tokenLen u16 BE][renewedToken]   # accepted, version 6
             | [0x17 SessionResumed][reclaimedClientId 16][tokenLen u16 BE][renewedToken]
                                     [groupCount u16 BE]([nameLength u16 BE][utf8 name])×groupCount
                                                                                    # accepted, version 7+
             | [0x18 SessionResumeRefused]                                                  # refused
```

**The group block is purely additive and version-gated the same way the token tail is.** A connection
negotiated at exactly `6` gets the byte-identical pre-#109 reply — `MeshHub` never appends the block below
`SessionResumedGroupsMinVersion`. At `7`+, `MeshHub.BuildReportableGroupNameBytes` builds the block from
whatever `RestoreGroupMembershipAsync` actually restored, with two bounds: a group name whose UTF-8
encoding would overflow the `ushort` `nameLength` prefix is dropped from the block entirely (the
membership itself is unaffected — only the reply cannot name it), and the reported group count is capped
at `ushort.MaxValue` (65,535), with any remainder silently unreported. `MeshClient.RestoreJoinedGroupsFromResumedReply`
parses this block and **replaces** `JoinedGroups` wholesale from it — see
[client.md](client.md#session-resumption), [hub.md](hub.md#session-resumption) and
[known-issues.md](known-issues.md) KI-59/KI-60. **No equivalent block exists for topic subscriptions** —
the restore this section describes has no topic-side counterpart anywhere in the resumption exchange, so
a resumed client's topic subscriptions are simply gone; see [known-issues.md](known-issues.md) KI-65.

**Resumption happens *after* registration, not inside it, and that is forced by the handshake's own
ordering.** The obvious design — carry the token in the `RegistrationRequest` frame — cannot work: the
client must send that frame **before** it knows what version was negotiated, so it cannot know whether
to use the old layout or a new one. A client that always sent the new layout would have its token
length field and token read as **credential bytes** by any hub that predates the feature, silently
corrupting authentication. Making it a post-registration exchange removes the problem entirely:

- The client checks `NegotiatedProtocolVersion` — a number it now has — before sending `0x16` at all.
- A hub that does not know the opcode falls off its dispatch ladder and ignores it, and the client's
  bounded wait for a reply expires leaving it on the identity it already has. That is precisely the
  required "expired/invalid tokens fall back to a fresh registration" behaviour, reached without a
  special case.
- Nothing in the registration frame moved, so no older peer can misparse anything.

**The reply is not necessarily the next frame on the wire.** The hub drains any offline store (issue
#28) onto the client's queue at registration, so `DeliverMessage` frames can arrive before `0x17`. The
client therefore handles the reply **in its receive loop**, completing a pending-resume
`TaskCompletionSource` that `ConnectAsync` awaits, rather than with a second blocking read that would
consume the wrong frame.

**Token rules** (hub side, `MeshHub.ResumeSessionAsync`):

| Rule | Why |
|---|---|
| 32 bytes from `RandomNumberGenerator` | it is a bearer credential for an identity |
| Only the **SHA-256 hash** is retained | the session table is then not a bag of live secrets |
| **Single use** — a successful resume issues a fresh token and invalidates the old | a token captured off the wire cannot be replayed later |
| The session must be **dormant** (its connection gone) | a token reclaims an unused identity, never takes a live one |
| The session's **name must match** the resuming connection's registered name | otherwise any token holder could take over any identity |
| Validated by `TryGetValue` **then** claimed by `TryRemove` | a token that fails validation is left in place for its rightful owner; the winning `TryRemove` is what makes two racing resumes resolve to one |

<a id="topic-pubsub-frames-issue-37"></a>

### Topic pub/sub frames (issue #37)

Six opcodes. **For the one commit that introduced them, this shipped with no version gate at all** —
`Protocol.MaxSupportedVersion` stayed `7` and no `Protocol.TopicPubSubMinVersion` constant existed,
unlike [Message headers](#message-headers) and [Session resumption](#session-resumption) above, both of
which added one when they landed. The very next commit, `fb2f9a0`, fixed this: `MaxSupportedVersion` is
now `8` and `Protocol.TopicPubSubMinVersion = 8` gates all four client → hub opcodes below, on both hub
and client. See [known-issues.md](known-issues.md) KI-61 (fixed) for the full history of that gap.

```
client → hub : [0x19 SubscribeTopic][utf8 pattern...]                                      # whole remainder is the pattern
client → hub : [0x1A UnsubscribeTopic][utf8 pattern...]
client → hub : [0x1B PublishTopicMessage][topicLen u16 BE][utf8 topic][body...]             # needs len ≥ 3
client → hub : [0x1C PublishTopicMessageWithHeaders][topicLen u16 BE][utf8 topic][headerLen u16 BE][headerBlock][body...]  # needs len ≥ 5
hub → client : [0x1D DeliverTopicMessage][senderId 16][topicLen u16 BE][utf8 topic][body...]                        # needs len ≥ 19
hub → client : [0x1E DeliverTopicMessageWithHeaders][senderId 16][topicLen u16 BE][utf8 topic][headerLen u16 BE][headerBlock][body...]  # needs len ≥ 21
```

A pattern is the same dot-separated hierarchy as a concrete topic, with two reserved wildcard segments
borrowed from MQTT — `+` matches exactly one segment, `#` matches the remainder and may only be the
pattern's final segment. Matching is entirely hub-side, via `TopicSubscriptionTrie`
(`TopicSubscriptionTrie.cs`) — see [hub.md](hub.md#topic-based-publishsubscribe) for the matching rules
and its concurrency model. The hub never decodes a header block on this path beyond the same
single-key `ReadPriority` scan the direct/group paths already use (see
[Priority header](#priority-header-ab16567)) — a topic publish is otherwise exactly as opaque to the hub
as a group send.

**Publishing requires no subscription of the publisher's own**, unlike a group send's membership
requirement (`GroupMessage`, [Message frames](#message-frames-exact-byte-offsets) above) — a topic is an
address, not a membership, so a publisher that has never called `SubscribeAsync` still reaches every
matching subscriber. There is also no authorisation seam of any kind for either subscribe or publish,
unlike groups' optional `GroupAuthoriser` — see [known-issues.md](known-issues.md) KI-62.

**This is the sixth new-opcode addition documented in this file, and — for one commit — the first one
that did not follow its own [additive-opcodes rule](#additive-opcodes-within-a-version) correctly.**
`DeliverTopicMessage`/`DeliverTopicMessageWithHeaders` (`0x1D`/`0x1E`) are hub → client only and are a
further confirming example of that rule, exactly like `GroupJoinRefused` and `QueueSaturated` before
them. `SubscribeTopic`/`UnsubscribeTopic`/`PublishTopicMessage`/`PublishTopicMessageWithHeaders`
(`0x19`–`0x1C`) are client → hub, which by the rule's own stated test — "one a client may *send*... must
bump `Protocol.MaxSupportedVersion`", the exact test `ResumeSession` was built to satisfy — needed a
widening and a `Protocol.TopicPubSubMinVersion` gate the same shape as
`Protocol.SessionResumptionMinVersion`. That did not ship with the feature itself, but the very next
commit, `fb2f9a0`, added exactly that: `Protocol.TopicPubSubMinVersion = 8`, checked by both
`MeshClient` (`RequireTopicPubSubSupport`, throwing `NotSupportedException`) and `MeshHub` (an added
`connection.NegotiatedProtocolVersion >= Protocol.TopicPubSubMinVersion` clause on each of the four
dispatch branches). The failure mode the gap briefly created was silent and specific — a client calling
`SubscribeAsync`/`PublishAsync` against a hub built without the feature got no exception, no refusal, and
`SubscribeAsync` still returned with the pattern appearing in `SubscribedTopics`, but no message ever
arrived — see [known-issues.md](known-issues.md) KI-61 (fixed) for the full write-up.

<a id="client-attribute-frames-issue-38"></a>

### Client attribute frames (issue #38)

Three opcodes, gated behind `Protocol.ClientAttributesMinVersion = 9` **from the outset** — the lesson
KI-61 taught the topic pub/sub feature the hard way, applied here without needing a follow-up commit.

```
client → hub : [0x1F SetClientAttributes][attributeBlock...]                                        # whole remainder is the block
client → hub : [0x20 FindClientsRequest][correlationId 4][criteriaBlock...]                          # needs len ≥ 5
hub → client : [0x21 FindClientsResponse][correlationId 4][resultCount u16 BE][entries...]           # needs len ≥ 7
```

Each `entries` item is `[id 16][nameLength u16 BE][utf8 name]`, repeated `resultCount` times.

The attribute and criteria blocks reuse the `HeaderEnvelope` codec [Message headers](#message-headers)
already defined — attributes are, on the wire, exactly the same shape as headers, a small string-keyed
map — rather than a second, parallel encoding. Neither block carries its own length prefix the way a
header block does elsewhere in this file (`[headerLen u16 BE][headerBlock]`); it consumes the rest of the
frame instead, the same shape `SubscribeTopic`'s pattern already uses, since there is nothing else in
either frame that needs to follow it.

**`SetClientAttributes` has no reply.** A bag rejected for exceeding `Protocol.MaxClientAttributeCount`/
`MaxClientAttributeKeyLength`/`MaxClientAttributeValueLength` is dropped silently, the same shape as every
other malformed-or-oversized fire-and-forget frame in this protocol — see
[Length-guard behaviour](#length-guard-behaviour-why-malformed-frames-do-nothing) below. `MeshClient`
itself never sends one that would be rejected, since `UpdateAttributesAsync` validates the identical
bounds client-side first.

**`FindClientsRequest`/`FindClientsResponse` follow `ClientLookupRequest`/`ClientLookupResponse`'s
correlation-id shape** almost exactly, with one difference: the response can carry many entries rather
than at most one, so it needs its own `resultCount` field the lookup reply does not. Answering the query
scans every connected client — see [hub.md](hub.md#client-attributes--directory-queries) for why that is
an acceptable cost for this call specifically, and why the reply itself is still bounded rather than
allowed to grow with the matched population.

**No authorisation seam exists for either verb, and an empty-criteria query enumerates every connected
client** — see [known-issues.md](known-issues.md) KI-66 and KI-67.

<a id="presence-frames-issue-39"></a>

### Presence frames (issue #39)

Three opcodes, gated behind `Protocol.PresenceMinVersion = 10` from the outset — the same discipline
issue #38 applied, not issue #37's.

```
client → hub : [0x22 SubscribePresence]                                                          # no payload
client → hub : [0x23 UnsubscribePresence]                                                         # no payload
hub → client : [0x24 PresenceChanged][changeType 1][clientId 16][nameLength u16 BE][utf8 name]    # needs len ≥ 20
```

`changeType` is `0x01` (joined) or `0x02` (left) — `Messages/PresenceChangeType.cs`, a public enum (unlike
every other wire-level discriminator in this protocol, which stays `internal` alongside `MessageType`
itself, since this one is also the shape of the public `PresenceChangedEventArgs.ChangeType` property).

**`GetClientsAsync` is not a fourth opcode.** It sends the existing `FindClientsRequest` (issue #38) with
an empty criteria block and reads back the existing `FindClientsResponse` — an empty `AttributeQuery`
already matches every connected client, so "list everyone" needed no new frame shape at all, only a named
client-side method. See [Client attribute frames](#client-attribute-frames-issue-38) above.

**Presence is opt-in on the hub as well as gated by version** — a `MeshHub` not constructed with
`enablePresence: true` silently ignores `SubscribePresence`/`UnsubscribePresence`, the same shape as an
unrecognised opcode (KI-9): no error frame, and no `PresenceChanged` is ever pushed, whatever the client
believes its subscription state to be. See [hub.md](hub.md#presence--roster).

<a id="peer-link-frames-issue-40"></a>

### Peer link frames (issue #40)

**A separate protocol from everything above.** Every opcode from here on is spoken only between two
`MeshHub` instances over a link established by `LinkPeerAsync`, never by `MeshClient` — versioned
independently via `Protocol.MinFederationVersion`/`MaxFederationVersion` (currently a fixed `1`, no range
to negotiate yet), not via `Protocol.MinSupportedVersion`/`MaxSupportedVersion`. There is no reason for
the two protocols to move in step, and this one shares the `MessageType` byte space purely because that
enum is where every wire opcode in this library happens to be listed.

```
peer → peer : [0x25 PeerHello][hubId 16][versionMin 1][versionMax 1][credential...]                  # rest of frame is the credential
peer → peer : [0x26 PeerHelloAck][hubId 16][negotiatedVersion 1]
peer → peer : [0x27 PeerRouteAdvertise][entryCount u16 BE][{id 16}{nameLen u16 BE}{utf8 name}]*
peer → peer : [0x28 PeerRouteWithdraw][entryCount u16 BE][{id 16}]*
peer → peer : [0x29 PeerDeliverMessage][recipientId 16][senderId 16][body...]                        # needs len ≥ 33
peer → peer : [0x2A PeerDeliverGroupMessage][nameLen u16 BE][utf8 groupName][senderId 16][body...]   # needs len ≥ 19
peer → peer : [0x2B PeerDeliverTopicMessage][topicLen u16 BE][utf8 topic][senderId 16][body...]      # needs len ≥ 19
```

`PeerHello` mirrors `RegistrationRequest`'s own shape (a version range plus an opaque, unbounded
credential trailing the frame) rather than inventing a different negotiation pattern; `PeerHelloAck`
mirrors `RegistrationComplete`. Both `PeerRouteAdvertise`/`PeerRouteWithdraw` and
`PeerDeliverGroupMessage`/`PeerDeliverTopicMessage` are bounded the same way `FindClientsResponse`
(issue #38) is: entries stop being added the instant the frame would exceed
`StreamFramer.MaxPayloadSize`, via `EnqueueRouteAdvertise`, and the three `PeerDeliver*` builders are
each preceded by the same `ExceedsFrameCap`/`DropOversizeFanOut` guard `SendToGroup`/`PublishToTopic`
already use for their own local delivery frame, since a forwarded frame is larger than the one that
produced it in exactly the same way a fan-out delivery frame already is.

**Headerless only.** None of the three `PeerDeliver*` frames carry a header block — `RouteMessageWithHeaders`,
`SendToGroupWithHeaders` and `PublishToTopicWithHeaders` are not wired into peer forwarding at all. See
[known-issues.md](known-issues.md) KI-69.

**Loop prevention is structural, not a hop-count field.** A `PeerRouteAdvertise` this hub receives is only
ever written into its own `_remoteNames`/`_remoteIdsToPeer` tables, never re-sent to a different peer; a
`PeerDeliverMessage`/`PeerDeliverGroupMessage`/`PeerDeliverTopicMessage` this hub receives is only ever
delivered to a local recipient or dropped, never forwarded again. Nothing in the wire format needs a hop
count because nothing in the code ever takes a second hop. See [known-issues.md](known-issues.md) KI-70
for what this means for a federation of more than two hubs.

**Trust.** `senderId` in a `PeerDeliver*` frame is taken on trust — the receiving hub does not check it
against the sending peer's own advertised routes before handing it to a local recipient's
`MessageReceived`/`GroupMessageReceived`/`TopicMessageReceived`. See
[known-issues.md](known-issues.md) KI-68 and [hub.md](hub.md#hub-to-hub-federation) for the trust model
this is part of.

<a id="retained-message-header-issue-42"></a>

### Retained message header (issue #42)

Retained messages (see [client.md](client.md#retained-messages) and
[hub.md](hub.md#retained-messages)) ride the same header-envelope route as request/response, delivery
acknowledgement, expiry, backpressure and priority above — **no new opcode, no protocol version bump.**
One new well-known key, `Messages/RetainHeaderKeys.cs` (`internal`):

| Key | Value | Wire string | Present on |
|---|---|---|---|
| `RetainHeaderKeys.Retain` | `"1"` | `"mesh.retain"` | a `GroupMessageWithHeaders` (`0x13`) or `PublishTopicMessageWithHeaders` (`0x1C`) frame that asks the hub to retain it; also echoed on the hub's own `DeliverGroupMessageWithHeaders`/`DeliverTopicMessageWithHeaders` replay of a retained value to a version-5+ recipient |

**This is a fourth exception to "the hub only ever reads a header block's length, never its content"**
(after message expiry, backpressure and priority above) — `WantsRetain` uses the identical
`HeaderEnvelope.TryReadValue` single-key scan, called from `SendToGroupWithHeaders`/
`PublishToTopicWithHeaders` at the point a header-bearing group send or topic publish is processed. Unlike
those three, this is not the only mechanism that reads header content on this path any more — it is simply
the newest of a now-established pattern for a capability that needs the hub to notice one flag without
decoding the rest of the block.

**Retention has no reply/acknowledgement frame of its own.** The hub replays a retained value using the
*existing* delivery opcodes a live send already produces (`DeliverGroupMessage`/
`DeliverGroupMessageWithHeaders`, `DeliverTopicMessage`/`DeliverTopicMessageWithHeaders`) rather than a
dedicated replay frame — a recipient cannot distinguish a replay from a live send by opcode alone, only by
the presence of `mesh.retain=1` on the header-bearing shape, and only if it negotiated
`Protocol.HeaderEnvelopeMinVersion` or above. A recipient below that version still receives the retained
body on the plain `DeliverGroupMessage`/`DeliverTopicMessage` frame, with no way to tell it apart from an
ordinary send — the same "payload survives, header does not" treatment every other header-bearing frame
already gets for an older peer (see [Message headers](#message-headers) above).

**An empty body clears the retained value rather than storing an empty one.** There is no separate "clear"
opcode or header value — sending `retain: true` with a zero-length body is itself the clear signal,
mirroring MQTT's own retained-message semantics.

**Retention is capped independently of any existing group/topic-count limit.** A retained value over
`Protocol.MaxRetainedMessageBytes` (64 KiB) is refused; a group or topic newly starting to retain a value
is additionally refused once `Protocol.MaxRetainedGroupCount`/`MaxRetainedTopicCount` (10,000 each) already
hold one — see [hub.md](hub.md#retained-messages) for the full storage and cap mechanics, and
[known-issues.md](known-issues.md) KI-73 for the one disclosed race this feature carries: a topic subscribe
landing in a narrow window around a retained publish can receive a duplicate delivery of the same content
(never a loss — group-side retention has no equivalent window at all).

<a id="additive-opcodes-within-a-version"></a>

### Additive opcodes within a version

`GroupJoinRefused` was added **without** bumping the wire-protocol version (`Protocol.Version` at the
time; `Protocol.MaxSupportedVersion` today), and the reasoning is the rule to apply next time:

- It travels **hub → client only**, so it can never reach a hub that does not know it.
- An older client that receives one falls off the end of its dispatch ladder and **ignores** it (see
  [Length-guard behaviour](#length-guard-behaviour-why-malformed-frames-do-nothing) below), which is the
  same outcome as before the opcode existed.
- No existing frame's layout or meaning changed.

An opcode that fails any of those three — one a client may *send*, one that changes an existing layout,
or one whose absence changes behaviour a peer depends on — **must** bump `Protocol.MaxSupportedVersion`.
**`ResumeSession` (`0x16`, issue #43) is the clearest case of the first**: it travels client → hub, so
an older hub would drop it silently rather than degrade, which is why session resumption bumped the
version to `6` instead of taking this route. Its two siblings (`0x17`, `0x18`) travel hub → client and
would each have qualified on their own — but they only ever answer a `0x16`, so gating them separately
would mean nothing.

Note the membership requirement on group sends shipped in the same change and is **not** covered by
that reasoning: it is a behavioural change to `GroupMessage` (`0x0E`) handling with no version bump, so a
client written against an older hub that published to groups without joining them will silently stop
being delivered. That is a deliberate, documented break — see
[known-issues.md](known-issues.md) KI-2.

The four header-bearing opcodes (`0x11`–`0x14`) are the counter-example that **proves** the rule above
rather than an exception to it — see [Message headers](#message-headers) for why two of them travel
client → hub and so could not take the additive-opcode route.

**`QueueSaturated` (`0x15`, PR #87, issue #30) is a second confirming example of the same rule**,
alongside `GroupJoinRefused`: hub → client only, an older client that does not recognise it simply falls
off its dispatch ladder and ignores it (exactly as if the opcode did not exist, which for that client it
effectively does not), and no existing frame's layout changed. Unlike `GroupJoinRefused`, sending it at
all is itself opt-in (`notifyOnQueueSaturation`), so most hubs will never emit it regardless of what any
connected client's own version supports.

**Topic pub/sub (`0x19`–`0x1E`, issue #37) was, for one commit, the first addition to break this rule
rather than confirm it.** `DeliverTopicMessage`/`DeliverTopicMessageWithHeaders` (hub → client) are a
third confirming example, same shape as the two above. But `SubscribeTopic`/`UnsubscribeTopic`/
`PublishTopicMessage`/`PublishTopicMessageWithHeaders` travel client → hub — the exact case this section
says "must bump `Protocol.MaxSupportedVersion`", the same test `ResumeSession` was built to satisfy two
paragraphs up — and none of them got that treatment when the feature landed. The next commit, `fb2f9a0`,
corrected it: `Protocol.TopicPubSubMinVersion = 8` now gates all four, so this is a **fourth confirming
example** once its brief exception is accounted for. See
[Topic pub/sub frames](#topic-pubsub-frames-issue-37) above and
[known-issues.md](known-issues.md) KI-61 (fixed) for the history.

Lookup (correlated request/response):
```
ClientLookupRequest : [0x06][correlationId i32 BE][utf8 name]    # client→hub, needs len ≥ 5
ClientLookupResponse: [0x07][correlationId i32 BE][found u8][id 16 if found==1]  # needs len ≥ 6
```
`found == 0x01` **and** total length ≥ 22 → the 16-byte id follows; otherwise the client resolves the
lookup to `null` (`MeshClient.cs:1234-1241` — re-pointed this pass for PR #87's shift). The client only
completes a lookup whose correlation id matches the pending request (see [client.md](client.md)).

Control (no payload beyond the opcode):
```
Disconnect : [0x08]     # either direction; graceful close
Ping       : [0x09]     # hub→client liveness probe
Pong       : [0x0A]     # client→hub reply
```
`Ping`/`Pong` only exist when the hub is configured with a `heartbeatInterval`. The client replies to a
`Ping` best-effort (`MeshClient.cs:1259-1272` — re-pointed this pass for PR #87's shift); the hub treats
**any** received frame (including `Pong`) as proof of life via its activity counter, so a busy client is
never pinged.

Backpressure signalling (PR #87, issue #30):
```
QueueSaturated : [0x15][recipientId 16]   # hub→client; a direct send of the recipient's was dropped
```
Hub → client only, best-effort, sent only when the hub was constructed with `notifyOnQueueSaturation`
and only for a direct send (`RouteMessage`/`RouteMessageWithHeaders`) — never for a broadcast or group
drop. The client decodes it in the receive loop and raises `SendRejected`
(`MeshClient.cs:1243-1258` — see [client.md](client.md#backpressure-signalling)). See
[Additive opcodes within a version](#additive-opcodes-within-a-version) below for why this needed no
version bump.

Topic pub/sub (issue #37) — see [Topic pub/sub frames](#topic-pubsub-frames-issue-37) above for the full
byte layout:
```
SubscribeTopic                 : [0x19][utf8 pattern...]                                          # client→hub
UnsubscribeTopic                : [0x1A][utf8 pattern...]                                          # client→hub
PublishTopicMessage             : [0x1B][topicLen u16 BE][utf8 topic][body...]                     # client→hub, needs len ≥ 3
PublishTopicMessageWithHeaders  : [0x1C][topicLen u16 BE][utf8 topic][headerLen u16 BE][headerBlock][body...]  # client→hub, needs len ≥ 5
DeliverTopicMessage             : [0x1D][senderId 16][topicLen u16 BE][utf8 topic][body...]                    # hub→client, needs len ≥ 19
DeliverTopicMessageWithHeaders  : [0x1E][senderId 16][topicLen u16 BE][utf8 topic][headerLen u16 BE][headerBlock][body...]  # hub→client, needs len ≥ 21
```
Unlike every block above it in this section, the two client → hub opcodes here (`0x19`/`0x1A`, plus
`0x1B`/`0x1C`) needed a version gate by this file's own rule — for one commit they shipped without one,
then commit `fb2f9a0` added `Protocol.TopicPubSubMinVersion` to cover all four. See
[Additive opcodes within a version](#additive-opcodes-within-a-version) below and
[known-issues.md](known-issues.md) KI-61 (fixed).

---

## Length-guard behaviour (why malformed frames "do nothing")

Both dispatch chains are length-guarded `if / else if` ladders with **no terminal `else`**
(`MeshHub.cs:1081-1199`, `MeshClient.cs:1057-1278`). A frame that is too short for its opcode, or carries an
unrecognised opcode, **falls through and is silently ignored** — no exception, no log at warning level.
When debugging "my message never arrives", suspect a framing/offset error first; it will not surface as
an error. If you add an opcode, add both the guard and the branch on the correct side, and mirror the
exact offsets above. PR #74's four header-bearing opcodes each added one more `else if` to both ladders
rather than changing an existing branch, growing the client's ladder by 188 lines (was 121) and the
hub's by 118 (was 77) — the length-guard style scales additively, which is what makes it the right shape
for a change like this one. **Neither PR #83 nor PR #84 added a branch** to the client's ladder — each
nested a check *inside* the existing `DeliverMessageWithHeaders` branch: PR #83's
`if (!TryCompletePendingRequest(...))`, and PR #84's `TryCompletePendingAck(...)` ahead of it in the same
condition (`MeshClient.cs:1118-1120`, see [client.md](client.md#request-response) and
[client.md](client.md#delivery-acknowledgement)), so the ladder still has exactly the same number of
`else if` branches after both PRs; only that one branch's body grew, twice. **PR #87 goes back to adding a
genuine new branch** — `QueueSaturated` (`0x15`) is a distinct opcode, not a nested check inside an
existing one, so the client's ladder gained its own `else if` (`MeshClient.cs:1243-1258`) the same way
PR #74's four opcodes did, growing the ladder by one branch rather than widening an existing condition.
**Issue #37's six topic opcodes add six more genuine branches to each ladder** (hub:
`MeshHub.cs:1483-1549`; client: `MeshClient.cs:1947-2021` for the two delivery branches, plus
`SubscribeAsync`/`UnsubscribeAsync`/`PublishAsync` on the send side) — the additive, one-branch-per-opcode
shape scales the same way regardless of whether the opcode should also have bumped the version. The hub's
four topic dispatch branches additionally each gained a `NegotiatedProtocolVersion` guard, one commit
after the opcodes themselves landed (see
[Additive opcodes within a version](#additive-opcodes-within-a-version) below for the four that briefly
should have had one and did not).

That fall-through is what makes a hub → client opcode addable without a version bump: the client's
`GroupJoinRefused` branch (`MeshClient.cs:1195-1220`) guards on `data.Length > 1`, so a refusal carrying an
**empty** name — which a hub will never send, since `JoinGroupAsync` returns early on an empty name
(`MeshHub.cs:2085`) — would itself fall through and be ignored. `QueueSaturated`'s own guard
(`data.Length >= 17`) works the same way for the same reason.

The **registration frame follows the same rule**: a truncated frame, a zero name length, or a declared
name length running past the payload drops the connection with **no error frame** (`MeshHub.cs:971-984`).
A client with a bad framing bug therefore sees the connection close rather than a
`RegistrationRefusedException` — do not read a silent close as "hub unreachable".

## Versioning

Version negotiation gates the handshake only; there is no per-message version tag on the wire — a
version-gated capability is instead gated by **opcode** (does this peer even send/recognise it) plus, on
the hub, by **`ClientConnection.NegotiatedProtocolVersion`** (does *this* peer's own negotiated version
support it). `Protocol.cs` (`Messages/Protocol.cs`) declares `MinSupportedVersion` (`4`) and
`MaxSupportedVersion` (`8`) bounding the range this build of the hub/client will speak, plus
`HeaderEnvelopeMinVersion` (`5`) marking the version at which the header envelope became available,
`SessionResumptionMinVersion` (`6`) marking the same for session resumption,
`SessionResumedGroupsMinVersion` (`7`, PR #135/issue #109) marking the version at which a `SessionResumed`
reply reports the group memberships the hub actually restored, and `TopicPubSubMinVersion` (`8`, commit
`fb2f9a0`) marking the version at which topic pub/sub's four client → hub opcodes may be used — see
[Session resumption](#session-resumption) above and
[Topic pub/sub frames](#topic-pubsub-frames-issue-37) below.
`MeshClient.ConnectAsync` always advertises its own `[MinSupportedVersion, MaxSupportedVersion]`;
`MeshHub.TryNegotiateProtocolVersion` (`MeshHub.cs:1381-1403`) intersects that with its own range and, on
overlap, picks the **highest** version common to both — a peer never has to downgrade further than
necessary. A malformed range (`versionMin > versionMax`) or a non-overlapping one refuses with
`Error(UnsupportedProtocolVersion)`.

Negotiation was introduced by PR #73 (issue #47), replacing a single `Protocol.Version` equality check,
but at the time both bounds were `4` and **nothing read the negotiated version except the logger** — see
the history in [known-issues.md](known-issues.md) KI-14. PR #74 (issue #32) is what actually exercises
the mechanism:

- **`MaxSupportedVersion` widened from `4` to `5`** to admit the header envelope, while
  `MinSupportedVersion` stayed at `4` — a hub or client that only understands `4` keeps interoperating
  with one that understands `5`; negotiation settles on `4`, and the peer that only sent the plain frames
  in the first place notices nothing.
- **`MeshHub.ClientConnection` now records its own `NegotiatedProtocolVersion`**, captured once at
  registration (`MeshHub.cs:2541`, constructor parameter, immutable thereafter) — the piece KI-14 said was
  missing. `RouteMessageWithHeaders` (`MeshHub.cs:1924`) and `SendToGroupWithHeaders`
  (`MeshHub.cs:2416`) — see [hub.md](hub.md#routing-helpers) — read it per-recipient to decide whether to
  forward a header-bearing frame unchanged or strip it to the plain equivalent.
- **`MeshClient` reads its own `NegotiatedProtocolVersion` too**, refusing with `NotSupportedException`
  before it will send a non-empty `MessageHeaders` over a connection negotiated below
  `HeaderEnvelopeMinVersion` — see [Message headers](#message-headers).

**Topic pub/sub (issue #37) was, for one commit, the first capability since PR #73 introduced real
negotiation to add opcodes a peer may *send* without widening `MaxSupportedVersion` or adding a
`Protocol.XyzMinVersion` constant at all.** For that one commit, `SubscribeAsync`/`UnsubscribeAsync`/the
headerless `PublishAsync` never read `NegotiatedProtocolVersion`; the header-bearing `PublishAsync`
overload read it only to guard the header block via the pre-existing `RequireHeaderEnvelopeSupport`,
which says nothing about whether the *topic* opcodes themselves are understood on the other end. This was
the pattern this section says **not** to follow — "do not assume a version bump alone makes a change
safe" cuts the other way here: the change needed the bump and did not take it. The very next commit,
`fb2f9a0`, corrected it — see [known-issues.md](known-issues.md) KI-61 (fixed) for the history.

This is the pattern to imitate for the next optional capability: widen `MaxSupportedVersion`, add a
`Protocol.XyzMinVersion` marking where it becomes available, and have **both** hub and client consult
`NegotiatedProtocolVersion` before doing anything the other side might not understand — do not assume a
version bump alone makes a change safe; something on both sides has to actually read the number.
**Issue #43 is the second capability to follow it**, and adds one further lesson: the gate only works
for something sent *after* the negotiated version is known. Anything that would have to travel in the
registration frame itself cannot be version-gated at all, because neither end knows the version yet —
restructure it into a later exchange rather than trying to make the frame conditional. **Topic pub/sub's
fix, commit `fb2f9a0`, is a third example of the pattern once fully applied** — `Protocol.TopicPubSubMinVersion`
plus `RequireTopicPubSubSupport` on the client and a `NegotiatedProtocolVersion` check on each of the
hub's four dispatch branches — and its own history (KI-61) is the cautionary counter-example for why
"added in the same commit as the opcodes" matters, not just "added eventually".

Any backward-incompatible change to the frames above must still bump `MaxSupportedVersion` (and, if the
old shape can no longer be produced or understood at all, `MinSupportedVersion` too)
([index §6](../for-clanker.md#6-cross-cutting-conventions-imitate-these) lists the add-a-message-type
checklist).

A **hub → client** opcode that no existing peer depends on can be added without a bump — that is the
precedent `GroupJoinRefused` set, and the conditions it had to meet are spelled out under
[Additive opcodes](#additive-opcodes-within-a-version). Do not read that precedent as "opcode additions
are free": it turns on the direction of travel and on old peers being unaffected by the frame's absence.
