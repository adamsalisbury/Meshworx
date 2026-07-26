# Wire protocol & message framing

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [transport.md](transport.md)

This is the contract on the wire. Get it wrong and frames are silently dropped rather than rejected, so
document/verify carefully when you touch it. Everything here is read from
`src/AdamSalisbury.Meshworx/Messages/MessageType.cs`, `Messages/Protocol.cs`, and the encode/decode
sites in `MeshHub.cs` / `MeshClient.cs`.

- **Protocol version is a negotiated range** (`Protocol.MinSupportedVersion` = `4`,
  `Protocol.MaxSupportedVersion` = `5`, `Messages/Protocol.cs:8`, `:14`). The client advertises the range
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
  bumped the version instead — see the note after the opcode table).
- **`MessageType` and `Protocol` are `internal`** — opcodes are not visible outside the assembly.
- **Byte order:** big-endian for all multi-byte integers (`BinaryPrimitives.*BigEndian`). Ids are
  16-byte `Guid`s written with `Guid.TryWriteBytes` / read with `new Guid(span)`.

---

## Two layers: framing vs message

1. **Transport framing** (owned by the transport). For `TcpTransport`: `[4-byte big-endian length N][N
   payload bytes]`, `N ≤ 1 MiB`. `WebSocketTransport` (PR #78, issue #18) needs no length prefix at all —
   one WebSocket binary message carries exactly one frame, since the WebSocket protocol already delimits
   messages; it enforces the identical 1 MiB cap independently, on both send and receive. `InMemoryTransport`
   likewise uses channel boundaries — no length prefix. Whatever the transport, the hub/client never see
   its framing; they receive one **message payload** per `ReceiveAsync`, and everything below this line is
   that payload, unchanged by which transport carried it. See [transport.md](transport.md) for each
   transport's framing in full.
2. **Message** (owned by hub/client). The **first payload byte is the opcode** (`MessageType`); the rest
   is opcode-specific. Empty frames (length 0) are ignored, not decoded (`MeshHub.cs:1009`,
   `MeshClient.cs:723`).

Everything in the tables below is the **message payload** (i.e. after the transport's framing header).

---

## Opcodes (`MessageType`, `Messages/MessageType.cs`)

| Name | Byte | Direction | Payload after the opcode byte |
|---|---|---|---|
| `RegistrationComplete` | `0x01` | hub → client | assigned client id (16), negotiated protocol version (1) |
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

`0x14` is the highest opcode in use; the next new one is `0x15`. The four header-bearing opcodes
(`0x11`–`0x14`, PR #74, issue #32) are each the existing opcode's frame with one extra
`[headerBlockLength(2, BE)][headerBlock]` pair spliced in right after the fields that address the
message (recipient id / group name) and before the opaque message body — see
[Message headers](#message-headers) below for why these needed a version bump rather than the
additive-opcode route.

**`RegistrationErrorCode`** (`RegistrationErrorCode.cs`, sent as the byte after `Error`):
`DuplicateClientName=0x01`, `UnsupportedProtocolVersion=0x02`, `ClientNameTooLong=0x03`,
`HubAtCapacity=0x04`, `AuthenticationFailed=0x05` (`RegistrationErrorCode.cs:31`).

---

## Registration handshake

```
client → hub : [0x04 RegistrationRequest][versionMin][versionMax][nameLen u16 BE][utf8 clientName][credential...]
hub → client : [0x01 RegistrationComplete][clientId (16 bytes)][negotiatedVersion]  # success
             | [0x05 Error][errorCode]                                             # refused
```

`versionMin`/`versionMax` is the client's supported range; `MeshClient` always sends
`Protocol.MinSupportedVersion`/`Protocol.MaxSupportedVersion` (`4`/`5` as of PR #74), so a hub and client
both built from this codebase negotiate `5`, but the wire and the hub's negotiation both treat it as a
real range — a client built against an older copy of the library (advertising `4`/`4`) still
interoperates, negotiating down to `4` and losing only the header envelope — see
[Versioning](#versioning). The **credential is everything after the name** — its length is implied by
the frame length, so it can be empty (the default). The hub does not interpret those bytes; it hands
them to the configured `ClientAuthenticator` and nothing else reads them. See
[hub.md](hub.md#authentication) and [types.md](types.md#authentication-types).

Hub-side validation order (`MeshHub.cs:890-984`), each failure sends the error (if applicable) and drops
the connection:
1. Frame ≥ **3** bytes and opcode `0x04` — else drop silently (no error frame) (`:890-895`).
2. **Negotiate a version** via `TryNegotiateProtocolVersion(versionMin, versionMax, out negotiatedVersion)`
   (`MeshHub.cs:1314-1336`, called at `:897`) — else `Error(UnsupportedProtocolVersion)` (`:897-903`).
   Negotiation fails if `versionMin > versionMax` (an inverted range) or if `[versionMin, versionMax]`
   does not overlap `[Protocol.MinSupportedVersion, Protocol.MaxSupportedVersion]`; otherwise it picks the
   **highest version common to both ranges**. **This is checked before the length checks below**, so a
   3-byte frame carrying an unsupported range still gets an error reply.
3. Frame ≥ 5 bytes, i.e. long enough to carry the name length — else drop silently (`:905-909`).
4. `nameLen != 0` **and** frame ≥ `5 + nameLen` — else drop silently (`:911-918`). A declared length of
   zero, or one running past the payload, is treated as malformed: **no error frame, connection
   dropped**. The empty name is refused here rather than admitted so it cannot reserve the empty string
   in the name registry.
5. Decode the name from bytes `[5, 5+nameLen)`; `clientName.Length ≤ 256` **chars** — else
   `Error(ClientNameTooLong)` (`:922-928`).
6. **Authentication**, only when an authenticator was configured (`:930-951`). Two parts, in order: an
   at-capacity **early-out** — already-claimed slots `>= maxClients` → `Error(HubAtCapacity)` without the
   callback running (`:937-941`) — then the callback itself, given the name and credential
   (`:943-950`). Refusal, throw, cancellation or timeout → `Error(AuthenticationFailed)`.
7. **Capacity claim** (`:958-962`): one atomic compare-and-swap takes a client slot if and only if fewer
   than `maxClients` are claimed. Failure → `Error(HubAtCapacity)`. This, not the early-out in step 6 and
   not the registered client count, is the decision that admits or refuses on capacity, so concurrent
   registrations cannot all pass and overshoot the cap.
8. Name not already claimed (`_clientNames.TryAdd`) — else `Error(DuplicateClientName)` (`:966-971`).
9. Send `RegistrationComplete` carrying the assigned id and the `negotiatedVersion` byte from step 2
   (`:977-981`).

Note that the **binding** capacity decision happens **after** authentication — an unauthenticated peer
cannot hold a slot away from one that would authenticate — and **before** the name is reserved, so a
client refused on either count never claims a name. The early-out in step 6 preserves the separate
property that a full hub never runs the callback.

Client-side (`MeshClient.cs:205-221`): an `Error` reply → `RegistrationRefusedException(errorCode)`; any
reply that isn't exactly an 18-byte `RegistrationComplete` → `InvalidOperationException`. On success the
trailing byte is read into `IMeshClient.NegotiatedProtocolVersion` (`MeshClient.cs:225`,
`IMeshClient.cs:28`) — `0` whenever the client is not connected.

A connection that never sends a valid registration within `registrationTimeout` (default 10 s) is
dropped without an error frame (`MeshHub.cs:878-886`).

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
broadcast from a direct message (`MeshHub.BroadcastMessage` builds a `0x03` frame, `MeshHub.cs:1552`):
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
outbound `DeliverGroupMessage` rather than re-encoding the decoded string (`MeshHub.cs:1060-1067`,
`:2011-2016`). The header-bearing group frames do the same, and additionally pass the header block
through as an opaque `ReadOnlyMemory<byte>` — the hub reads only its **length**, never its content, on
both the direct and group paths (`RouteMessageWithHeaders`/`SendToGroupWithHeaders`, see
[hub.md](hub.md#routing-helpers)).

<a id="message-headers"></a>

### Message headers

`MessageHeaders` (`Messages/MessageHeaders.cs`) is a small, immutable, string-keyed
`IReadOnlyDictionary<string, string>` that travels alongside a message body without the hub ever
decoding it. `MessageHeaders.Empty` is the shared no-headers instance; `SendAsync`/`SendToGroupAsync`
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
span-slice exception escape on a malformed block; the hub never calls it (it only reads the block's
length to route/strip), but `MeshClient` does, on receipt of `DeliverMessageWithHeaders`/
`DeliverGroupMessageWithHeaders`. There, `TryReadHeaderBlock` (`MeshClient.cs:1115-1127`) catches the
`FormatException`, logs a warning, and drops **only that one frame** rather than tearing down the
connection — the same "one bad frame must not kill the loop" principle as the rest of the receive loop
(see [Length-guard behaviour](#length-guard-behaviour-why-malformed-frames-do-nothing)).

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
the inbound `JoinGroup` name bytes and replies with exactly those (`RefuseGroupJoin`, `MeshHub.cs:1874`,
copy at `:1746`, echo at `:1884-1886`). Re-encoding the *decoded* string is not size-preserving: every
byte that is not valid UTF-8 decodes to `U+FFFD` and re-encodes as three, so a name of invalid bytes
would **triple**. Join frames carry no length cap of their own (KI-8), so a re-encoded refusal could
exceed the transport's 1 MiB payload limit and throw on send — which faults that connection's send loop,
and the faulted send loop is awaited during teardown, abandoning the rest of it including the release of
the client's capacity slot. Echoing keeps the refusal no larger than the frame that provoked it, which
the transport has already bounded. If you touch this path, keep the echo.

**Group sends require membership.** The hub silently drops a `GroupMessage` from a client that has not
joined the target group (`MeshHub.cs:1983`, `:1993-1999`). There is no error frame for this — a correct
client only sends to groups it has joined, and it learns that a join did *not* take effect from
`GroupJoinRefused`. See [hub.md](hub.md#group-authorisation) and
[known-issues.md](known-issues.md) KI-2.

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

Note the membership requirement on group sends shipped in the same change and is **not** covered by
that reasoning: it is a behavioural change to `GroupMessage` (`0x0E`) handling with no version bump, so a
client written against an older hub that published to groups without joining them will silently stop
being delivered. That is a deliberate, documented break — see
[known-issues.md](known-issues.md) KI-2.

The four header-bearing opcodes (`0x11`–`0x14`) are the counter-example that **proves** the rule above
rather than an exception to it — see [Message headers](#message-headers) for why two of them travel
client → hub and so could not take the additive-opcode route.

Lookup (correlated request/response):
```
ClientLookupRequest : [0x06][correlationId i32 BE][utf8 name]    # client→hub, needs len ≥ 5
ClientLookupResponse: [0x07][correlationId i32 BE][found u8][id 16 if found==1]  # needs len ≥ 6
```
`found == 0x01` **and** total length ≥ 22 → the 16-byte id follows; otherwise the client resolves the
lookup to `null` (`MeshClient.cs:717-724`). The client only completes a lookup whose correlation id
matches the pending request (see [client.md](client.md)).

Control (no payload beyond the opcode):
```
Disconnect : [0x08]     # either direction; graceful close
Ping       : [0x09]     # hub→client liveness probe
Pong       : [0x0A]     # client→hub reply
```
`Ping`/`Pong` only exist when the hub is configured with a `heartbeatInterval`. The client replies to a
`Ping` best-effort (`MeshClient.cs:726-737`); the hub treats **any** received frame (including `Pong`)
as proof of life via its activity counter, so a busy client is never pinged.

---

## Length-guard behaviour (why malformed frames "do nothing")

Both dispatch chains are length-guarded `if / else if` ladders with **no terminal `else`**
(`MeshHub.cs:1015-1132`, `MeshClient.cs:807-995`). A frame that is too short for its opcode, or carries an
unrecognised opcode, **falls through and is silently ignored** — no exception, no log at warning level.
When debugging "my message never arrives", suspect a framing/offset error first; it will not surface as
an error. If you add an opcode, add both the guard and the branch on the correct side, and mirror the
exact offsets above. PR #74's four header-bearing opcodes each added one more `else if` to both ladders
rather than changing an existing branch, growing the client's ladder by 188 lines (was 121) and the
hub's by 118 (was 77) — the length-guard style scales additively, which is what makes it the right shape
for a change like this one.

That fall-through is what makes a hub → client opcode addable without a version bump: the client's
`GroupJoinRefused` branch (`MeshClient.cs:928-953`) guards on `data.Length > 1`, so a refusal carrying an
**empty** name — which a hub will never send, since `JoinGroupAsync` returns early on an empty name
(`MeshHub.cs:1727`) — would itself fall through and be ignored.

The **registration frame follows the same rule**: a truncated frame, a zero name length, or a declared
name length running past the payload drops the connection with **no error frame** (`MeshHub.cs:905-918`).
A client with a bad framing bug therefore sees the connection close rather than a
`RegistrationRefusedException` — do not read a silent close as "hub unreachable".

## Versioning

Version negotiation gates the handshake only; there is no per-message version tag on the wire — a
version-gated capability is instead gated by **opcode** (does this peer even send/recognise it) plus, on
the hub, by **`ClientConnection.NegotiatedProtocolVersion`** (does *this* peer's own negotiated version
support it). `Protocol.cs` (`Messages/Protocol.cs`) declares `MinSupportedVersion` (`4`) and
`MaxSupportedVersion` (`5`) bounding the range this build of the hub/client will speak, plus
`HeaderEnvelopeMinVersion` (`5`) marking the version at which the header envelope became available.
`MeshClient.ConnectAsync` always advertises its own `[MinSupportedVersion, MaxSupportedVersion]`;
`MeshHub.TryNegotiateProtocolVersion` (`MeshHub.cs:1314-1336`) intersects that with its own range and, on
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
  registration (`MeshHub.cs:2177`, constructor parameter, immutable thereafter) — the piece KI-14 said was
  missing. `RouteMessageWithHeaders` (`MeshHub.cs:1609`) and `SendToGroupWithHeaders`
  (`MeshHub.cs:2055`) — see [hub.md](hub.md#routing-helpers) — read it per-recipient to decide whether to
  forward a header-bearing frame unchanged or strip it to the plain equivalent.
- **`MeshClient` reads its own `NegotiatedProtocolVersion` too**, refusing with `NotSupportedException`
  before it will send a non-empty `MessageHeaders` over a connection negotiated below
  `HeaderEnvelopeMinVersion` — see [Message headers](#message-headers).

This is the pattern to imitate for the next optional capability: widen `MaxSupportedVersion`, add a
`Protocol.XyzMinVersion` marking where it becomes available, and have **both** hub and client consult
`NegotiatedProtocolVersion` before doing anything the other side might not understand — do not assume a
version bump alone makes a change safe; something on both sides has to actually read the number.

Any backward-incompatible change to the frames above must still bump `MaxSupportedVersion` (and, if the
old shape can no longer be produced or understood at all, `MinSupportedVersion` too)
([index §6](../for-clanker.md#6-cross-cutting-conventions-imitate-these) lists the add-a-message-type
checklist).

A **hub → client** opcode that no existing peer depends on can be added without a bump — that is the
precedent `GroupJoinRefused` set, and the conditions it had to meet are spelled out under
[Additive opcodes](#additive-opcodes-within-a-version). Do not read that precedent as "opcode additions
are free": it turns on the direction of travel and on old peers being unaffected by the frame's absence.
