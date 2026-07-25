# Wire protocol & message framing

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [transport.md](transport.md)

This is the contract on the wire. Get it wrong and frames are silently dropped rather than rejected, so
document/verify carefully when you touch it. Everything here is read from
`src/AdamSalisbury.Meshworx/Messages/MessageType.cs`, `Messages/Protocol.cs`, and the encode/decode
sites in `MeshHub.cs` / `MeshClient.cs`.

- **Protocol version:** `3` (`Protocol.Version`, `Messages/Protocol.cs:5`). Version `3` changed the
  `RegistrationRequest` layout to carry a length-prefixed name plus an opaque credential — see
  [Registration handshake](#registration-handshake). Nothing else on the wire changed.
  `GroupJoinRefused` (`0x10`) was added **within** version 3 and did **not** bump it — see
  [Additive opcodes](#additive-opcodes-within-a-version) for why that is sound and when it is not.
- **`MessageType` and `Protocol` are `internal`** — opcodes are not visible outside the assembly.
- **Byte order:** big-endian for all multi-byte integers (`BinaryPrimitives.*BigEndian`). Ids are
  16-byte `Guid`s written with `Guid.TryWriteBytes` / read with `new Guid(span)`.

---

## Two layers: framing vs message

1. **Transport framing** (owned by the transport). For `TcpTransport`: `[4-byte big-endian length N][N
   payload bytes]`, `N ≤ 1 MiB`. `InMemoryTransport` uses channel boundaries — no length prefix. The
   hub/client never see the length prefix; they receive one **message payload** per `ReceiveAsync`.
2. **Message** (owned by hub/client). The **first payload byte is the opcode** (`MessageType`); the rest
   is opcode-specific. Empty frames (length 0) are ignored, not decoded (`MeshHub.cs:675`,
   `MeshClient.cs:546`).

Everything in the tables below is the **message payload** (i.e. after the transport's framing header).

---

## Opcodes (`MessageType`, `Messages/MessageType.cs`)

| Name | Byte | Direction | Payload after the opcode byte |
|---|---|---|---|
| `RegistrationComplete` | `0x01` | hub → client | assigned client id (16) |
| `SendMessage` | `0x02` | client → hub | recipient id (16), message bytes |
| `DeliverMessage` | `0x03` | hub → client | sender id (16), message bytes |
| `RegistrationRequest` | `0x04` | client → hub | version (1), name length (2, BE), UTF-8 name, opaque credential (rest of frame) |
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

`0x10` is the highest opcode in use; the next new one is `0x11`.

**`RegistrationErrorCode`** (`RegistrationErrorCode.cs`, sent as the byte after `Error`):
`DuplicateClientName=0x01`, `UnsupportedProtocolVersion=0x02`, `ClientNameTooLong=0x03`,
`HubAtCapacity=0x04`, `AuthenticationFailed=0x05` (`RegistrationErrorCode.cs:31`).

---

## Registration handshake

```
client → hub : [0x04 RegistrationRequest][version=3][nameLen u16 BE][utf8 clientName][credential...]
hub → client : [0x01 RegistrationComplete][clientId (16 bytes)]      # success
             | [0x05 Error][errorCode]                               # refused
```

The **credential is everything after the name** — its length is implied by the frame length, so it can
be empty (the default). The hub does not interpret those bytes; it hands them to the configured
`ClientAuthenticator` and nothing else reads them. See [hub.md](hub.md#authentication) and
[types.md](types.md#authentication-types).

Hub-side validation order (`MeshHub.cs:558-650`), each failure sends the error (if applicable) and drops
the connection:
1. Frame ≥ **2** bytes and opcode `0x04` — else drop silently (no error frame) (`:558-563`).
2. `version == 3` — else `Error(UnsupportedProtocolVersion)` (`:565-571`). **This is checked before the
   length checks below**, so a 2-byte frame carrying the wrong version still gets an error reply.
3. Frame ≥ 4 bytes, i.e. long enough to carry the name length — else drop silently (`:573-577`).
4. `nameLen != 0` **and** frame ≥ `4 + nameLen` — else drop silently (`:579-586`). A declared length of
   zero, or one running past the payload, is treated as malformed: **no error frame, connection
   dropped**. The empty name is refused here rather than admitted so it cannot reserve the empty string
   in the name registry.
5. Decode the name from bytes `[4, 4+nameLen)`; `clientName.Length ≤ 256` **chars** — else
   `Error(ClientNameTooLong)` (`:590-596`).
6. **Authentication**, only when an authenticator was configured (`:598-619`). Two parts, in order: an
   at-capacity **early-out** — already-claimed slots `>= maxClients` → `Error(HubAtCapacity)` without the
   callback running (`:605-609`) — then the callback itself, given the name and credential
   (`:611-618`). Refusal, throw, cancellation or timeout → `Error(AuthenticationFailed)`.
7. **Capacity claim** (`:626-630`): one atomic compare-and-swap takes a client slot if and only if fewer
   than `maxClients` are claimed. Failure → `Error(HubAtCapacity)`. This, not the early-out in step 6 and
   not the registered client count, is the decision that admits or refuses on capacity, so concurrent
   registrations cannot all pass and overshoot the cap.
8. Name not already claimed (`_clientNames.TryAdd`) — else `Error(DuplicateClientName)` (`:634-639`).

Note that the **binding** capacity decision happens **after** authentication — an unauthenticated peer
cannot hold a slot away from one that would authenticate — and **before** the name is reserved, so a
client refused on either count never claims a name. The early-out in step 6 preserves the separate
property that a full hub never runs the callback.

Client-side (`MeshClient.cs:126-163`): an `Error` reply → `RegistrationRefusedException(errorCode)`; any
reply that isn't exactly a 17-byte `RegistrationComplete` → `InvalidOperationException`.

A connection that never sends a valid registration within `registrationTimeout` (default 10 s) is
dropped without an error frame (`MeshHub.cs:546-554`).

---

## Message frames (exact byte offsets)

Direct send / deliver:
```
SendMessage       : [0x02][recipientId 16][body...]              # client→hub, needs len ≥ 17
DeliverMessage    : [0x03][senderId 16][body...]                 # hub→client, needs len ≥ 17
```
Broadcast is sent as `BroadcastMessage` but **delivered as `DeliverMessage`** — recipients cannot tell a
broadcast from a direct message (`MeshHub.BroadcastMessage` builds a `0x03` frame, `MeshHub.cs:1137`):
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
```
The hub passes the original name bytes straight through from the inbound `GroupMessage` into the
outbound `DeliverGroupMessage` rather than re-encoding the decoded string (`MeshHub.cs:713-719`,
`:1458-1463`).

**`GroupJoinRefused` echoes the same bytes, and that is load-bearing rather than tidy.** The hub copies
the inbound `JoinGroup` name bytes and replies with exactly those (`RefuseGroupJoin`, `MeshHub.cs:1321`,
copy at `:1193`, echo at `:1331-1333`). Re-encoding the *decoded* string is not size-preserving: every
byte that is not valid UTF-8 decodes to `U+FFFD` and re-encodes as three, so a name of invalid bytes
would **triple**. Join frames carry no length cap of their own (KI-8), so a re-encoded refusal could
exceed the transport's 1 MiB payload limit and throw on send — which faults that connection's send loop,
and the faulted send loop is awaited during teardown, abandoning the rest of it including the release of
the client's capacity slot. Echoing keeps the refusal no larger than the frame that provoked it, which
the transport has already bounded. If you touch this path, keep the echo.

**Group sends require membership.** The hub silently drops a `GroupMessage` from a client that has not
joined the target group (`MeshHub.cs:1430`, `:1440-1446`). There is no error frame for this — a correct
client only sends to groups it has joined, and it learns that a join did *not* take effect from
`GroupJoinRefused`. See [hub.md](hub.md#group-authorisation) and
[known-issues.md](known-issues.md) KI-2.

<a id="additive-opcodes-within-a-version"></a>

### Additive opcodes within a version

`GroupJoinRefused` was added **without** bumping `Protocol.Version`, and the reasoning is the rule to
apply next time:

- It travels **hub → client only**, so it can never reach a hub that does not know it.
- An older client that receives one falls off the end of its dispatch ladder and **ignores** it (see
  [Length-guard behaviour](#length-guard-behaviour-why-malformed-frames-do-nothing) below), which is the
  same outcome as before the opcode existed.
- No existing frame's layout or meaning changed.

An opcode that fails any of those three — one a client may *send*, one that changes an existing layout,
or one whose absence changes behaviour a peer depends on — **must** bump `Protocol.Version`.

Note the membership requirement on group sends shipped in the same change and is **not** covered by
that reasoning: it is a behavioural change to `GroupMessage` (`0x0E`) handling with no version bump, so a
client written against an older hub that published to groups without joining them will silently stop
being delivered. That is a deliberate, documented break — see
[known-issues.md](known-issues.md) KI-2.

Lookup (correlated request/response):
```
ClientLookupRequest : [0x06][correlationId i32 BE][utf8 name]    # client→hub, needs len ≥ 5
ClientLookupResponse: [0x07][correlationId i32 BE][found u8][id 16 if found==1]  # needs len ≥ 6
```
`found == 0x01` **and** total length ≥ 22 → the 16-byte id follows; otherwise the client resolves the
lookup to `null` (`MeshClient.cs:639-646`). The client only completes a lookup whose correlation id
matches the pending request (see [client.md](client.md)).

Control (no payload beyond the opcode):
```
Disconnect : [0x08]     # either direction; graceful close
Ping       : [0x09]     # hub→client liveness probe
Pong       : [0x0A]     # client→hub reply
```
`Ping`/`Pong` only exist when the hub is configured with a `heartbeatInterval`. The client replies to a
`Ping` best-effort (`MeshClient.cs:648-659`); the hub treats **any** received frame (including `Pong`)
as proof of life via its activity counter, so a busy client is never pinged.

---

## Length-guard behaviour (why malformed frames "do nothing")

Both dispatch chains are length-guarded `if / else if` ladders with **no terminal `else`**
(`MeshHub.cs:681-756`, `MeshClient.cs:714-835`). A frame that is too short for its opcode, or carries an
unrecognised opcode, **falls through and is silently ignored** — no exception, no log at warning level.
When debugging "my message never arrives", suspect a framing/offset error first; it will not surface as
an error. If you add an opcode, add both the guard and the branch on the correct side, and mirror the
exact offsets above.

That fall-through is what makes a hub → client opcode addable without a version bump: the client's
`GroupJoinRefused` branch (`MeshClient.cs:768-793`) guards on `data.Length > 1`, so a refusal carrying an
**empty** name — which a hub will never send, since `JoinGroupAsync` returns early on an empty name
(`MeshHub.cs:1174`) — would itself fall through and be ignored.

The **registration frame follows the same rule**: a truncated frame, a zero name length, or a declared
name length running past the payload drops the connection with **no error frame** (`MeshHub.cs:573-586`).
A client with a bad framing bug therefore sees the connection close rather than a
`RegistrationRefusedException` — do not read a silent close as "hub unreachable".

## Versioning

`Protocol.Version` gates the handshake only; there is no per-message version. A client and hub must
agree on version `3` or registration is refused with `UnsupportedProtocolVersion`. Any backward-
incompatible change to the frames above must bump `Protocol.Version`
([index §6](../for-clanker.md#6-cross-cutting-conventions-imitate-these) lists the add-a-message-type
checklist).

A **hub → client** opcode that no existing peer depends on can be added without a bump — that is the
precedent `GroupJoinRefused` set, and the conditions it had to meet are spelled out under
[Additive opcodes](#additive-opcodes-within-a-version). Do not read that precedent as "opcode additions
are free": it turns on the direction of travel and on old peers being unaffected by the frame's absence.
