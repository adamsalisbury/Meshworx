# Wire protocol & message framing

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [transport.md](transport.md)

This is the contract on the wire. Get it wrong and frames are silently dropped rather than rejected, so
document/verify carefully when you touch it. Everything here is read from
`src/AdamSalisbury.Meshworx/Messages/MessageType.cs`, `Messages/Protocol.cs`, and the encode/decode
sites in `MeshHub.cs` / `MeshClient.cs`.

- **Protocol version:** `2` (`Protocol.Version`, `Messages/Protocol.cs:5`).
- **`MessageType` and `Protocol` are `internal`** — opcodes are not visible outside the assembly.
- **Byte order:** big-endian for all multi-byte integers (`BinaryPrimitives.*BigEndian`). Ids are
  16-byte `Guid`s written with `Guid.TryWriteBytes` / read with `new Guid(span)`.

---

## Two layers: framing vs message

1. **Transport framing** (owned by the transport). For `TcpTransport`: `[4-byte big-endian length N][N
   payload bytes]`, `N ≤ 1 MiB`. `InMemoryTransport` uses channel boundaries — no length prefix. The
   hub/client never see the length prefix; they receive one **message payload** per `ReceiveAsync`.
2. **Message** (owned by hub/client). The **first payload byte is the opcode** (`MessageType`); the rest
   is opcode-specific. Empty frames (length 0) are ignored, not decoded (`MeshHub.cs:334`,
   `MeshClient.cs:511`).

Everything in the tables below is the **message payload** (i.e. after the transport's framing header).

---

## Opcodes (`MessageType`, `Messages/MessageType.cs`)

| Name | Byte | Direction | Payload after the opcode byte |
|---|---|---|---|
| `RegistrationComplete` | `0x01` | hub → client | assigned client id (16) |
| `SendMessage` | `0x02` | client → hub | recipient id (16), message bytes |
| `DeliverMessage` | `0x03` | hub → client | sender id (16), message bytes |
| `RegistrationRequest` | `0x04` | client → hub | version (1), UTF-8 name |
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

**`RegistrationErrorCode`** (`RegistrationErrorCode.cs`, sent as the byte after `Error`):
`DuplicateClientName=0x01`, `UnsupportedProtocolVersion=0x02`, `ClientNameTooLong=0x03`,
`HubAtCapacity=0x04`.

---

## Registration handshake

```
client → hub : [0x04 RegistrationRequest][version=2][utf8 clientName]
hub → client : [0x01 RegistrationComplete][clientId (16 bytes)]      # success
             | [0x05 Error][errorCode]                               # refused
```

Hub-side validation order (`MeshHub.cs:259-306`), each failure sends the error (if applicable) and drops
the connection:
1. Frame ≥ 3 bytes and opcode `0x04` — else drop silently (no error frame).
2. `version == 2` — else `Error(UnsupportedProtocolVersion)`.
3. `clientName.Length ≤ 256` **chars** — else `Error(ClientNameTooLong)`.
4. `registered count < maxClients` — else `Error(HubAtCapacity)`.
5. name not already claimed (`_clientNames.TryAdd`) — else `Error(DuplicateClientName)`.

Client-side (`MeshClient.cs:126-160`): an `Error` reply → `RegistrationRefusedException(errorCode)`; any
reply that isn't exactly a 17-byte `RegistrationComplete` → `InvalidOperationException`.

A connection that never sends a valid registration within `registrationTimeout` (default 10 s) is
dropped without an error frame (`MeshHub.cs:248-256`).

---

## Message frames (exact byte offsets)

Direct send / deliver:
```
SendMessage       : [0x02][recipientId 16][body...]              # client→hub, needs len ≥ 17
DeliverMessage    : [0x03][senderId 16][body...]                 # hub→client, needs len ≥ 17
```
Broadcast is sent as `BroadcastMessage` but **delivered as `DeliverMessage`** — recipients cannot tell a
broadcast from a direct message (`MeshHub.BroadcastMessage` builds a `0x03` frame, `MeshHub.cs:637`):
```
BroadcastMessage  : [0x0B][body...]                              # client→hub
```

Groups:
```
JoinGroup         : [0x0C][utf8 groupName...]                    # whole remainder is the name
LeaveGroup        : [0x0D][utf8 groupName...]
GroupMessage      : [0x0E][nameLen u16 BE][utf8 groupName][body...]   # client→hub, needs len ≥ 3
DeliverGroupMessage: [0x0F][senderId 16][nameLen u16 BE][utf8 groupName][body...]  # hub→client, needs len ≥ 19
```
The hub passes the original name bytes straight through from the inbound `GroupMessage` into the
outbound `DeliverGroupMessage` rather than re-encoding the decoded string (`MeshHub.cs:369-375`,
`:766-771`).

Lookup (correlated request/response):
```
ClientLookupRequest : [0x06][correlationId i32 BE][utf8 name]    # client→hub, needs len ≥ 5
ClientLookupResponse: [0x07][correlationId i32 BE][found u8][id 16 if found==1]  # needs len ≥ 6
```
`found == 0x01` **and** total length ≥ 22 → the 16-byte id follows; otherwise the client resolves the
lookup to `null` (`MeshClient.cs:604-611`). The client only completes a lookup whose correlation id
matches the pending request (see [client.md](client.md)).

Control (no payload beyond the opcode):
```
Disconnect : [0x08]     # either direction; graceful close
Ping       : [0x09]     # hub→client liveness probe
Pong       : [0x0A]     # client→hub reply
```
`Ping`/`Pong` only exist when the hub is configured with a `heartbeatInterval`. The client replies to a
`Ping` best-effort (`MeshClient.cs:613-624`); the hub treats **any** received frame (including `Pong`)
as proof of life via its activity counter, so a busy client is never pinged.

---

## Length-guard behaviour (why malformed frames "do nothing")

Both dispatch chains are length-guarded `if / else if` ladders with **no terminal `else`**
(`MeshHub.cs:340-412`, `MeshClient.cs:513-643`). A frame that is too short for its opcode, or carries an
unrecognised opcode, **falls through and is silently ignored** — no exception, no log at warning level.
When debugging "my message never arrives", suspect a framing/offset error first; it will not surface as
an error. If you add an opcode, add both the guard and the branch on the correct side, and mirror the
exact offsets above.

## Versioning

`Protocol.Version` gates the handshake only; there is no per-message version. A client and hub must
agree on version `2` or registration is refused with `UnsupportedProtocolVersion`. Any backward-
incompatible change to the frames above must bump `Protocol.Version`
([index §6](../for-clanker.md#6-cross-cutting-conventions-imitate-these) lists the add-a-message-type
checklist).
