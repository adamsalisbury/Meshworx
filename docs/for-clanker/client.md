# Client & reconnection — `MeshClient` / `IMeshClient` / `MeshClientReconnector`

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [transport.md](transport.md) · [protocol.md](protocol.md) · [known-issues.md](known-issues.md)

The application-facing side. `MeshClient` connects to a hub over an `ITransport`, sends messages, looks
up peers, manages group membership, and raises events for inbound traffic and disconnects.
`MeshClientReconnector` optionally wraps a client to keep it connected.

- `public sealed class MeshClient : IMeshClient, IAsyncDisposable` — `src/AdamSalisbury.Meshworx/MeshClient.cs:12`
- `public interface IMeshClient : IAsyncDisposable` — `src/AdamSalisbury.Meshworx/IMeshClient.cs:6`
- `public sealed class MeshClientReconnector : IAsyncDisposable` — `src/AdamSalisbury.Meshworx/MeshClientReconnector.cs:33`

---

## `MeshClient`

### Public surface

| Member | Signature / notes | Source |
|---|---|---|
| ctor | `MeshClient(ILogger<MeshClient>, TimeSpan? idleTimeout=null, TimeSpan? sendTimeout=null, int maxSendAttempts=1, TimeSpan? sendRetryDelay=null)` | `MeshClient.cs:90` |
| `Id` | `Guid` — assigned by hub; `Guid.Empty` when disconnected | `MeshClient.cs:131` |
| `Name` | `string` — set on connect, cleared on disconnect | `MeshClient.cs:134` |
| `IsConnected` | `bool` — true only in `Connected` state | `MeshClient.cs:140` |
| `JoinedGroups` | `IReadOnlyCollection<string>` — **snapshot** of client-side membership | `MeshClient.cs:152` |
| `ConnectAsync` | `Task ConnectAsync(ITransport, string clientName, ReadOnlyMemory<byte> credential=default, CancellationToken=default)` | `MeshClient.cs:176` |
| `NegotiatedProtocolVersion` | `byte` — the wire-protocol version agreed with the hub during the last successful `ConnectAsync`; `0` when not connected | `MeshClient.cs:137` |
| `DisconnectAsync` | `Task DisconnectAsync(CancellationToken=default)` — graceful; no `Disconnected` event | `MeshClient.cs:294` |
| `SendAsync` | `Task SendAsync(Guid recipientId, ReadOnlyMemory<byte>, CancellationToken=default)` — compatibility overload, forwards to the headers overload with `MessageHeaders.Empty` | `MeshClient.cs:368` |
| `SendAsync` (headers) | `Task SendAsync(Guid recipientId, ReadOnlyMemory<byte>, MessageHeaders headers, CancellationToken=default)` — PR #74 (issue #32); **throws `ArgumentException` if `headers` contains any of the six reserved request/reply/acknowledgement/expiry keys** (PR #83, extended by PR #84, extended again by PR #85); see [Sending headers](#sending-headers) | `MeshClient.cs:377` |
| `SendAsync` (delivery options) | `Task SendAsync(Guid recipientId, ReadOnlyMemory<byte>, DeliveryOptions options, CancellationToken=default)` — PR #84; with `DeliveryOptions.None` identical to the plain overload; with `DeliveryOptions.RequireAck(timeout)` awaits an end-to-end delivery acknowledgement or throws `TimeoutException`, see [Delivery acknowledgement](#delivery-acknowledgement) | `MeshClient.cs:390` |
| `SendAsync` (time-to-live) | `Task SendAsync(Guid recipientId, ReadOnlyMemory<byte>, TimeSpan timeToLive, CancellationToken=default)` — PR #85 (issue #29); throws `ArgumentOutOfRangeException` if `timeToLive` is not positive; the message is dropped, not delivered, once it expires, see [Message expiry (time-to-live)](#message-expiry-time-to-live) | `MeshClient.cs:446` |
| `BroadcastAsync` | `Task BroadcastAsync(ReadOnlyMemory<byte>, CancellationToken=default)` | `MeshClient.cs:558` |
| `JoinGroupAsync` / `LeaveGroupAsync` | `Task ...(string groupName, CancellationToken=default)` — **optimistic**: `JoinGroupAsync` records membership *before* sending and the hub may still refuse, see [Group membership](#group-membership) | `MeshClient.cs:580` / `:621` |
| `SendToGroupAsync` | `Task SendToGroupAsync(string groupName, ReadOnlyMemory<byte>, CancellationToken=default)` — compatibility overload, forwards to the headers overload with `MessageHeaders.Empty` | `MeshClient.cs:637` |
| `SendToGroupAsync` (headers) | `Task SendToGroupAsync(string groupName, ReadOnlyMemory<byte>, MessageHeaders headers, CancellationToken=default)` — PR #74 (issue #32); see [Sending headers](#sending-headers) | `MeshClient.cs:646` |
| `GetClientIdByNameAsync` | `Task<Guid?> GetClientIdByNameAsync(string name, CancellationToken=default)` | `MeshClient.cs:798` |
| `RequestAsync` | `Task<ReadOnlyMemory<byte>> RequestAsync(Guid recipientId, ReadOnlyMemory<byte>, TimeSpan timeout, CancellationToken=default)` — PR #83; correlated request/reply over a direct message, see [Request/response (RPC)](#request-response) | `MeshClient.cs:847` |
| `ReplyAsync` | `Task ReplyAsync(MessageReceivedEventArgs request, ReadOnlyMemory<byte>, CancellationToken=default)` — PR #83; answers a request received via `MessageReceived`, see [Request/response (RPC)](#request-response) | `MeshClient.cs:904` |
| `MessageReceived` | `event EventHandler<MessageReceivedEventArgs>` — direct **and** broadcast; `Headers` populated when the sender attached any (PR #74); `CorrelationId` set when the message is a request awaiting a reply (PR #83) | `MeshClient.cs:164` |
| `GroupMessageReceived` | `event EventHandler<GroupMessageReceivedEventArgs>` — carries group name; `Headers` populated when the sender attached any (PR #74) | `MeshClient.cs:167` |
| `GroupJoinRefused` | `event EventHandler<GroupJoinRefusedEventArgs>` — the hub refused a join; the group has **already** been removed from `JoinedGroups` when this fires | `MeshClient.cs:170` |
| `Disconnected` | `event EventHandler<DisconnectedEventArgs>` — **unexpected** endings only | `MeshClient.cs:173` |
| `DisposeAsync` | `ValueTask` — `DisconnectAsync` then disposes the lookup semaphore | `MeshClient.cs:928` |

> **Coordinate caveat — resolved for PR #73, re-pointed for PR #74, PR #83 and PR #84, re-pointed again
> for PR #85.** Every row in this table, and every `MeshClient.cs`/`IMeshClient.cs` citation in the rest
> of this file, was re-derived from the current source as of PR #85 (`feature/message-ttl-expiry`, three
> commits on top of `main` at PR #84's merge, the last two of which are fixes to the first — "harden
> expiry parsing against out-of-range values" and "apply the expiry check to group messages" — not
> separate features).
>
> `MeshClient.cs` grew by a net **63** lines (1575 → 1638) across four separate insertion points (a fifth
> hunk, the `DeliverGroupMessageWithHeaders` expiry check, replaces one line for one — net zero, no shift
> introduced), each individually verified against the source (content-equality spot checks at every
> boundary, the same technique validated on every prior pass back to #64): the new
> `SendAsync(..., TimeSpan, ...)` overload plus one extra `<see cref>` line in `SendCoreAsync`'s doc
> comment (+24, immediately after the `DeliveryOptions` overload), the reserved-key guard's growth from
> five keys to six and its refactor from a chain of `ContainsKey` checks into a `foreach` over a new
> `ReservedHeaderKeys` array (+9, so **+33** cumulative from this point), the `DeliverMessageWithHeaders`
> branch's check widening from two conditions to three (+1, so **+34** cumulative), and the new
> `IsExpired` method itself, inserted after `TryReadHeaderBlock` (+29, so **+63** cumulative). Everything
> before `SendAsync(..., TimeSpan, ...)` (old line ≤ 443, covering the whole
> Lifecycle, `ConnectAsync`, receive-loop dispatch header, claim-protocol and group-membership sections)
> is **unmoved** by this pass. `IMeshClient.cs` grew by **33** lines in one place — the new
> `SendAsync(..., TimeSpan, ...)` interface member, inserted directly after the `DeliveryOptions` overload
> and before `BroadcastAsync` (old line ≥ 154) — mirroring PR #84's own single-insertion-point shift for
> the same interface.
>
> The prior pass's own note (PR #84, re-pointing `MeshClient.cs` by +212 and `IMeshClient.cs` by +32) is
> folded into the coordinates above rather than repeated; it corrected one pre-existing wrong citation
> (`IMeshClient.cs`'s `Disconnected` `<remarks>` contract, which had pointed at `RequestAsync`'s own
> declaration) to `:334-344`, now re-pointed by this pass's own +33 shift to `:367-377`. See
> [`Disconnected` semantics](#disconnected-semantics-important) below.

### Lifecycle & state machine

Internal `enum ConnectionState { Disconnected, Connecting, Connected, Disconnecting }`
(`MeshClient.cs:1614`), guarded by `_stateLock` (`System.Threading.Lock`). Send/lookup/group methods
throw `InvalidOperationException("Not connected to a hub.")` unless `Connected`.

**`ConnectAsync`** (`MeshClient.cs:176`):
1. Validates `transport`/`clientName`, rejects names longer than 256 chars, and refuses to connect
   unless currently `Disconnected` (state-specific message otherwise).
2. Sends `RegistrationRequest` (`[0x04][versionMin][versionMax][nameLen u16 BE][utf8 name][credential]`,
   `MeshClient.cs:211-219`), always advertising `Protocol.MinSupportedVersion`/`MaxSupportedVersion`
   (`4`/`5` as of PR #74), then awaits one frame.
3. If the reply is `Error`, throws `RegistrationRefusedException` carrying the
   `RegistrationErrorCode` — which now includes `AuthenticationFailed` if the hub's authenticator
   rejected the credential, and (unchanged) `UnsupportedProtocolVersion` if the hub could not negotiate a
   version in this client's range. If the reply is not a well-formed `RegistrationComplete` (exactly
   18 bytes as of PR #73, was 17), throws `InvalidOperationException`.
4. Records `Id` and `NegotiatedProtocolVersion` (the hub's reply's trailing byte, `MeshClient.cs:238-240`
   — `0` until a successful connect), sets `Connected`, and starts `ReceiveLoopAsync`.
5. On **any** failure it cleans up (disposes the transport), resets to `Disconnected` (and
   `NegotiatedProtocolVersion` to `0`), logs, and rethrows.

> **`NegotiatedProtocolVersion` is read by the send path since PR #74 (issue #32), not just logged.**
> `SendAsync`'s headers overload — and, since PR #83, `RequestAsync`/`ReplyAsync`, and since PR #84 the
> `RequireAck` branch of `SendAsync(..., DeliveryOptions, ...)`, all of which share the same internal
> `SendCoreAsync` (`MeshClient.cs:468-521`) — calls `RequireHeaderEnvelopeSupport` (call site `:509`,
> definition `:696-705`), which throws `NotSupportedException` if this connection's negotiated version is
> below `Protocol.HeaderEnvelopeMinVersion` — see [Sending headers](#sending-headers),
> [Request/response (RPC)](#request-response) and [Delivery acknowledgement](#delivery-acknowledgement).
> `SendToGroupAsync`'s headers overload has its own separate call to the same check (`:676`); group sends
> do not go through `SendCoreAsync` and cannot participate in the request/response or acknowledgement
> patterns (both only ever address a single `recipientId`). This resolves
> [known-issues.md](known-issues.md) KI-14 for the header envelope specifically; a future capability gated
> the same way would need its own check.

> **The client takes ownership of the transport** (`IMeshClient.cs` doc, `MeshClient.cs` cleanup paths):
> it is disposed on disconnect or if the handshake fails. **The caller must not use or dispose the
> transport after `ConnectAsync`.** Each connection needs a fresh transport — this is why
> `MeshClientReconnector` takes a `transportFactory`, not a transport.

#### The `credential` parameter

`credential` is an **opaque `ReadOnlyMemory<byte>` inserted before the `CancellationToken`** — a
**source-breaking** change to `ConnectAsync` on both `IMeshClient` (`IMeshClient.cs:61` — unaffected by
PR #83, which only appends new members after `GetClientIdByNameAsync`) and
`MeshClient`, dating from the historical v2→v3 protocol transition (unrelated to, and unaffected by, the
version-negotiation work in PR #73). Any call site that passed a token positionally
(`ConnectAsync(transport, name, cancellationToken)`) will now fail to compile; pass the token by name,
or supply a credential.

- It defaults to empty, so `ConnectAsync(transport, name)` is unchanged.
- It is copied into the registration frame and sent once, during the handshake. It is **not** retained,
  and there is no way to present a credential after connecting.
- The client never inspects it. Whether it is required, and what it must contain, is entirely determined
  by the `ClientAuthenticator` the hub was constructed with — see [hub.md](hub.md#authentication).
- A hub with no authenticator ignores whatever you send, so a credential is safe to pass unconditionally.

```csharp
await client.ConnectAsync(transport, "Alice", credential: Encoding.UTF8.GetBytes(apiKey));
```

If the hub's authenticator refuses, `ConnectAsync` throws
`RegistrationRefusedException` with `ErrorCode == RegistrationErrorCode.AuthenticationFailed`. That code
is **also** what you get if the hub's authenticator threw, hung or was starved of concurrency slots — the
client cannot tell those apart, so do not treat it as proof the credential itself was wrong. Retrying is
reasonable; retrying in a tight loop is not.

There is a subtle **synchronous-completion guard** (`MeshClient.cs:241-261`): if the hub has already
buffered a `Disconnect`, `ReceiveLoopAsync` can run to completion synchronously and a `Disconnected`
handler may reconnect from within it, replacing `_cts`. The code only records `_receiveLoopTask` if
`_cts` is still the one it created, so a stale synchronous loop never clobbers a newer connection.
Preserve this reference-equality check if you refactor connect.

### The receive loop

`ReceiveLoopAsync` (`MeshClient.cs:961`) is the single reader. It:
- Sets `AsyncLocal<bool> _inReceiveLoop = true` so a `DisconnectAsync` invoked **from a handler** skips
  awaiting the loop (would deadlock) — see below.
- Runs an optional **idle monitor** (`MonitorIdleAsync`, `MeshClient.cs:986`) on a `PeriodicTimer`,
  comparing an `activitySequence` counter between ticks; on a fully idle interval it cancels the loop's
  linked CTS, ending the connection as `ConnectionLost`.
- Dispatches inbound frames: `DeliverMessage` → `MessageReceived`; `DeliverGroupMessage` →
  `GroupMessageReceived`; `DeliverMessageWithHeaders`/`DeliverGroupMessageWithHeaders` (PR #74, issue #32)
  → the same two events with `Headers` populated from a decoded `MessageHeaders`, via `TryReadHeaderBlock`
  (`:1381-1393`, see [protocol.md](protocol.md#message-headers)); `GroupJoinRefused` → removes the group
  from `_joinedGroups`, logs a `Warning`, then raises `GroupJoinRefused` (`MeshClient.cs:1173-1198`);
  `ClientLookupResponse` → completes the pending lookup (if correlation matches, `:1199-1220`); `Ping` →
  replies `Pong` (best-effort); `Disconnect` → sets reason `RemoteDisconnect` and breaks.
- Wraps each handler invocation in `try/catch` and logs a throwing subscriber (callback boundary) so it
  cannot halt delivery (`MeshClient.cs:1041-1054`, `:1067-1080`). **The two header-bearing branches are
  no longer identical to each other since PR #83.** `DeliverGroupMessageWithHeaders`
  (`MeshClient.cs:1131-1172`) raises `GroupMessageReceived` inside a plain `try/catch` — group sends
  cannot participate in request/response or acknowledgement (`RequestAsync`/
  `SendAsync(..., DeliveryOptions, ...)` only ever address a single `recipientId`) — but **since PR #85 it
  is gated by one check first**: `!IsExpired(headers, senderId)` (`:1150`, method at `:1407-1422`) drops an
  already-expired group message before the event is ever raised; see
  [Message expiry (time-to-live)](#message-expiry-time-to-live) below. `DeliverMessageWithHeaders`
  (`MeshClient.cs:1083-1130`) is now gated by **three** nested checks in sequence,
  `TryCompletePendingAck(senderId, headers)`, `TryCompletePendingRequest(senderId, headers, messageData)`,
  **then, since PR #85, `!IsExpired(headers, senderId)`** (`:1096-1098`; methods at `:1502-1547`,
  `:1439-1484` and `:1407-1422` respectively) **before** it raises `MessageReceived` at all: a frame that
  is either a delivery acknowledgement, a reply to one of this client's own calls, or already expired is
  resolved (or dropped) internally and never surfaces through the event. **Since PR #84**, once a genuine
  `MessageReceived` *is* raised for an incoming message (`:1100-1115`, also setting
  `CorrelationId = TryGetRequestCorrelationId(headers)` at `:1107` so a handler receiving a genuine
  incoming request knows to answer it with `ReplyAsync`), the branch fires a delivery acknowledgement back
  to the sender — **fire-and-forget**, not awaited — if the sender requested one (`:1117-1126`, dispatched
  via `TrySendAcknowledgementAsync`). See [Request/response (RPC)](#request-response),
  [Delivery acknowledgement](#delivery-acknowledgement) and
  [Message expiry (time-to-live)](#message-expiry-time-to-live) for the full behaviour of each.
- On termination (`finally`, `MeshClient.cs:1255-1301`): stops the idle monitor, **faults any pending
  lookup** with `InvalidOperationException` so a caller on a non-cancellable token is not left hanging
  and `_lookupLock` is released; faults every still-pending `RequestAsync` call the same way (`:1278-1287`,
  PR #83) and clears `_pendingRequests`; **since PR #84, does the same for every still-pending
  `SendAsync(..., DeliveryOptions.RequireAck(...))` call** (`:1289-1297`) and clears `_pendingAcks` — so a
  connection that drops mid-request or mid-acknowledgement does not leave a caller waiting forever; then
  calls `HandleReceiveLoopTerminationAsync`.

`HandleReceiveLoopTerminationAsync` (`MeshClient.cs:1312`) decides whether the ending raises
`Disconnected`. There are **two** gates and both must pass:

1. **The entry gate** (`MeshClient.cs:1314-1325`). Under `_stateLock`, the teardown claims the connection
   by moving `Connected` → `Disconnecting`. If the state was anything other than `Connected`, a local
   `DisconnectAsync` already owns the teardown, so the loop returns immediately and stays silent.
2. **The claim gate** (`MeshClient.cs:1337-1357`). After `CleanUpAsync`, the loop reads
   `_localDisconnectRequested` into a local `raiseDisconnected` **in the same locked block that
   publishes `_state = ConnectionState.Disconnected`** (`MeshClient.cs:1339-1350`). If a `DisconnectAsync`
   claimed the teardown while it was in flight, the loop logs at Debug and returns without raising
   (`MeshClient.cs:1352-1357`).

Gate 1 alone used to be the whole mechanism, and it was **not sufficient**. If the receive loop won the
race out of `Connected`, a concurrent `DisconnectAsync` found the client already `Disconnecting`,
returned as a silent no-op, and the loop went on to raise `Disconnected(ConnectionLost)` for a
disconnect the application had itself requested — issue #10, fixed by PR #62.

#### The claim protocol (load-bearing)

`DisconnectAsync`'s early return is no longer a pure no-op (`MeshClient.cs:298-316`): when it finds the
state is `Disconnecting` it sets `_localDisconnectRequested = true`, claiming the in-flight teardown so
that it stays silent. The flag is a plain `bool` guarded by `_stateLock` (`MeshClient.cs:30`, unaffected
by every pass since PR #73 — it sits before every insertion point any of them has made).

Because the claim and the loop's read of it are taken under that same lock, the outcome is decided
atomically: either the claim lands before the loop publishes the disconnected state and the event is
suppressed, or the state is already `Disconnected`, the decision has been taken, and there is nothing
left to claim. `ConnectAsync` clears the flag in the same locked block that moves the state to
`Connecting` (`MeshClient.cs:205`), so an unconsumed claim — a redundant second `DisconnectAsync`, say —
cannot leak forward and silence a genuine drop on the *next* connection.

The net contract is that **the outcome does not depend on which side wins the race**: whoever tears the
connection down, an application-requested disconnect is silent. Do not "simplify" the early return back
to a bare `return`, and do not move the `raiseDisconnected` read out of the locked block. One window is
deliberately left open — see [known-issues.md](known-issues.md) KI-21.

### `Disconnected` semantics (important)

- Fires **only** for unexpected endings: `RemoteDisconnect` (hub sent `Disconnect`) or `ConnectionLost`
  (transport failed or idle timeout tripped). Reason is a `DisconnectReason` on the event args.
- **Does not fire for a local `DisconnectAsync`** — including one that races a remote drop. Whichever
  side tears the connection down, an application-requested disconnect stays silent: `DisconnectAsync`
  either performs the teardown itself or claims the one already in flight (see the claim protocol
  above). The interface XML docs state this contract (`IMeshClient.cs:70-83`, `:367-377` — this second
  citation was found pointing at `RequestAsync`'s declaration instead and corrected this pass, see the
  coordinate caveat above).
  - **The one exception** is a narrow residual window: a `DisconnectAsync` arriving *after* the teardown
    has published the disconnected state has nothing left to claim, and the event fires. Read
    [known-issues.md](known-issues.md) KI-21 before you rely on the suppression being absolute.
- When it fires the client has **already reset** to `Disconnected`, so a handler may immediately call
  `ConnectAsync` again (this is how the reconnector works, and it is a supported pattern). This
  pattern is also *why* KI-21 is left open: closing it would require invoking the event under
  `_stateLock`, which would deadlock a handler that reconnects synchronously.
- **Deadlock-safety:** you may call `DisconnectAsync` from inside a `MessageReceived` or `Disconnected`
  handler. The `_inReceiveLoop` `AsyncLocal` flows into the synchronous handler and makes
  `DisconnectAsync` skip `await`-ing the receive loop task (`MeshClient.cs:344`, corrected this pass from a
  pre-existing off-by-two citation). Do not remove this.

<a id="group-membership"></a>

### Group membership — optimistic, and revocable by the hub

`JoinGroupAsync` (`MeshClient.cs:580`) is **fire-and-forget with an optimistic local record**. The order
of operations changed in PR #66 and the new order is load-bearing:

1. Validate the name and grab the connected transport (`MeshClient.cs:582-584`) — both *before* anything
   is recorded, so a rejected call leaves no trace.
2. **Record the membership in `_joinedGroups`, then send the frame** (`:590-594`). Not the other way
   round: the hub may refuse, and its `GroupJoinRefused` can arrive and be handled by the receive loop
   **before this method resumes**. Recording afterwards would reinstate the very group the refusal had
   just removed.
3. If the send throws, take the record back — **but only if this call is what added it** (`recorded`,
   `:593`, rollback at `:608-614`). A join of a group already joined, or one racing a concurrent join of
   the same name, must not roll back a record its predecessor owns; the group would then be missing from
   `JoinedGroups` while the client is still in it on the hub, and `MeshClientReconnector` — which
   restores from that snapshot — would silently not restore it.

`LeaveGroupAsync` (`:621`) keeps the opposite order: send first, then remove locally (`:627-633`).

**What the return value means.** `JoinGroupAsync` returning means *the request was sent*, not that you
are a member. A hub with a `GroupAuthoriser` may refuse. Applications that depend on membership must
watch `GroupJoinRefused`:

```csharp
client.GroupJoinRefused += (_, e) =>
    Console.WriteLine($"Refused membership of {e.GroupName}");

await client.JoinGroupAsync("engineering");
// membership is NOT yet proven here
```

On a refusal the receive loop removes the group from `_joinedGroups` **first**, then logs, then raises
the event (`MeshClient.cs:1173-1198`) — so `JoinedGroups` never claims a membership the hub has denied,
and a later disconnect does not hand the group to the reconnector to restore. The refusal is **not**
retried by anything in the library; a handler that wants to try again must ask again itself.

> **The refusal carries no correlation id**, so a refusal for an older join can clear a membership a
> later join legitimately obtained. The divergence is fail-safe — the client under-reports while the hub
> keeps the member — but it is real. [known-issues.md](known-issues.md) KI-27.

Note the client logs the refused group name **unclipped** (`MeshClient.cs:1187`), unlike the hub, which
clips to 64 characters. The name came from your own hub, so this is not the same exposure, but it is
worth knowing if you parse client logs.

<a id="sending-headers"></a>

### Sending headers

PR #74 (issue #32) added a `MessageHeaders` overload to both `SendAsync` and `SendToGroupAsync` — a
small, string-keyed, immutable bag of metadata (correlation id, content-type hint, trace context, and
the like) that travels alongside the payload without the hub ever decoding it. Full wire-format
write-up: [protocol.md](protocol.md#message-headers). **PR #83's `RequestAsync`/`ReplyAsync` and PR #84's
`SendAsync(..., DeliveryOptions.RequireAck(...), ...)` are both built entirely on this overload** — see
[Request/response (RPC)](#request-response) and [Delivery acknowledgement](#delivery-acknowledgement)
below.

```csharp
var headers = new MessageHeaders(new Dictionary<string, string> { ["correlationId"] = "abc123" });
await client.SendAsync(recipientId, payload, headers);
```

- **The plain `SendAsync(recipientId, message, cancellationToken)` overload still exists** and is now a
  one-line forward to the headers overload with `MessageHeaders.Empty` (`MeshClient.cs:368-374`) — existing
  call sites need no change, and an empty `MessageHeaders` produces the exact same bytes on the wire as
  before (no header block is written at all when `headers.Count == 0`).
- **The headers overload itself now guards six reserved keys — two from PR #83, three more from PR #84,
  one more from PR #85.** `SendAsync(recipientId, message, headers, cancellationToken)` calls
  `ThrowIfReservedHeaderKeyPresent(headers)` (`MeshClient.cs:384`, method at `:543-555`) before doing
  anything else: if `headers` contains `"mesh.request-id"` or `"mesh.reply"` (`RequestReplyHeaderKeys`,
  PR #83), `"mesh.ack-id"`, `"mesh.ack-request"` or `"mesh.ack"` (`DeliveryAcknowledgementHeaderKeys`,
  PR #84), **or** `"mesh.expires-at"` (`MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds`, PR #85), it
  throws `ArgumentException` rather than letting the message collide with the request/response, delivery-
  acknowledgement or expiry machinery. **Since PR #85 the guard is a `foreach` over a single
  `ReservedHeaderKeys` array (`:527-535`)** rather than a chain of individual `ContainsKey` checks, so
  adding a seventh key in future needs only a new array entry, not a rewritten condition. The body-build
  logic and the actual frame-send all live in a shared private `SendCoreAsync` (`:468-521`) that
  `RequestAsync`/`ReplyAsync`, the `RequireAck` branch of `SendAsync(..., DeliveryOptions, ...)`, the
  `SendAsync(..., TimeSpan, ...)` time-to-live overload (see
  [Message expiry (time-to-live)](#message-expiry-time-to-live)), and the receive loop's automatic
  acknowledgement reply all call directly, **bypassing** this guard — they are the legitimate producers of
  those six keys. `SendToGroupAsync`'s headers overload is untouched: it does not share `SendCoreAsync` and
  does not guard these keys, because group sends cannot be a request, a reply, an acknowledgement, or
  carry a time-to-live in the first place — there is no `SendToGroupAsync(..., TimeSpan, ...)` overload.
  See [Request/response (RPC)](#request-response), [Delivery acknowledgement](#delivery-acknowledgement),
  [Message expiry (time-to-live)](#message-expiry-time-to-live) and
  [known-issues.md](known-issues.md) KI-42.
- **A non-empty `MessageHeaders` requires a connection negotiated at `Protocol.HeaderEnvelopeMinVersion`
  (`5`) or above.** Below that, both `SendAsync` and `SendToGroupAsync` throw `NotSupportedException`
  (`RequireHeaderEnvelopeSupport`, `MeshClient.cs:696-705`) rather than silently sending the message
  without its headers — a caller that assumes headers arrived when the hub actually stripped them (or
  never received them) would be a much harder bug to find than an eagerly-thrown exception.
- **`MessageHeaders`'s public constructor copies its input into a fresh `Dictionary<string, string>` using
  `StringComparer.Ordinal`** — key lookups are case-sensitive, and passing an enumerable with a duplicate
  key **throws `ArgumentException`** rather than silently keeping the last value, unlike an object
  initializer or a plain `Dictionary` indexer assignment. See
  [known-issues.md](known-issues.md) KI-34.
- **`GetEncodedLength` throws `ArgumentException` for an oversized header set** (aggregate encoded length
  over 65 535 bytes, or any single key over 255 UTF-8 bytes / value over 65 535 UTF-8 bytes) — this
  surfaces synchronously from `SendAsync`/`SendToGroupAsync` itself, before anything is written to the
  wire, so it is a caller bug to handle, not a delivery failure.
- **On receipt**, `MessageReceivedEventArgs.Headers`/`GroupMessageReceivedEventArgs.Headers` default to
  `MessageHeaders.Empty` — check `Headers.Count` or use `TryGetValue` rather than assuming a particular
  key is present, since the sender controls what it attaches (or an older/degraded peer may have had its
  header block stripped in transit by the hub — see [hub.md](hub.md#routing-helpers)).

<a id="request-response"></a>

### Request/response (RPC)

PR #83 added a correlated request/reply helper — `RequestAsync`/`ReplyAsync` (`IMeshClient.cs:314-318` /
`:301-304`, implemented `MeshClient.cs:847-901` / `:904-925`) — built entirely on the existing
[header envelope](protocol.md#message-headers) and the existing `SendMessageWithHeaders`/
`DeliverMessageWithHeaders` opcodes (`0x11`/`0x12`). **No new opcode and no protocol version bump were
needed**: a request and its reply are both just direct messages carrying two new well-known header keys,
`Messages/RequestReplyHeaderKeys.cs` (`internal`, not part of the public surface):

| Key | Value | Wire string |
|---|---|---|
| `RequestReplyHeaderKeys.CorrelationId` | the sender's own request correlation id, invariant-culture integer | `"mesh.request-id"` |
| `RequestReplyHeaderKeys.Reply` | present with value `"1"` only on the reply frame | `"mesh.reply"` |

Both keys ride inside the same `MessageHeaders` block `SendAsync`'s headers overload already sends, so
**`RequestAsync`/`ReplyAsync` inherit the header envelope's own version gate** — they throw
`NotSupportedException` (via the shared `RequireHeaderEnvelopeSupport` check inside `SendCoreAsync`, see
above) on a connection negotiated below `Protocol.HeaderEnvelopeMinVersion` (`5`), exactly like a manual
`SendAsync(recipientId, message, headers)` call would.

```csharp
// Responder
bob.MessageReceived += async (_, e) =>
{
    if (e.CorrelationId is not null)
    {
        byte[] reply = Encoding.UTF8.GetBytes($"echo:{Encoding.UTF8.GetString(e.Data.Span)}");
        await bob.ReplyAsync(e, reply);
    }
};

// Requester
ReadOnlyMemory<byte> reply = await alice.RequestAsync(
    bobId, Encoding.UTF8.GetBytes("ping"), TimeSpan.FromSeconds(5));
```

**How `RequestAsync` works** (`MeshClient.cs:847-901`):
1. Rejects a non-positive `timeout` with `ArgumentOutOfRangeException` before sending anything.
2. Claims a fresh correlation id via `Interlocked.Increment(ref _requestCorrelationId)` (`:858`) and
   records a `PendingRequest(recipientId, completion)` in `_pendingRequests[correlationId]` (`:865`) —
   **before** the send, so a reply racing back extremely fast still finds an entry.
3. Sends the request as an ordinary direct message via the shared `SendCoreAsync` (`:468-521`), with a
   `MessageHeaders` carrying only `CorrelationId`.
4. Awaits the completion source, bounded by a linked `CancellationTokenSource` cancelled after `timeout`
   (`:881-892`) — a timeout is translated to `TimeoutException`, a genuine external cancellation still
   surfaces as `OperationCanceledException`.
5. **`finally` always removes the entry from `_pendingRequests`** (`:899`), whether the call succeeded,
   timed out, or was cancelled — so a reply that arrives later for that id is discarded by
   `TryCompletePendingRequest` rather than resolving a *future* request that happens to reuse the id.

**How `ReplyAsync` works** (`MeshClient.cs:904-925`): takes back the exact `MessageReceivedEventArgs` the
request arrived on. If `request.CorrelationId` is `null` (an ordinary message, not a request), it throws
`InvalidOperationException` rather than sending a reply frame nothing is waiting for. Otherwise it sends a
direct message back to `request.SenderId` carrying both `CorrelationId` (echoed) and `Reply = "1"`, again
via `SendCoreAsync`.

**How an incoming reply is matched, and why a hostile peer cannot resolve someone else's request**
(`TryCompletePendingRequest`, `MeshClient.cs:1439-1484`, called from the receive loop's
`DeliverMessageWithHeaders` branch at `:1097`, **second** of the two nested checks — `:1096` is the
delivery-acknowledgement check added by PR #84, see [Delivery acknowledgement](#delivery-acknowledgement)
— **before** `MessageReceived` is ever raised for that frame):
1. A frame without `Reply == "1"` is not a reply at all — returns `false`, so the caller raises
   `MessageReceived` as normal (this is how an incoming *request* is delivered — see below).
2. A reply with a missing/malformed `CorrelationId` is logged and discarded (`:1451`) — still intercepted,
   never raised through `MessageReceived`.
3. A reply whose correlation id is not currently in `_pendingRequests` is logged at `Debug` and discarded
   (`:1455-1462`) — the request it answers has already timed out, been cancelled, or never existed on this
   connection.
4. **A reply is only accepted from the client the request was actually addressed to**
   (`pending.ExpectedResponderId != senderId`, `:1463-1474`): a mismatch is logged at `Warning` and
   discarded, **without removing the pending entry** — so a forged reply from any other client connected
   to the same hub cannot resolve, and cannot strand, a request meant for someone else. The genuine
   responder's reply, arriving afterwards, still completes it. This is the actual security property: the
   hub does not authenticate senders end-to-end (see [known-issues.md](known-issues.md) KI-17), but
   `RequestAsync` cannot be tricked into accepting attacker-controlled bytes as long as the attacker is not
   the client the request was sent to.
5. A genuine match resolves via a compare-and-remove (`TryRemove(new KeyValuePair<...>(correlationId,
   pending))`, `:1475-1481`) against the *exact instance* just matched — so a reply racing a fresh
   `RequestAsync` call that has already claimed the same id (having removed and replaced the entry itself)
   cannot steal that fresh call's slot.

**In every case above, the frame is consumed** (`TryCompletePendingRequest` returns `true` for anything
carrying `Reply == "1"`, matched or not) — a reply frame, forged or genuine, matched or stale, **never**
reaches `MessageReceived`. **The delivery-acknowledgement check (`TryCompletePendingAck`) runs first and is
independent** — a frame can only ever be one or the other, since `mesh.ack=1` and `mesh.reply=1` are never
both present on a frame this library produces; see
[Delivery acknowledgement](#delivery-acknowledgement) below.

**On the receiving side of a genuine incoming request** (not a reply), the same `DeliverMessageWithHeaders`
branch sets `MessageReceivedEventArgs.CorrelationId = TryGetRequestCorrelationId(headers)`
(`MeshClient.cs:1107`, method at `:1602-1612`) — `long?`, `null` for an ordinary message, set for a
request. A handler that finds it set should call `ReplyAsync`, passing the same event args back in. **Since
PR #84**, once that event has been raised (successfully or not), the same branch also fires an automatic
delivery acknowledgement back to the sender if one was requested — see
[Delivery acknowledgement](#delivery-acknowledgement) next.

<a id="delivery-acknowledgement"></a>

### Delivery acknowledgement

PR #84 added `SendAsync(Guid recipientId, ReadOnlyMemory<byte> message, DeliveryOptions options,
CancellationToken cancellationToken = default)` (`IMeshClient.cs:149-153`, implemented
`MeshClient.cs:390-443`) — an opt-in, end-to-end delivery acknowledgement for a single direct message,
built the same way PR #83's request/response was: entirely inside the existing
[header envelope](protocol.md#message-headers), no new opcode, no protocol version bump. `DeliveryOptions`
(`src/AdamSalisbury.Meshworx/DeliveryOptions.cs`) is a small `readonly struct`:

| Member | Meaning |
|---|---|
| `DeliveryOptions.None` | The default value of the struct — identical to the plain `SendAsync` overload. |
| `DeliveryOptions.RequireAck(TimeSpan timeout)` | Requests an acknowledgement; `timeout` must be positive or the call throws `ArgumentOutOfRangeException` immediately, before anything is sent. |

```csharp
byte[] payload = Encoding.UTF8.GetBytes("must arrive");
await alice.SendAsync(bobId, payload, DeliveryOptions.RequireAck(TimeSpan.FromSeconds(5)));
// completes once Bob's MessageReceived has been raised for this message and Bob's client has
// acknowledged it — or throws TimeoutException if that does not happen within 5 seconds.
```

**With `DeliveryOptions.None`, the overload is a one-line forward to the plain `SendAsync`**
(`MeshClient.cs:396-400`) — no correlation id is claimed, no header block is written, and the call behaves
exactly like `SendAsync(recipientId, message, cancellationToken)` always has. Every other bullet below
applies only to `RequireAck`.

**How the `RequireAck` path works** (`MeshClient.cs:402-442`), and it deliberately mirrors `RequestAsync`
(above) rather than inventing a new pattern:
1. Claims a fresh id via `Interlocked.Increment(ref _ackCorrelationId)` — a **separate counter and a
   separate `ConcurrentDictionary<long, PendingAck>` (`_pendingAcks`, fields at `:51-52`)** from
   `RequestAsync`'s `_pendingRequests`/`_requestCorrelationId`, so a reply and an acknowledgement can never
   collide on the same id space.
2. Records `PendingAck(recipientId, completion)` in `_pendingAcks[ackId]` **before** the send (`:408`), for
   the same reason `RequestAsync` does — a fast-arriving acknowledgement must still find an entry.
3. Sends the message via the shared `SendCoreAsync` (`:420`, definition `:468-521`), with a
   `MessageHeaders` carrying `DeliveryAcknowledgementHeaderKeys.CorrelationId` and `.Request = "1"`
   (`Messages/DeliveryAcknowledgementHeaderKeys.cs`, `internal`).
4. Awaits the completion source, bounded by a linked `CancellationTokenSource` cancelled after
   `options.AcknowledgementTimeout` (`:422-434`) — a timeout is translated to `TimeoutException`, a genuine
   external cancellation still surfaces as `OperationCanceledException`.
5. **`finally` always removes the entry from `_pendingAcks`** (`:441`), whether the call succeeded, timed
   out, or was cancelled — so a late acknowledgement for that id is discarded rather than resolving a
   *future* send that happens to reuse it.

**The recipient's client sends the acknowledgement automatically — this is not something the application
calls.** In `ReceiveLoopAsync`'s `DeliverMessageWithHeaders` branch (`MeshClient.cs:1083-1130`), once
`MessageReceived` has been raised for an incoming message (`:1100-1115`, whether or not a subscriber
threw), the branch fires `TrySendAcknowledgementAsync(senderId, headers, cancellationToken)`
(`:1117-1126`, method at `:1549-1596`) if the sender attached `DeliveryAcknowledgementHeaderKeys.Request`.
**This call is fire-and-forget (`_ = TrySendAcknowledgementAsync(...)`), not awaited** — the receive loop's
own inbound frame processing (including the connection's `Ping`/`Pong` keepalive) must not be
head-of-line-blocked behind a slow or stalled write back to the sender. `TrySendAcknowledgementAsync`
swallows every exception internally (`:1584-1596`, a callback/detached-task boundary, the same reasoning
already applied to a throwing `MessageReceived` subscriber) — there is nothing for the caller to observe,
by design.

**How an incoming acknowledgement is matched, and why a hostile peer cannot forge one** (`TryCompletePendingAck`,
`MeshClient.cs:1502-1547`, called from the receive loop's `DeliverMessageWithHeaders` branch at `:1096`,
**before** `TryCompletePendingRequest` runs and **before** `MessageReceived` is ever raised for that
frame) — the logic is the acknowledgement mirror of `TryCompletePendingRequest`, checked field-for-field:
a frame without `Ack == "1"` is not an acknowledgement (returns `false`); a missing/malformed correlation
id is logged and discarded; an unmatched or expired correlation id is logged at `Debug` and discarded; an
acknowledgement from any sender **other than** `PendingAck.ExpectedAcknowledgerId` is logged at `Warning`
and discarded **without removing the pending entry**, so a forged acknowledgement cannot strand the real
send while the genuinely addressed recipient's acknowledgement can still arrive and complete it; and a
genuine match resolves via a compare-and-remove against the exact instance just matched, so it cannot
steal a slot a concurrent `RequireAck` send has already claimed for the same id. **Every frame carrying
`Ack == "1"` is consumed** — matched or not, it never reaches `MessageReceived`.

**Contract & gotchas:**
- **A connection drop faults every outstanding `RequireAck` call** with `InvalidOperationException` ("The
  connection was closed before an acknowledgement arrived.") from the receive loop's termination `finally`
  (`MeshClient.cs:1289-1297`) — the same treatment `RequestAsync` gets (`:1278-1287`). A caller on a
  non-cancellable token is not left hanging past the connection's own teardown.
- **"Acknowledged" means "handed to the application", not "handled successfully".** The acknowledgement is
  sent once `MessageReceived?.Invoke(...)` has returned, *regardless of whether a subscriber threw*
  (`MeshClient.cs:1100-1126`) — a throwing handler still results in the sender's `RequireAck` call
  completing successfully. See [known-issues.md](known-issues.md) KI-44.
- **A `TimeoutException` from `RequireAck` does not prove the message was not delivered.** The
  acknowledgement is an ordinary routed message, subject to the same silent-drop paths as any other send —
  a full outbound queue on the hub (KI-1) or the connection dropping between delivery and the
  acknowledgement being sent can both produce a timeout on a message that genuinely arrived. See
  [known-issues.md](known-issues.md) KI-45.
- **The hub is completely unaware of delivery acknowledgement**, for exactly the same reason it is unaware
  of request/response (see below): it never inspects `MessageHeaders` content, only the header block's
  length. See [known-issues.md](known-issues.md) KI-46.

**Contract & gotchas:**
- **Concurrent `RequestAsync` calls on the same client are fully independent** — each is tracked by its
  own correlation id in the `ConcurrentDictionary`; there is no equivalent of `GetClientIdByNameAsync`'s
  single-slot serialisation. Fire many at once.
- **Group messages cannot be requests, replies, or acknowledgements.** `RequestAsync` and
  `SendAsync(..., DeliveryOptions, ...)` only ever take a single `recipientId`; `SendToGroupAsync`'s
  headers overload does not go through `SendCoreAsync` and is not gated by
  `ThrowIfReservedHeaderKeyPresent`. The `DeliverGroupMessageWithHeaders` receive-loop branch
  (`MeshClient.cs:1131-1172`) is untouched by PR #83 and PR #84 alike — it still just raises
  `GroupMessageReceived` inside a plain `try/catch`, with no `CorrelationId` or acknowledgement concept at
  all.
- **A connection drop faults every outstanding `RequestAsync` call** with `InvalidOperationException`
  ("The connection was closed before a reply arrived.") from the receive loop's termination `finally`
  (`MeshClient.cs:1278-1287`) — see [The receive loop](#the-receive-loop) above. **Since PR #84, the same
  `finally` does the equivalent for every outstanding `SendAsync(..., DeliveryOptions.RequireAck(...))`
  call** (`:1289-1297`, message "The connection was closed before an acknowledgement arrived."). A caller
  on a non-cancellable token is not left hanging past the connection's own teardown, for either helper.
- **The six reserved header keys cannot be set through the public `SendAsync(headers)` overload** —
  `ThrowIfReservedHeaderKeyPresent` (`:543-555`) throws `ArgumentException` if the caller's own
  `MessageHeaders` contains any of them. This is a genuine new constraint on a previously unrestricted
  parameter: an application already using one of the literal strings `"mesh.request-id"`/`"mesh.reply"`
  (before PR #83), `"mesh.ack-id"`/`"mesh.ack-request"`/`"mesh.ack"` (before PR #84), or
  `"mesh.expires-at"` (before PR #85) as one of its own header keys will now see `SendAsync` throw where
  it previously succeeded. See [known-issues.md](known-issues.md) KI-42.
- **The hub is completely unaware of request/response or delivery acknowledgement.** It never inspects
  `MessageHeaders` (headers ride opaque, same as always — see
  [protocol.md](protocol.md#message-headers)), so `RequestAsync`/`ReplyAsync` and
  `SendAsync(..., DeliveryOptions, ...)` are both pure client-to-client conventions enforced only by the
  sending `MeshClient`'s own guard and the receiving `MeshClient`'s own `TryCompletePendingRequest`/
  `TryCompletePendingAck` checks. A peer that is not built on this library — or a `MeshClient` caller that
  bypasses `SendAsync` entirely and somehow gets a raw `SendMessageWithHeaders` frame onto the wire — can
  still produce a frame carrying `Reply = "1"` or `Ack = "1"`, which **any** receiving `MeshClient` will
  intercept and drop before `MessageReceived`, whether or not it matches a real pending request or
  acknowledgement. See [known-issues.md](known-issues.md) KI-43 and KI-46.

<a id="message-expiry-time-to-live"></a>

### Message expiry (time-to-live)

PR #85 (issue #29) added `SendAsync(Guid recipientId, ReadOnlyMemory<byte> message, TimeSpan timeToLive,
CancellationToken cancellationToken = default)` (`IMeshClient.cs:182-186`, implemented
`MeshClient.cs:446-466`) — an opt-in, per-message time-to-live. Built the same "fourth route" way as
PR #83's request/response and PR #84's delivery acknowledgement: entirely inside the existing
[header envelope](protocol.md#message-headers), no new opcode, no protocol version bump. One new
`internal` well-known key, `Messages/MessageExpiryHeaderKeys.cs`: `ExpiresAtUnixMilliseconds`, wire string
`"mesh.expires-at"`.

```csharp
byte[] payload = Encoding.UTF8.GetBytes("only useful for the next five seconds");
await alice.SendAsync(bobId, payload, TimeSpan.FromSeconds(5));
// if this has not reached Bob's client within 5 seconds of the call, it is discarded rather than
// delivered stale — Bob's MessageReceived never fires for it, and Alice's call does not learn this
// happened (delivery is still fire-and-forget, exactly as the plain SendAsync overload is).
```

**How it works** (`MeshClient.cs:446-466`):
1. Rejects a non-positive `timeToLive` with `ArgumentOutOfRangeException` before sending anything
   (`:452-455`).
2. Computes an **absolute** expiry instant, `DateTimeOffset.UtcNow.Add(timeToLive).ToUnixTimeMilliseconds()`
   (`:457`), measured against **this client's own clock** at the moment of the call — there is no hub
   clock authority; see the clock-skew caveat below.
3. Attaches it as the sole entry of a `MessageHeaders` under `ExpiresAtUnixMilliseconds`, formatted as an
   invariant-culture integer (`:458-463`), and sends via the shared `SendCoreAsync` (`:465`, definition
   `:468-521`) — the same private helper `RequestAsync`, `ReplyAsync`, the `RequireAck` branch of
   `SendAsync(..., DeliveryOptions, ...)` and the receive loop's automatic acknowledgement reply all use.
   There is no pending-completion table and no correlation id: unlike request/response and delivery
   acknowledgement, this overload's returned task completes once the hub has accepted the message — it
   does not itself wait to learn whether the message was later dropped for having expired.

**Parsing is shared and deliberately never throws.** `MessageExpiryHeaderKeys.TryParseExpiry`
(`Messages/MessageExpiryHeaderKeys.cs:36-56`) is called by both this client's own receive loop and
`MeshHub`'s send loop (see [hub.md](hub.md#dropping-expired-frames)), so the two never drift apart on how
a bad value is treated. Absent, non-numeric, or numeric-but-out-of-range (e.g. `long.MaxValue`, which
parses fine but is far outside what `DateTimeOffset.FromUnixTimeMilliseconds` can represent) all mean
"does not expire" — identical to a message with no time-to-live at all. The value comes from the sender's
own header block and is otherwise unvalidated, so this must never throw and crash whichever loop is
checking it; a malformed *header block* itself (as opposed to a malformed expiry value inside a
well-formed block) is a separate concern already handled by `TryReadHeaderBlock`/`HeaderEnvelope.Read`.

**On receipt, an already-expired message is dropped before the application ever sees it — on both
message shapes, not just direct.** `IsExpired(MessageHeaders headers, Guid senderId)`
(`MeshClient.cs:1407-1422`) is the client-side check, called from **two** places in the receive loop:
- `DeliverMessageWithHeaders`'s three-condition check (`:1096-1098`) — `!IsExpired(...)` is the **third**
  condition, evaluated after the request/response and delivery-acknowledgement interceptions, so it only
  ever applies to a message that was headed for `MessageReceived` in the first place. See
  [The receive loop](#the-receive-loop) above.
- `DeliverGroupMessageWithHeaders`'s single condition (`:1150`) — added by this pass's own second commit
  ("apply the expiry check to group messages"), which was **not** present in the first commit: the
  original change only guarded the direct-message branch, and an expired group message would still have
  reached `GroupMessageReceived` until this fix landed within the same PR.

An expired message is logged at `Debug` (`MeshClient.cs:1420`, `"Discarding an expired message from
{SenderId}"`) and silently dropped — no event fires for it on either branch, and the sender is not
notified (delivery remains fire-and-forget, exactly as an un-expired plain send is).

**The hub also drops an expired frame, independently, while it is still queued** — see
[hub.md](hub.md#dropping-expired-frames). A message can therefore be discarded at either end: by the hub
before it ever reaches the recipient's transport, or by the recipient's own receive loop if it arrives
just after expiring. Either way the effect on the application is identical — the message never surfaces.

**Contract & gotchas:**
- **There is no hub clock authority — this is a genuine, documented clock-skew caveat, not an edge case.**
  The expiry is computed from the *sender's* clock and compared against the *hub's* and the *recipient's*
  own clocks independently. If the sender's clock runs fast, a message may be discarded earlier than the
  sender intended (relative to wall-clock time the sender thinks it is using); if it runs slow, a message
  may survive longer than intended, or the receiving ends' comparisons may simply disagree with each
  other. Meaningful use of a short time-to-live assumes the clocks involved are reasonably synchronised
  (for example via NTP). See [known-issues.md](known-issues.md) KI-47.
- **There is no `SendToGroupAsync(..., TimeSpan, ...)` overload.** Time-to-live is direct-message-only on
  the sending side; only the *receiving* side of a group message is guarded (the `DeliverGroupMessageWithHeaders`
  bullet above) — relevant only if some other sender (a hand-built frame, or a future overload) ever
  attaches the header to a group send.
- **A message can pass the hub's expiry check and still expire before the recipient's own check runs** —
  the two checks are independent and use independent clock reads at different instants (queued-frame
  dequeue time at the hub; frame-processing time at the recipient). This is expected, not a bug: both
  checks exist to *reduce* stale delivery, not to guarantee an exact cutoff.
- **`SendAsync(..., TimeSpan, ...)` bypasses `ThrowIfReservedHeaderKeyPresent`** the same way
  `RequestAsync`, `ReplyAsync` and the `RequireAck` branch do — it is a legitimate producer of the
  `"mesh.expires-at"` key via `SendCoreAsync` directly, not the public headers overload.

### `GetClientIdByNameAsync` — the correlated lookup

`MeshClient.cs:798`. Serialised by `_lookupLock` (`SemaphoreSlim(1,1)`): **one lookup in flight at a
time per client**; concurrent callers queue. Each request carries a 4-byte correlation id (`unchecked`
increment, `:40-41`, `:818`). A single-slot `_pendingLookup` (`PendingLookup(correlationId,
TaskCompletionSource<Guid?>)`, declared at `:1622`, assigned at `:820`) is completed by the receive loop
**only when the ids match** (`:1199-1220`) — so a late response from a cancelled lookup cannot resolve a
subsequent one. Returns `null` when the hub reports "not found". Cancelling via the token abandons the
wait; the `finally` clears `_pendingLookup` and releases the lock (`:831-843`).

This lookup's own single-slot `_pendingLookup`/correlation-id scheme is **not** what `RequestAsync` or
`SendAsync(..., DeliveryOptions, ...)` use — both need to support multiple calls in flight at once, so
each has its own, separate `ConcurrentDictionary`-backed table: `_pendingRequests` (`:46-47`, PR #83) and
`_pendingAcks` (`:51-52`, PR #84). See [Request/response (RPC)](#request-response) and
[Delivery acknowledgement](#delivery-acknowledgement).

### Threading & idempotency

- `_stateLock` guards state, the transport/cts references and the `_localDisconnectRequested` claim flag.
  `_groupMembershipLock` guards `_joinedGroups`. `_lookupLock` serialises lookups.
- `DisconnectAsync` and `DisposeAsync` are safe to call when not connected (early return). Note that
  `DisconnectAsync`'s early return is not *quite* inert: in the `Disconnecting` state it claims the
  in-flight teardown (`MeshClient.cs:312-315`). It is still idempotent and side-effect-free from the
  caller's point of view.
- Send methods snapshot the transport under `_stateLock`, then release it before the `await SendAsync`
  — so a slow send does not hold the state lock.
- **`_pendingRequests` (PR #83) needs no lock of its own** — it is a `ConcurrentDictionary<long,
  PendingRequest>` (`:46`), so concurrent `RequestAsync` calls and the single receive loop's
  `TryCompletePendingRequest` all touch it safely without going through `_stateLock`. Correlation ids come
  from `Interlocked.Increment(ref _requestCorrelationId)` (`:47`, `:858`), so concurrent `RequestAsync`
  calls on the same client never collide on an id.
- **`_pendingAcks` (PR #84) is the same shape and needs no lock of its own either** — a second, independent
  `ConcurrentDictionary<long, PendingAck>` (`:51`) with its own correlation counter,
  `_ackCorrelationId` (`:52`, incremented at `:402`). Concurrent `SendAsync(..., DeliveryOptions, ...)`
  calls, concurrent `RequestAsync` calls, and the single receive loop all touch their respective tables
  independently — none of the three shares a lock with either of the others.

---

## `MeshClientReconnector`

Keeps an `IMeshClient` connected, transparently re-establishing on unexpected drops. It owns **only the
connection lifecycle** — you still use `reconnector.Client` to send/receive.

### Surface

| Member | Notes | Source |
|---|---|---|
| ctor | `(IMeshClient client, string clientName, Func<CancellationToken,Task<ITransport>> transportFactory, TimeSpan? retryDelay=null, TimeSpan? connectTimeout=null, bool restoreGroupMembership=true, ILogger<MeshClientReconnector>?=null, ReadOnlyMemory<byte> credential=default)` | `MeshClientReconnector.cs:86` |
| `Client` | `IMeshClient` — the managed client | `MeshClientReconnector.cs:128` |
| `StartAsync` | Fail-fast initial connect; then begins monitoring. Throws if already started or if the first connect fails (retryable). | `MeshClientReconnector.cs:164` |
| `Reconnected` | `event EventHandler` — raised after a re-established connection | `MeshClientReconnector.cs:143` |
| `GetMeterForTesting` | `internal Meter` — the `Meter` this reconnector publishes `meshworx.client.reconnects` to (PR #72, issue #24). Internal so a test can filter a `MeterListener` to exactly this instance | `MeshClientReconnector.cs:154` |
| `DisposeAsync` | Stops monitoring, unsubscribes, disconnects the client | `MeshClientReconnector.cs:358` |

### How it works

- **`transportFactory` produces a fresh transport per attempt** — because the client consumes/disposes a
  transport per connection. Defaults: `retryDelay` 1 s, `connectTimeout` 10 s.
- **The `credential` is stored and re-sent on every connect and reconnect**
  (`MeshClientReconnector.cs:112`, `:287`), so an authenticated client keeps its credential across drops
  without any work from you. It is captured once at construction: **there is no way to rotate it** on a
  live reconnector — a credential that expires mid-session will cause every subsequent reconnect attempt
  to fail with `AuthenticationFailed`, retried at `retryDelay` forever. If your credentials expire,
  dispose the reconnector and build a new one with the fresh credential.
- `StartAsync` (`MeshClientReconnector.cs:164`) does one bounded connect; **throws on failure** and
  resets the started flag so it can be retried (`:179-185`). On success it subscribes `OnDisconnected`
  to `Client.Disconnected` (`:187`), then **re-reads `Client.IsConnected` and queues a reconnect signal
  itself if the connection has already gone** (`:200-203`), and only then starts the loop.
  That re-read is not belt-and-braces — it closes a race. `Client.ConnectAsync` returns with the
  client's receive loop already running on a background task, so a drop landing between it returning and
  the subscription line is raised with **no subscriber**: the event is genuinely lost. Without the
  re-read nothing signals the loop and it parks on `WaitToReadAsync` for ever, leaving a permanently
  disconnected client that never recovers (issue #8, fixed in PR #60). The client resets itself to a
  disconnected state *before* raising `Disconnected`, which is what makes the state re-read a reliable
  detector of a drop in that window.
- `OnDisconnected` (`:208`) just `TryWrite`s to a **capacity-1 `DropWrite` channel** (`:225`) — disconnect
  notifications are coalesced (many drops → at most one queued reconnect) and the client's receive loop
  is never blocked.
- `ReconnectLoopAsync` (`:228`) drains the signal, calls `ConnectWithRetryAsync`, then raises
  `Reconnected` (throwing handler logged, loop survives).
- `ConnectWithRetryAsync` (`MeshClientReconnector.cs:263`) retries each bounded attempt after
  `retryDelay` until it succeeds or the reconnector is disposed. Two guards inside it are load-bearing,
  not incidental — both were added by PR #60 and removing either reintroduces a hang or a leak:
  - **It returns immediately if `Client.IsConnected` is already true** (`:273-276`). The trigger is
    **level-based, not edge-based**: a queued signal records that the connection *was* lost, not that it
    still is. Read [known-issues.md](known-issues.md) KI-19 before touching this.
  - **It disposes the transport the factory produced if `Client.ConnectAsync` rejects it** (`:289-296`),
    because the client only takes ownership once it accepts the transport. See KI-20 — note the same
    guard is **not** present on `StartAsync`'s connect (`:171-185`).
  - **Only once this method has itself re-established the connection does it increment
    `meshworx.client.reconnects`** (`:301`, PR #72) — the early return above, for a drop signal that
    turned out stale, never reaches it, so a no-op reconnect pass is never counted. See
    [Metrics](#metrics) below.

<a id="metrics"></a>

### Metrics

Added by PR #72 (issue #24). `MeshClientReconnector` owns its own `System.Diagnostics.Metrics.Meter`
(`_meter`, `MeshClientReconnector.cs:61`), created inline at field-initialisation time rather than in the
constructor body, and disposed in `DisposeAsync` (`:382`). It is named via the same shared internal
constant `MeshHub` uses (`MeshworxMeterName.Value`, `"AdamSalisbury.Meshworx"`), so one
`AddMeter("AdamSalisbury.Meshworx")` call collects both components' instruments — see
[hub.md](hub.md#metrics). `GetMeterForTesting()` (`:154-157`) exposes the instance for a test to filter a
`MeterListener` to exactly this reconnector, the same reasoning as the equivalent method on `MeshHub`.

The one instrument, `meshworx.client.reconnects` (`Counter<long>`, created `:119-122`), is incremented
**only** at `:301` inside `ConnectWithRetryAsync`, immediately after `Client.ConnectAsync` succeeds and
before the early return — see the bullet above. Two connect paths that are **not** counted:

- **`StartAsync`'s own initial connect never reaches `ConnectWithRetryAsync` at all** — `StartAsync`
  calls `Client.ConnectAsync` directly (`:177`), so the first connection a reconnector makes is never
  recorded as a reconnect, only genuine re-establishments after a drop are.
- **A stale reconnect signal that turns out to be a no-op** (`Client.IsConnected` already true when
  `ConnectWithRetryAsync` is entered, `:273-276`) returns before the increment, so a duplicate signal or
  a drop an application handler already recovered from does not inflate the count.

Pinned by `MeshClientReconnectorMetricsTests.cs` — see [testing.md](testing.md#metrics-tests), whose one
test asserts both exclusions directly: `capture.Values` is empty immediately after `StartAsync`, and
reads exactly `[1L]` after one genuine drop-and-reconnect.

### Contract & gotcha

- **It DOES re-join groups by default; it does not re-send in-flight messages.** `restoreGroupMembership`
  defaults to `true`, and `RestoreGroupMembershipAsync` (`MeshClientReconnector.cs:316`) re-joins each
  pending group by calling `Client.JoinGroupAsync` (`:335`). Pass `restoreGroupMembership: false` to take
  manual control. In-flight messages are never re-sent — that part remains your responsibility.
  (This corrects a claim that predated PR #52; the type's `<remarks>` state it, `:20-25`.)
- **Restoration re-joins over the wire, so every re-join is authorised afresh.** A hub with a
  `GroupAuthoriser` sees each restored join as a new request and may refuse it — a restore cannot
  reinstate a membership the hub would now deny. A refused group is dropped from the client's membership
  and is **not** retried; `IMeshClient.GroupJoinRefused` fires so the application can decide what to do.
  See [hub.md](hub.md#group-authorisation).
- `Reconnected` fires after every successful re-establish but **not** after the initial `StartAsync`
  connect.
- **`Reconnected` can fire without this reconnector having reconnected anything.** Since the loop treats
  an already-connected client as the goal met (`MeshClientReconnector.cs:273-276`), a signal that has
  gone stale — a duplicate for a drop already serviced, or a drop an application `Disconnected` handler
  recovered from itself — still runs the loop through to raising `Reconnected`. Treat the event as
  "the connection is up again", not as "I re-established it", and make your handlers safe to run more
  than once for a single drop. See [known-issues.md](known-issues.md) KI-19. Note that `Reconnected` and
  `meshworx.client.reconnects` are not the same signal: the metric only increments when
  `ConnectWithRetryAsync` itself did the reconnecting, whereas `Reconnected` fires on this same stale-goal-met
  path too — the counter is the stricter of the two.

```csharp
await using var reconnector = new MeshClientReconnector(
    new MeshClient(logger),
    "Alice",
    async ct => await TcpTransport.ConnectAsync("localhost", 22001, ct));

// Against a TLS hub the factory simply calls the TLS overload, so every reconnect renegotiates:
//   async ct => (ITransport)await TcpTransport.ConnectAsync("hub.example.com", 22001, tlsOptions, ct)

// Group membership is restored automatically. Watch for a re-join the hub refuses:
reconnector.Client.GroupJoinRefused += (_, e) =>
    logger.LogWarning("No longer permitted in {Group}", e.GroupName);

await reconnector.StartAsync();
await reconnector.Client.SendAsync(recipientId, payload);
```

Tested in `MeshClientReconnectorTests.cs` (944 lines) plus, for the metrics added by PR #72,
`MeshClientReconnectorMetricsTests.cs` — see [testing.md](testing.md#metrics-tests).
