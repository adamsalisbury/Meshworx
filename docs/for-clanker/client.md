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
| ctor | `MeshClient(ILogger<MeshClient>, TimeSpan? idleTimeout=null, TimeSpan? sendTimeout=null, int maxSendAttempts=1, TimeSpan? sendRetryDelay=null)` | `MeshClient.cs:85` |
| `Id` | `Guid` — assigned by hub; `Guid.Empty` when disconnected | `MeshClient.cs:126` |
| `Name` | `string` — set on connect, cleared on disconnect | `MeshClient.cs:129` |
| `IsConnected` | `bool` — true only in `Connected` state | `MeshClient.cs:135` |
| `JoinedGroups` | `IReadOnlyCollection<string>` — **snapshot** of client-side membership | `MeshClient.cs:147` |
| `ConnectAsync` | `Task ConnectAsync(ITransport, string clientName, ReadOnlyMemory<byte> credential=default, CancellationToken=default)` | `MeshClient.cs:171` |
| `NegotiatedProtocolVersion` | `byte` — the wire-protocol version agreed with the hub during the last successful `ConnectAsync`; `0` when not connected | `MeshClient.cs:132` |
| `DisconnectAsync` | `Task DisconnectAsync(CancellationToken=default)` — graceful; no `Disconnected` event | `MeshClient.cs:289` |
| `SendAsync` | `Task SendAsync(Guid recipientId, ReadOnlyMemory<byte>, CancellationToken=default)` — compatibility overload, forwards to the headers overload with `MessageHeaders.Empty` | `MeshClient.cs:363` |
| `SendAsync` (headers) | `Task SendAsync(Guid recipientId, ReadOnlyMemory<byte>, MessageHeaders headers, CancellationToken=default)` — PR #74 (issue #32); **throws `ArgumentException` if `headers` contains either of the two reserved request/reply keys** (PR #83); see [Sending headers](#sending-headers) | `MeshClient.cs:372` |
| `BroadcastAsync` | `Task BroadcastAsync(ReadOnlyMemory<byte>, CancellationToken=default)` | `MeshClient.cs:455` |
| `JoinGroupAsync` / `LeaveGroupAsync` | `Task ...(string groupName, CancellationToken=default)` — **optimistic**: `JoinGroupAsync` records membership *before* sending and the hub may still refuse, see [Group membership](#group-membership) | `MeshClient.cs:477` / `:518` |
| `SendToGroupAsync` | `Task SendToGroupAsync(string groupName, ReadOnlyMemory<byte>, CancellationToken=default)` — compatibility overload, forwards to the headers overload with `MessageHeaders.Empty` | `MeshClient.cs:534` |
| `SendToGroupAsync` (headers) | `Task SendToGroupAsync(string groupName, ReadOnlyMemory<byte>, MessageHeaders headers, CancellationToken=default)` — PR #74 (issue #32); see [Sending headers](#sending-headers) | `MeshClient.cs:543` |
| `GetClientIdByNameAsync` | `Task<Guid?> GetClientIdByNameAsync(string name, CancellationToken=default)` | `MeshClient.cs:695` |
| `RequestAsync` | `Task<ReadOnlyMemory<byte>> RequestAsync(Guid recipientId, ReadOnlyMemory<byte>, TimeSpan timeout, CancellationToken=default)` — PR #83; correlated request/reply over a direct message, see [Request/response (RPC)](#request-response) | `MeshClient.cs:744` |
| `ReplyAsync` | `Task ReplyAsync(MessageReceivedEventArgs request, ReadOnlyMemory<byte>, CancellationToken=default)` — PR #83; answers a request received via `MessageReceived`, see [Request/response (RPC)](#request-response) | `MeshClient.cs:801` |
| `MessageReceived` | `event EventHandler<MessageReceivedEventArgs>` — direct **and** broadcast; `Headers` populated when the sender attached any (PR #74); `CorrelationId` set when the message is a request awaiting a reply (PR #83) | `MeshClient.cs:159` |
| `GroupMessageReceived` | `event EventHandler<GroupMessageReceivedEventArgs>` — carries group name; `Headers` populated when the sender attached any (PR #74) | `MeshClient.cs:162` |
| `GroupJoinRefused` | `event EventHandler<GroupJoinRefusedEventArgs>` — the hub refused a join; the group has **already** been removed from `JoinedGroups` when this fires | `MeshClient.cs:165` |
| `Disconnected` | `event EventHandler<DisconnectedEventArgs>` — **unexpected** endings only | `MeshClient.cs:168` |
| `DisposeAsync` | `ValueTask` — `DisconnectAsync` then disposes the lookup semaphore | `MeshClient.cs:825` |

> **Coordinate caveat — resolved for PR #73, re-pointed for PR #74, re-pointed again for PR #83.** Every
> row in this table, and every `MeshClient.cs`/`IMeshClient.cs` citation in the rest of this file, was
> re-derived from the current source as of PR #83 (the `RequestAsync`/`ReplyAsync` helper), which grew
> `MeshClient.cs` by a net 225 lines across nine separate insertion points — a two-`using` addition at the
> top (+2), the `_pendingRequests`/`_requestCorrelationId` fields (+6), the `SendAsync`/`SendCoreAsync`
> split plus the reserved-key guard (+17), `RequireHeaderEnvelopeSupport`'s move into `SendCoreAsync`
> (+19), the whole `RequestAsync`/`ReplyAsync` pair (+81), the receive loop's `TryCompletePendingRequest`
> wrap around the `DeliverMessageWithHeaders` branch (+4), the termination `finally`'s pending-request
> fault-out (+11), `TryCompletePendingRequest`/`TryGetRequestCorrelationId` (+78), and the new
> `PendingRequest` record (+7) — each verified against the source individually rather than computed from
> a single offset (see the [index](../for-clanker.md) for the full shift map). `IMeshClient.cs` grew by 53
> lines in one place (the `RequestAsync`/`ReplyAsync` interface members), after every citation already in
> this file, so none of them moved. Names and behaviour were already accurate as of the PR #74 pass; only
> the line numbers moved, except where noted inline below.

### Lifecycle & state machine

Internal `enum ConnectionState { Disconnected, Connecting, Connected, Disconnecting }`
(`MeshClient.cs:1347`), guarded by `_stateLock` (`System.Threading.Lock`). Send/lookup/group methods
throw `InvalidOperationException("Not connected to a hub.")` unless `Connected`.

**`ConnectAsync`** (`MeshClient.cs:171`):
1. Validates `transport`/`clientName`, rejects names longer than 256 chars, and refuses to connect
   unless currently `Disconnected` (state-specific message otherwise).
2. Sends `RegistrationRequest` (`[0x04][versionMin][versionMax][nameLen u16 BE][utf8 name][credential]`,
   `MeshClient.cs:206-214`), always advertising `Protocol.MinSupportedVersion`/`MaxSupportedVersion`
   (`4`/`5` as of PR #74), then awaits one frame.
3. If the reply is `Error`, throws `RegistrationRefusedException` carrying the
   `RegistrationErrorCode` — which now includes `AuthenticationFailed` if the hub's authenticator
   rejected the credential, and (unchanged) `UnsupportedProtocolVersion` if the hub could not negotiate a
   version in this client's range. If the reply is not a well-formed `RegistrationComplete` (exactly
   18 bytes as of PR #73, was 17), throws `InvalidOperationException`.
4. Records `Id` and `NegotiatedProtocolVersion` (the hub's reply's trailing byte, `MeshClient.cs:233-235`
   — `0` until a successful connect), sets `Connected`, and starts `ReceiveLoopAsync`.
5. On **any** failure it cleans up (disposes the transport), resets to `Disconnected` (and
   `NegotiatedProtocolVersion` to `0`), logs, and rethrows.

> **`NegotiatedProtocolVersion` is read by the send path since PR #74 (issue #32), not just logged.**
> `SendAsync`'s headers overload — and, since PR #83, `RequestAsync`/`ReplyAsync` too, which share the
> same internal `SendCoreAsync` (`MeshClient.cs:391-433`) — calls `RequireHeaderEnvelopeSupport`
> (call site `:421`, definition `:593-602`), which throws `NotSupportedException` if this connection's
> negotiated version is below `Protocol.HeaderEnvelopeMinVersion` — see
> [Sending headers](#sending-headers) and [Request/response (RPC)](#request-response).
> `SendToGroupAsync`'s headers overload has its own separate call to the same check (`:573`); group sends
> do not go through `SendCoreAsync` and cannot participate in the request/response pattern (`RequestAsync`
> only ever addresses a single `recipientId`). This resolves [known-issues.md](known-issues.md) KI-14 for
> the header envelope specifically; a future capability gated the same way would need its own check.

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

There is a subtle **synchronous-completion guard** (`MeshClient.cs:236-256`): if the hub has already
buffered a `Disconnect`, `ReceiveLoopAsync` can run to completion synchronously and a `Disconnected`
handler may reconnect from within it, replacing `_cts`. The code only records `_receiveLoopTask` if
`_cts` is still the one it created, so a stale synchronous loop never clobbers a newer connection.
Preserve this reference-equality check if you refactor connect.

### The receive loop

`ReceiveLoopAsync` (`MeshClient.cs:858`) is the single reader. It:
- Sets `AsyncLocal<bool> _inReceiveLoop = true` so a `DisconnectAsync` invoked **from a handler** skips
  awaiting the loop (would deadlock) — see below.
- Runs an optional **idle monitor** (`MonitorIdleAsync`, `MeshClient.cs:883`) on a `PeriodicTimer`,
  comparing an `activitySequence` counter between ticks; on a fully idle interval it cancels the loop's
  linked CTS, ending the connection as `ConnectionLost`.
- Dispatches inbound frames: `DeliverMessage` → `MessageReceived`; `DeliverGroupMessage` →
  `GroupMessageReceived`; `DeliverMessageWithHeaders`/`DeliverGroupMessageWithHeaders` (PR #74, issue #32)
  → the same two events with `Headers` populated from a decoded `MessageHeaders`, via `TryReadHeaderBlock`
  (`:1255-1267`, see [protocol.md](protocol.md#message-headers)); `GroupJoinRefused` → removes the group
  from `_joinedGroups`, logs a `Warning`, then raises `GroupJoinRefused` (`MeshClient.cs:1057-1082`);
  `ClientLookupResponse` → completes the pending lookup (if correlation matches, `:1083-1104`); `Ping` →
  replies `Pong` (best-effort); `Disconnect` → sets reason `RemoteDisconnect` and breaks.
- Wraps each handler invocation in `try/catch` and logs a throwing subscriber (callback boundary) so it
  cannot halt delivery (`MeshClient.cs:938-951`, `:964-977`). **The two header-bearing branches are no
  longer identical to each other since PR #83.** `DeliverGroupMessageWithHeaders` still just raises
  `GroupMessageReceived` inside a plain `try/catch` — group sends cannot participate in request/response
  (`RequestAsync` only ever addresses a single `recipientId`). `DeliverMessageWithHeaders`
  (`MeshClient.cs:980-1014`) is now gated by `TryCompletePendingRequest(senderId, headers, messageData)`
  (`:993`, method at `:1284-1329`) **before** it raises `MessageReceived` at all: a frame that is a reply
  to one of this client's own `RequestAsync` calls is resolved internally and never surfaces through the
  event, and the surviving `MessageReceived?.Invoke(...)` call (`:995-1010`) now also sets
  `CorrelationId = TryGetRequestCorrelationId(headers)` (`:1335-1345`) so a handler receiving a genuine
  incoming request knows to answer it with `ReplyAsync`. See
  [Request/response (RPC)](#request-response) for the full behaviour.
- On termination (`finally`, `MeshClient.cs:1139-1175`): stops the idle monitor, **faults any pending
  lookup** with `InvalidOperationException` so a caller on a non-cancellable token is not left hanging
  and `_lookupLock` is released; **since PR #83, also faults every still-pending `RequestAsync` call**
  the same way (`:1162-1171`) and clears `_pendingRequests`, so a connection that drops mid-request does
  not leave a caller awaiting a reply forever; then calls `HandleReceiveLoopTerminationAsync`.

`HandleReceiveLoopTerminationAsync` (`MeshClient.cs:1186`) decides whether the ending raises
`Disconnected`. There are **two** gates and both must pass:

1. **The entry gate** (`MeshClient.cs:1188-1199`). Under `_stateLock`, the teardown claims the connection
   by moving `Connected` → `Disconnecting`. If the state was anything other than `Connected`, a local
   `DisconnectAsync` already owns the teardown, so the loop returns immediately and stays silent.
2. **The claim gate** (`MeshClient.cs:1211-1231`). After `CleanUpAsync`, the loop reads
   `_localDisconnectRequested` into a local `raiseDisconnected` **in the same locked block that
   publishes `_state = ConnectionState.Disconnected`** (`MeshClient.cs:1213-1224`). If a `DisconnectAsync`
   claimed the teardown while it was in flight, the loop logs at Debug and returns without raising
   (`MeshClient.cs:1226-1231`).

Gate 1 alone used to be the whole mechanism, and it was **not sufficient**. If the receive loop won the
race out of `Connected`, a concurrent `DisconnectAsync` found the client already `Disconnecting`,
returned as a silent no-op, and the loop went on to raise `Disconnected(ConnectionLost)` for a
disconnect the application had itself requested — issue #10, fixed by PR #62.

#### The claim protocol (load-bearing)

`DisconnectAsync`'s early return is no longer a pure no-op (`MeshClient.cs:293-311`): when it finds the
state is `Disconnecting` it sets `_localDisconnectRequested = true`, claiming the in-flight teardown so
that it stays silent. The flag is a plain `bool` guarded by `_stateLock` (`MeshClient.cs:30`).

Because the claim and the loop's read of it are taken under that same lock, the outcome is decided
atomically: either the claim lands before the loop publishes the disconnected state and the event is
suppressed, or the state is already `Disconnected`, the decision has been taken, and there is nothing
left to claim. `ConnectAsync` clears the flag in the same locked block that moves the state to
`Connecting` (`MeshClient.cs:200`), so an unconsumed claim — a redundant second `DisconnectAsync`, say —
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
  above). The interface XML docs state this contract (`IMeshClient.cs:70-83`, `:249-259`).
  - **The one exception** is a narrow residual window: a `DisconnectAsync` arriving *after* the teardown
    has published the disconnected state has nothing left to claim, and the event fires. Read
    [known-issues.md](known-issues.md) KI-21 before you rely on the suppression being absolute.
- When it fires the client has **already reset** to `Disconnected`, so a handler may immediately call
  `ConnectAsync` again (this is how the reconnector works, and it is a supported pattern). This
  pattern is also *why* KI-21 is left open: closing it would require invoking the event under
  `_stateLock`, which would deadlock a handler that reconnects synchronously.
- **Deadlock-safety:** you may call `DisconnectAsync` from inside a `MessageReceived` or `Disconnected`
  handler. The `_inReceiveLoop` `AsyncLocal` flows into the synchronous handler and makes
  `DisconnectAsync` skip `await`-ing the receive loop task (`MeshClient.cs:337`). Do not remove this.

<a id="group-membership"></a>

### Group membership — optimistic, and revocable by the hub

`JoinGroupAsync` (`MeshClient.cs:477`) is **fire-and-forget with an optimistic local record**. The order
of operations changed in PR #66 and the new order is load-bearing:

1. Validate the name and grab the connected transport (`MeshClient.cs:479-481`) — both *before* anything
   is recorded, so a rejected call leaves no trace.
2. **Record the membership in `_joinedGroups`, then send the frame** (`:487-491`). Not the other way
   round: the hub may refuse, and its `GroupJoinRefused` can arrive and be handled by the receive loop
   **before this method resumes**. Recording afterwards would reinstate the very group the refusal had
   just removed.
3. If the send throws, take the record back — **but only if this call is what added it** (`recorded`,
   `:490`, rollback at `:505-511`). A join of a group already joined, or one racing a concurrent join of
   the same name, must not roll back a record its predecessor owns; the group would then be missing from
   `JoinedGroups` while the client is still in it on the hub, and `MeshClientReconnector` — which
   restores from that snapshot — would silently not restore it.

`LeaveGroupAsync` (`:518`) keeps the opposite order: send first, then remove locally (`:524-530`).

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
the event (`MeshClient.cs:1057-1082`) — so `JoinedGroups` never claims a membership the hub has denied,
and a later disconnect does not hand the group to the reconnector to restore. The refusal is **not**
retried by anything in the library; a handler that wants to try again must ask again itself.

> **The refusal carries no correlation id**, so a refusal for an older join can clear a membership a
> later join legitimately obtained. The divergence is fail-safe — the client under-reports while the hub
> keeps the member — but it is real. [known-issues.md](known-issues.md) KI-27.

Note the client logs the refused group name **unclipped** (`MeshClient.cs:1071`), unlike the hub, which
clips to 64 characters. The name came from your own hub, so this is not the same exposure, but it is
worth knowing if you parse client logs.

<a id="sending-headers"></a>

### Sending headers

PR #74 (issue #32) added a `MessageHeaders` overload to both `SendAsync` and `SendToGroupAsync` — a
small, string-keyed, immutable bag of metadata (correlation id, content-type hint, trace context, and
the like) that travels alongside the payload without the hub ever decoding it. Full wire-format
write-up: [protocol.md](protocol.md#message-headers). **PR #83's `RequestAsync`/`ReplyAsync` are built
entirely on this overload** — see [Request/response (RPC)](#request-response) below.

```csharp
var headers = new MessageHeaders(new Dictionary<string, string> { ["correlationId"] = "abc123" });
await client.SendAsync(recipientId, payload, headers);
```

- **The plain `SendAsync(recipientId, message, cancellationToken)` overload still exists** and is now a
  one-line forward to the headers overload with `MessageHeaders.Empty` (`MeshClient.cs:363-369`) — existing
  call sites need no change, and an empty `MessageHeaders` produces the exact same bytes on the wire as
  before (no header block is written at all when `headers.Count == 0`).
- **The headers overload itself now guards two reserved keys (PR #83).** `SendAsync(recipientId, message,
  headers, cancellationToken)` calls `ThrowIfReservedHeaderKeyPresent(headers)` (`MeshClient.cs:379`,
  method at `:441-452`) before doing anything else: if `headers` contains `"mesh.request-id"` or
  `"mesh.reply"` (the two `RequestReplyHeaderKeys`, `Messages/RequestReplyHeaderKeys.cs`), it throws
  `ArgumentException` rather than letting the message collide with the request/response machinery. Both
  the body-build logic and the actual frame-send now live in a shared private `SendCoreAsync`
  (`:391-433`) that `RequestAsync`/`ReplyAsync` call directly, **bypassing** this guard — they are the one
  legitimate producer of those keys. `SendToGroupAsync`'s headers overload is untouched: it does not share
  `SendCoreAsync` and does not guard these keys, because group sends cannot be a request or a reply in the
  first place. See [Request/response (RPC)](#request-response) and
  [known-issues.md](known-issues.md) KI-42.
- **A non-empty `MessageHeaders` requires a connection negotiated at `Protocol.HeaderEnvelopeMinVersion`
  (`5`) or above.** Below that, both `SendAsync` and `SendToGroupAsync` throw `NotSupportedException`
  (`RequireHeaderEnvelopeSupport`, `MeshClient.cs:593-602`) rather than silently sending the message
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

PR #83 added a correlated request/reply helper — `RequestAsync`/`ReplyAsync` (`IMeshClient.cs:249-253` /
`:269-272`, implemented `MeshClient.cs:744-798` / `:801-822`) — built entirely on the existing
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

**How `RequestAsync` works** (`MeshClient.cs:744-798`):
1. Rejects a non-positive `timeout` with `ArgumentOutOfRangeException` before sending anything.
2. Claims a fresh correlation id via `Interlocked.Increment(ref _requestCorrelationId)` (`:755`) and
   records a `PendingRequest(recipientId, completion)` in `_pendingRequests[correlationId]` (`:762`) —
   **before** the send, so a reply racing back extremely fast still finds an entry.
3. Sends the request as an ordinary direct message via the shared `SendCoreAsync` (`:391-433`), with a
   `MessageHeaders` carrying only `CorrelationId`.
4. Awaits the completion source, bounded by a linked `CancellationTokenSource` cancelled after `timeout`
   (`:778-789`) — a timeout is translated to `TimeoutException`, a genuine external cancellation still
   surfaces as `OperationCanceledException`.
5. **`finally` always removes the entry from `_pendingRequests`** (`:796`), whether the call succeeded,
   timed out, or was cancelled — so a reply that arrives later for that id is discarded by
   `TryCompletePendingRequest` rather than resolving a *future* request that happens to reuse the id.

**How `ReplyAsync` works** (`MeshClient.cs:801-822`): takes back the exact `MessageReceivedEventArgs` the
request arrived on. If `request.CorrelationId` is `null` (an ordinary message, not a request), it throws
`InvalidOperationException` rather than sending a reply frame nothing is waiting for. Otherwise it sends a
direct message back to `request.SenderId` carrying both `CorrelationId` (echoed) and `Reply = "1"`, again
via `SendCoreAsync`.

**How an incoming reply is matched, and why a hostile peer cannot resolve someone else's request**
(`TryCompletePendingRequest`, `MeshClient.cs:1284-1329`, called from the receive loop's
`DeliverMessageWithHeaders` branch at `:993` **before** `MessageReceived` is ever raised for that frame):
1. A frame without `Reply == "1"` is not a reply at all — returns `false`, so the caller raises
   `MessageReceived` as normal (this is how an incoming *request* is delivered — see below).
2. A reply with a missing/malformed `CorrelationId` is logged and discarded (`:1296`) — still intercepted,
   never raised through `MessageReceived`.
3. A reply whose correlation id is not currently in `_pendingRequests` is logged at `Debug` and discarded
   (`:1300-1307`) — the request it answers has already timed out, been cancelled, or never existed on this
   connection.
4. **A reply is only accepted from the client the request was actually addressed to**
   (`pending.ExpectedResponderId != senderId`, `:1308-1319`): a mismatch is logged at `Warning` and
   discarded, **without removing the pending entry** — so a forged reply from any other client connected
   to the same hub cannot resolve, and cannot strand, a request meant for someone else. The genuine
   responder's reply, arriving afterwards, still completes it. This is the actual security property: the
   hub does not authenticate senders end-to-end (see [known-issues.md](known-issues.md) KI-17), but
   `RequestAsync` cannot be tricked into accepting attacker-controlled bytes as long as the attacker is not
   the client the request was sent to.
5. A genuine match resolves via a compare-and-remove (`TryRemove(new KeyValuePair<...>(correlationId,
   pending))`, `:1320-1326`) against the *exact instance* just matched — so a reply racing a fresh
   `RequestAsync` call that has already claimed the same id (having removed and replaced the entry itself)
   cannot steal that fresh call's slot.

**In every case above, the frame is consumed** (`TryCompletePendingRequest` returns `true` for anything
carrying `Reply == "1"`, matched or not) — a reply frame, forged or genuine, matched or stale, **never**
reaches `MessageReceived`.

**On the receiving side of a genuine incoming request** (not a reply), the same `DeliverMessageWithHeaders`
branch sets `MessageReceivedEventArgs.CorrelationId = TryGetRequestCorrelationId(headers)`
(`MeshClient.cs:1002`, method at `:1335-1345`) — `long?`, `null` for an ordinary message, set for a
request. A handler that finds it set should call `ReplyAsync`, passing the same event args back in.

**Contract & gotchas:**
- **Concurrent `RequestAsync` calls on the same client are fully independent** — each is tracked by its
  own correlation id in the `ConcurrentDictionary`; there is no equivalent of `GetClientIdByNameAsync`'s
  single-slot serialisation. Fire many at once.
- **Group messages cannot be requests or replies.** `RequestAsync` only ever takes a single `recipientId`;
  `SendToGroupAsync`'s headers overload does not go through `SendCoreAsync` and is not gated by
  `ThrowIfReservedHeaderKeyPresent`. The `DeliverGroupMessageWithHeaders` receive-loop branch
  (`MeshClient.cs:1015-1056`) is untouched by PR #83 — it still just raises `GroupMessageReceived` inside
  a plain `try/catch`, with no `CorrelationId` concept at all.
- **A connection drop faults every outstanding `RequestAsync` call** with `InvalidOperationException`
  ("The connection was closed before a reply arrived.") from the receive loop's termination `finally`
  (`MeshClient.cs:1162-1171`) — see [The receive loop](#the-receive-loop) above. A caller on a
  non-cancellable token is not left hanging past the connection's own teardown.
- **The two reserved header keys cannot be set through the public `SendAsync(headers)` overload** —
  `ThrowIfReservedHeaderKeyPresent` (`:441-452`) throws `ArgumentException` if the caller's own
  `MessageHeaders` contains either one. This is a genuine new constraint on a previously unrestricted
  parameter: an application already using the literal string `"mesh.request-id"` or `"mesh.reply"` as one
  of its own header keys before PR #83 will now see `SendAsync` throw where it previously succeeded. See
  [known-issues.md](known-issues.md) KI-42.
- **The hub is completely unaware of request/response.** It never inspects `MessageHeaders` (headers ride
  opaque, same as always — see [protocol.md](protocol.md#message-headers)), so `RequestAsync`/`ReplyAsync`
  are a pure client-to-client convention enforced only by the sending `MeshClient`'s own guard and the
  receiving `MeshClient`'s own `TryCompletePendingRequest` check. A peer that is not built on this library
  — or a `MeshClient` caller that bypasses `SendAsync` entirely and somehow gets a raw
  `SendMessageWithHeaders` frame onto the wire — can still produce a frame carrying `Reply = "1"`, which
  **any** receiving `MeshClient` will intercept and drop before `MessageReceived`, whether or not it
  matches a real pending request. See [known-issues.md](known-issues.md) KI-43.

### `GetClientIdByNameAsync` — the correlated lookup

`MeshClient.cs:695`. Serialised by `_lookupLock` (`SemaphoreSlim(1,1)`): **one lookup in flight at a
time per client**; concurrent callers queue. Each request carries a 4-byte correlation id (`unchecked`
increment, `:40-41`, `:715`). A single-slot `_pendingLookup` (`PendingLookup(correlationId,
TaskCompletionSource<Guid?>)`, declared at `:1355`, assigned at `:717`) is completed by the receive loop
**only when the ids match** (`:1083-1104`) — so a late response from a cancelled lookup cannot resolve a
subsequent one. Returns `null` when the hub reports "not found". Cancelling via the token abandons the
wait; the `finally` clears `_pendingLookup` and releases the lock (`:728-740`).

This lookup's own single-slot `_pendingLookup`/correlation-id scheme is **not** what `RequestAsync` uses —
`RequestAsync` needed to support multiple calls in flight at once, so it has its own, separate
`ConcurrentDictionary`-backed table (`_pendingRequests`, `:46-47`). See
[Request/response (RPC)](#request-response).

### Threading & idempotency

- `_stateLock` guards state, the transport/cts references and the `_localDisconnectRequested` claim flag.
  `_groupMembershipLock` guards `_joinedGroups`. `_lookupLock` serialises lookups.
- `DisconnectAsync` and `DisposeAsync` are safe to call when not connected (early return). Note that
  `DisconnectAsync`'s early return is not *quite* inert: in the `Disconnecting` state it claims the
  in-flight teardown (`MeshClient.cs:307-310`). It is still idempotent and side-effect-free from the
  caller's point of view.
- Send methods snapshot the transport under `_stateLock`, then release it before the `await SendAsync`
  — so a slow send does not hold the state lock.
- **`_pendingRequests` (PR #83) needs no lock of its own** — it is a `ConcurrentDictionary<long,
  PendingRequest>` (`:46`), so concurrent `RequestAsync` calls and the single receive loop's
  `TryCompletePendingRequest` all touch it safely without going through `_stateLock`. Correlation ids come
  from `Interlocked.Increment(ref _requestCorrelationId)` (`:47`, `:755`), so concurrent `RequestAsync`
  calls on the same client never collide on an id.

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
