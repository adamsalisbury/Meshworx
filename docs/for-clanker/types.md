# Public value types — event args, enums, authentication & authorisation types, exception

[← back to index](../for-clanker.md) · related: [client.md](client.md) · [hub.md](hub.md) · [protocol.md](protocol.md)

The small public types that flow through events, errors and the two integrator seams (authentication and
authorisation). All are trivial and self-evident — this file is a complete inventory so you don't have to
hunt for them. `namespace AdamSalisbury.Meshworx.Messages` unless noted otherwise.

## Event args

| Type | Members | Raised by | Source |
|---|---|---|---|
| `MessageReceivedEventArgs` | `required Guid SenderId`, `required ReadOnlyMemory<byte> Data`, `MessageHeaders Headers = MessageHeaders.Empty`, `long? CorrelationId = null` (PR #83) | `IMeshClient.MessageReceived` (direct **and** broadcast) | `Messages/MessageReceivedEventArgs.cs` |
| `GroupMessageReceivedEventArgs` | `required Guid SenderId`, `required string GroupName`, `required ReadOnlyMemory<byte> Data`, `MessageHeaders Headers = MessageHeaders.Empty` | `IMeshClient.GroupMessageReceived` | `Messages/GroupMessageReceivedEventArgs.cs` |
| `GroupJoinRefusedEventArgs` | `required string GroupName` | `IMeshClient.GroupJoinRefused` | `Messages/GroupJoinRefusedEventArgs.cs:6` |
| `DisconnectedEventArgs` | `required DisconnectReason Reason` | `IMeshClient.Disconnected` | `Messages/DisconnectedEventArgs.cs` |
| `ClientConnectionEventArgs` | `required Guid ClientId`, `required string ClientName` | `IMeshHub.ClientConnected` / `ClientDisconnected` (namespace `AdamSalisbury.Meshworx`) | `ClientConnectionEventArgs.cs` |
| `QueueSaturatedEventArgs` | `required Guid SenderId`, `required Guid RecipientId` | `IMeshHub.QueueSaturated` (namespace `AdamSalisbury.Meshworx`, **not** `.Messages` — PR #87, issue #30) | `QueueSaturatedEventArgs.cs` |
| `SendRejectedEventArgs` | `required Guid RecipientId` | `IMeshClient.SendRejected` (PR #87, issue #30) | `Messages/SendRejectedEventArgs.cs` |

All use `required` init-only properties (C# 11) — construct with object initialisers.

> **`Data` is a view over the received frame.** It is currently backed by a fresh per-frame `byte[]`
> (see [transport.md](transport.md)), so retaining it is safe today; the robust idiom is to copy
> (`e.Data.ToArray()` / `e.Data.Span.CopyTo(...)`) if you keep it past the handler. `Span` is the usual
> access, e.g. `Encoding.UTF8.GetString(e.Data.Span)`.

> **`MessageReceivedEventArgs.CorrelationId` (PR #83)** is set only when this message is a request sent
> via `IMeshClient.RequestAsync`, awaiting a reply — `null` for every ordinary message, including
> broadcasts. A handler that finds it set should answer with `IMeshClient.ReplyAsync`, passing the same
> event args back in. It is never set on `GroupMessageReceivedEventArgs` — group sends cannot be requests.
> A *reply* frame is resolved internally by the receive loop before `MessageReceived` is ever raised for
> it, so this property never distinguishes "is a reply" from "is an ordinary message" — only "is a
> request". See [client.md](client.md#request-response).

## Message content types

`MessageHeaders` (`sealed class`, `Messages/MessageHeaders.cs`, added by PR #74, issue #32) —
`IReadOnlyDictionary<string, string>`, `StringComparer.Ordinal` (case-sensitive keys). A small,
immutable bag of metadata that travels alongside a message body without the hub ever interpreting it —
see [protocol.md](protocol.md#message-headers) for the wire format and
[client.md](client.md#sending-headers) for how to send/receive it.

- `MessageHeaders.Empty` — the shared, zero-entry instance used as the default on
  `MessageReceivedEventArgs`/`GroupMessageReceivedEventArgs.Headers` and accepted by `SendAsync`/
  `SendToGroupAsync` to mean "no headers" (produces the plain, header-less frame).
- **Public constructor copies its input** into a fresh `Dictionary<string, string>` — mutating the
  source afterwards does not affect the `MessageHeaders`. **Throws `ArgumentException` if the input
  contains a duplicate key**, the same as calling `Dictionary<TKey,TValue>.Add` twice for the same key —
  this differs from an object initializer or indexer assignment, which would silently keep the last
  value. See [known-issues.md](known-issues.md) KI-34.
- An `internal` `FromOwnedDictionary` factory (not part of the public surface) wraps a dictionary without
  copying — used only by `HeaderEnvelope.Read`, which builds a fresh one for this purpose and never
  touches it again.

`RequestReplyHeaderKeys` (`internal static class`, `Messages/RequestReplyHeaderKeys.cs`, added by PR #83)
— not part of the public surface, listed here because its two `const string` values are effectively
reserved vocabulary within any `MessageHeaders` an application constructs. `CorrelationId =
"mesh.request-id"`, `Reply = "mesh.reply"`. `MeshClient.SendAsync`'s headers overload throws
`ArgumentException` if a caller's own `MessageHeaders` contains either key — see
[client.md](client.md#request-response) and [known-issues.md](known-issues.md) KI-42/KI-43.

`DeliveryOptions` (`readonly struct`, `IEquatable<DeliveryOptions>`, `DeliveryOptions.cs`, namespace
`AdamSalisbury.Meshworx` — **not** `.Messages` — added by PR #84, extended by PR #87 (issue #30) and by
priority lanes (`ab16567`)) — controls whether `IMeshClient.SendAsync(Guid, ReadOnlyMemory<byte>,
DeliveryOptions, CancellationToken)` waits for an end-to-end delivery acknowledgement, asks the hub to
await capacity on a saturated recipient queue instead of dropping, and/or sets the message's
`MessagePriority`. Four properties (`RequireAcknowledgement`, `AcknowledgementTimeout`, `AwaitCapacity`,
`Priority`), all read-only, `Equals`/`GetHashCode` covering all four; four ways to obtain or modify one:

- `DeliveryOptions.None` — a `static readonly` field, and the struct's own default value (so
  `default(DeliveryOptions)` and a caller who never touches this type both get fire-and-forget, identical
  to every other `SendAsync` overload). `Priority` defaults to `MessagePriority.Normal`.
- `DeliveryOptions.RequireAck(TimeSpan timeout)` — a static factory; throws `ArgumentOutOfRangeException`
  synchronously if `timeout` is not positive. Sets `RequireAcknowledgement`/`AcknowledgementTimeout`.
- `DeliveryOptions.AwaitingCapacity()` — a static factory (PR #87); sets `AwaitCapacity` without requiring
  an acknowledgement. Does **not** make the `SendAsync` call itself wait — see
  [client.md](client.md#backpressure-signalling).
- `DeliveryOptions.AtPriority(MessagePriority priority)` — a static factory (priority lanes); sets
  `Priority` alone. See [client.md](client.md#message-priority).
- `options.WithAwaitCapacity()` — an instance method (PR #87) returning a copy of `options` with
  `AwaitCapacity` also set, so `RequireAck(...).WithAwaitCapacity()` gets both. Read its own remarks (and
  [known-issues.md](known-issues.md) KI-49) before combining the two — their timeouts are independent.
- `options.WithPriority(MessagePriority priority)` — an instance method (priority lanes) returning a copy
  of `options` with `Priority` also set, so it composes with `RequireAck(...)`/`AwaitingCapacity()`.
- There is no public constructor.

See [client.md](client.md#delivery-acknowledgement), [client.md](client.md#backpressure-signalling) and
[client.md](client.md#message-priority) for how to use it, and [known-issues.md](known-issues.md)
KI-44/KI-45/KI-48/KI-49/KI-54 for what its guarantees do and do not cover.

`DeliveryAcknowledgementHeaderKeys` (`internal static class`,
`Messages/DeliveryAcknowledgementHeaderKeys.cs`, added by PR #84) — the acknowledgement counterpart to
`RequestReplyHeaderKeys` above, same shape: not part of the public surface, listed here because its three
`const string` values are reserved vocabulary. `CorrelationId = "mesh.ack-id"` (both the original message
and its acknowledgement), `Request = "mesh.ack-request"` (marks the original message as wanting one),
`Ack = "mesh.ack"` (marks the acknowledgement frame itself). `MeshClient.SendAsync`'s headers overload
throws `ArgumentException` if a caller's own `MessageHeaders` contains any of the three — see
[client.md](client.md#delivery-acknowledgement) and [known-issues.md](known-issues.md) KI-42/KI-46.

`MessageExpiryHeaderKeys` (`internal static class`, `Messages/MessageExpiryHeaderKeys.cs`, added by
PR #85, issue #29) — reserved vocabulary for per-message time-to-live, same shape as the two above. One
`const string`: `ExpiresAtUnixMilliseconds = "mesh.expires-at"`, an absolute Unix-millisecond expiry
computed from the sending client's own clock. Unlike the request/response and delivery-acknowledgement
keys, the hub itself reads this one (without fully decoding the header block) to drop an already-expired
queued frame — see [protocol.md](protocol.md#message-expiry-headers) and
[hub.md](hub.md#dropping-expired-frames). `MeshClient.SendAsync`'s headers overload throws
`ArgumentException` if a caller's own `MessageHeaders` contains it — see
[client.md](client.md#message-expiry-time-to-live) and [known-issues.md](known-issues.md) KI-42/KI-47.

`BackpressureHeaderKeys` (`internal static class`, `Messages/BackpressureHeaderKeys.cs`, added by PR #87,
issue #30) — reserved vocabulary for `DeliveryOptions.AwaitCapacity`, same shape again. One
`const string`: `AwaitCapacity = "mesh.await-capacity"`, present with value `"1"` when the sender asked
the hub to await room on the recipient's queue instead of dropping. The hub reads this one too (the
second, alongside message expiry, to do so) via `MeshHub.WantsAwaitCapacity`, at **enqueue** time rather
than expiry's **dequeue** time — see [protocol.md](protocol.md#backpressure-header) and
[hub.md](hub.md#backpressure-signalling-and-awaiting-capacity). `MeshClient.SendAsync`'s headers overload
throws `ArgumentException` if a caller's own `MessageHeaders` contains it — see
[client.md](client.md#backpressure-signalling) and [known-issues.md](known-issues.md) KI-42.

## Enums

| Type | Values | Source |
|---|---|---|
| `DisconnectReason` | `RemoteDisconnect` (hub sent `Disconnect`), `ConnectionLost` (transport failed / idle timeout) | `Messages/DisconnectReason.cs` |
| `RegistrationErrorCode : byte` | `DuplicateClientName=0x01`, `UnsupportedProtocolVersion=0x02`, `ClientNameTooLong=0x03`, `HubAtCapacity=0x04`, `AuthenticationFailed=0x05` (namespace `AdamSalisbury.Meshworx`) | `RegistrationErrorCode.cs` |
| `MessagePriority` | `Normal=0` (default), `Low=1`, `High=2` (namespace `AdamSalisbury.Meshworx.Messages`, `ab16567`) | `Messages/MessagePriority.cs:13-32` |

`RegistrationErrorCode`'s byte values are wire values — see [protocol.md](protocol.md). Do not renumber
without a version bump. `AuthenticationFailed` was added with protocol version 3; it is the code the hub
returns for **every** authentication outcome that is not success — see below.

## Authentication types

Added with protocol version 3. Both live in namespace `AdamSalisbury.Meshworx` (the root, not
`.Messages`) and are the entire public surface of the hub's authentication seam. Neither is used unless
you pass a `ClientAuthenticator` to the `MeshHub` constructor.

### `ClientAuthenticator` — `ClientAuthenticator.cs:18`

```csharp
public delegate ValueTask<bool> ClientAuthenticator(
    RegistrationContext context,
    CancellationToken cancellationToken);
```

A **delegate, not an interface** — so a lambda, a method group or a closure over your DI container all
work without a type. Return `true` to admit the client, `false` to refuse it with
`AuthenticationFailed`.

Contract, from the delegate's own XML docs and the hub's call site:

- Invoked **once per registration**, after the name and credential are parsed and **before** the client
  claims a capacity slot, reserves its name, or is admitted. A cheap at-capacity early-out runs just
  ahead of it, so an already-full hub never reaches the callback — but the binding capacity decision is
  taken *after* it returns, so a peer that never authenticates cannot hold a slot. See
  [hub.md](hub.md#registration-handshake-hub-side).
- `cancellationToken` is cancelled when the **hub** is shutting down. It is not the client's token.
- `ValueTask<bool>` — return `ValueTask.FromResult(...)` for a synchronous decision and it allocates
  nothing.
- **It runs on unauthenticated input.** Any peer that can reach the listener can cause it to run. The hub
  bounds this (concurrency cap, timeout, exception isolation) but cannot make your check cheap or
  constant-time. See [hub.md](hub.md#authentication) for the full set of protections and the gotchas.

### `RegistrationContext` — `RegistrationContext.cs:7`

```csharp
public sealed record RegistrationContext
{
    public required string ClientName { get; init; }        // RegistrationContext.cs:12
    public required ReadOnlyMemory<byte> Credential { get; init; }  // RegistrationContext.cs:18
}
```

A `sealed record` with `required` init-only properties — construct with an object initialiser (the hub
does, at `MeshHub.cs:1366`). Trivially constructible in tests.

- `ClientName` — the name being registered under. Already validated for length, **not** yet checked for
  uniqueness, so two concurrent registrations for the same name can both reach your authenticator.
- `Credential` — exactly the bytes the client sent after its name, empty if it sent none. The library
  assigns no meaning to them.
- **Only guaranteed valid for the duration of the call** — copy it if it must outlive the invocation.
  (In the current implementation the hub already copies it out of the inbound frame, `MeshHub.cs:1365`, so
  it does not alias a larger buffer — but the documented contract is the one to code against.)

<a id="authorisation-types"></a>

## Authorisation types

Added **within** protocol version 3 (no version bump — see
[protocol.md](protocol.md#additive-opcodes-within-a-version)). Both live in namespace
`AdamSalisbury.Meshworx` (the root, not `.Messages`) and are the entire public surface of the hub's
**group** authorisation seam. Neither is used unless you pass a `GroupAuthoriser` to the `MeshHub`
constructor.

These are the counterpart to the [authentication types](#authentication-types) above, and the split is
the point: the authenticator establishes **who a peer is**, the authoriser decides **what that peer may
do**. They compose — a `GroupJoinContext.ClientName` is only as trustworthy as the `ClientAuthenticator`
that admitted it.

### `GroupAuthoriser` — `GroupAuthoriser.cs:35`

```csharp
public delegate ValueTask<bool> GroupAuthoriser(
    GroupJoinContext context,
    CancellationToken cancellationToken);
```

A **delegate, not an interface**, matching `ClientAuthenticator`. Return `true` to admit the client to
the group; `false` refuses the join and the hub sends the client a `GroupJoinRefused` frame.

Contract, from the delegate's own XML docs and the hub's call site (`AuthoriseGroupJoinAsync`,
`MeshHub.cs:1909`):

- Invoked **once per join request**, including every re-join a client issues after reconnecting, so a
  decision is never carried across a connection and a reconnector's membership restore cannot bypass it.
  See [hub.md](hub.md#group-authorisation).
- `cancellationToken` is the **calling client's** token — cancelled when that client disconnects or the
  hub shuts down. (Note this differs from `ClientAuthenticator`, whose token is the hub's.)
- **Fails closed.** Returning `false`, throwing, cancelling from inside the callback, or exceeding
  `groupAuthorisationTimeout` all refuse the join.
- `ValueTask<bool>` — a synchronous decision (`ValueTask.FromResult(...)`) takes an explicitly
  allocation-free fast path in the hub (`MeshHub.cs:1939-1944`) and is the common case; anything else,
  including an already-faulted result, goes through the bounded `WaitAsync`.
- **It runs on input from an already-admitted client**, driven from that client's own receive loop, which
  reads nothing else from that client until it returns. So a slow callback stalls only the client that
  asked — and there is deliberately **no** concurrency semaphore, unlike `ClientAuthenticator`. The
  consequence you must design for is in [known-issues.md](known-issues.md) KI-28.

### `GroupJoinContext` — `GroupJoinContext.cs:7`

```csharp
public sealed record GroupJoinContext
{
    public required Guid ClientId { get; init; }      // GroupJoinContext.cs:12
    public required string ClientName { get; init; }  // GroupJoinContext.cs:22
    public required string GroupName { get; init; }   // GroupJoinContext.cs:28
}
```

A `sealed record` with `required` init-only properties — construct with an object initialiser (the hub
does, at `MeshHub.cs:1912`). Trivially constructible in tests.

- `ClientId` — the hub-assigned id. Fresh per connection, so it is **not** stable across a reconnect;
  authorise on the name plus your own state if you need continuity.
- `ClientName` — the name that passed the hub's `ClientAuthenticator`. **With no authenticator configured
  the name is self-asserted and is not an identity** — the hub's only name rule is uniqueness.
- `GroupName` — client-supplied and **untrusted**. Match it against known groups rather than parsing
  meaning out of it. It is also unbounded in length (KI-8), so do not use it as a key in anything you
  cannot afford a client to grow.

## Offline delivery types

Added for issue #28. The seam that turns store-and-forward on, plus the shape of what is stored. See
[hub.md](hub.md#offline-delivery) for how the hub drives them.

### `IOfflineStore` — `IOfflineStore.cs`

Two methods, both keyed by **client name** rather than id — the id is minted per connection, so it is
the name that survives a client going away and coming back.

| Member | Signature | Contract |
|---|---|---|
| `TryEnqueueAsync` | `ValueTask<bool>(string clientName, OfflineMessage message, CancellationToken = default)` | `true` = stored, `false` = refused (the hub then drops the message as if the feature were off). An implementation that *evicts* to make room still returns `true` — the result describes this message, not what it displaced |
| `TakeAllAsync` | `ValueTask<IReadOnlyList<OfflineMessage>>(string clientName, CancellationToken = default)` | Removes and returns everything held, **oldest first**. Called once per successful registration, so make "this name holds nothing" cheap. Discard anything past its window here rather than returning it |

**Implementations must be thread-safe** — both methods are called from per-connection handler tasks that
run concurrently, and one name can be enqueued to by many senders while its owner is reconnecting.

### `OfflineMessage` — `OfflineMessage.cs`

`public sealed record OfflineMessage(Guid SenderId, ReadOnlyMemory<byte> HeaderBlock,
ReadOnlyMemory<byte> Body, DateTimeOffset QueuedAt)`, plus `int ByteCount => Body.Length +
HeaderBlock.Length`.

**It holds the message's parts, not a built delivery frame, and that is the design point** — the frame's
shape depends on the version the *returning* connection negotiates, which is unknowable at storage time.
The hub copies both byte ranges out of the receive buffer before constructing one, so neither aliases a
buffer that is about to be reused; an implementation may hold them indefinitely.

### `InMemoryOfflineStore` — `InMemoryOfflineStore.cs`

`public sealed class InMemoryOfflineStore : IOfflineStore`, the bounded process-local default.
Constructor: `(int? maxMessagesPerClient = null, int? maxBytesPerClient = null, TimeSpan? timeToLive =
null, int? maxClients = null)`, defaulting to 100 messages, 1 MiB, 5 minutes and 1000 names —
each exposed as a `Default*` constant. Non-positive values throw `ArgumentOutOfRangeException`.

- **A full queue refuses the new message rather than evicting the oldest.** That keeps "accepted means
  it will be delivered unless it expires" true, and makes the loss visible where it happens (the hub
  counts `reason=offline-queue-full`). Implement the interface for the opposite policy.
- **Expiry is purged lazily**, on the next call touching that name, not by a timer — a store nobody is
  using does no work, and a queue whose messages have all aged out accepts new ones rather than staying
  nominally full. Because the queue is in arrival order, the expired messages are always a prefix.
- **One lock per name**, with the same `Removed`-flag retire-and-retry dance the hub uses for groups
  (`MeshHub.RemoveMemberFromGroup`) — including doing the `TryRemove` *inside* the lock, which is what
  makes the enqueue-side retry terminate rather than spin against a queue that is dead but still mapped.
- **The name cap counts distinct names holding something**, checked only when a genuinely new name is
  about to be added; a name already in the store is never refused by it. Draining a name frees its slot.

## Compression types (issue #75)

`namespace AdamSalisbury.Meshworx.Compression`, all in `src/AdamSalisbury.Meshworx/Compression/`. Endpoint
concern only — no hub code references any of them, and none of them touches the wire yet (see
[client.md](client.md#compression-strategies-issue-75) for why nothing calls these).

| Type | Members | Source |
|---|---|---|
| `ICompressionStrategy` | `string AlgorithmId`, `ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte>)`, `ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte>, int maxDecompressedBytes)` | `ICompressionStrategy.cs` |
| `ICompressionStrategyRegistry` | `IReadOnlyList<string> AlgorithmIds`, `bool Contains(string)`, `bool TryResolve(string, out ICompressionStrategy?)`, `ICompressionStrategy Resolve(string)` | `ICompressionStrategyRegistry.cs` |
| `CompressionStrategyRegistry` | the above plus `static CreateDefault()`, `Register(ICompressionStrategy)`, `Remove(string)`, `Clear()` (`Register`/`Clear` return `this` for chaining) | `CompressionStrategyRegistry.cs` |
| `BrotliCompressionStrategy` | `static readonly Default`, `ctor()`, `ctor(CompressionLevel)` | `BrotliCompressionStrategy.cs` |
| `DeflateCompressionStrategy` | `static readonly Default`, `ctor()`, `ctor(CompressionLevel)` | `DeflateCompressionStrategy.cs` |
| `CompressionAlgorithms` | `const string Brotli = "br"`, `const string Deflate = "deflate"` | `CompressionAlgorithms.cs` |
| `UnknownCompressionAlgorithmException` | `string? AlgorithmId` + four ctors (three of them only to satisfy `CA1032`, as with `RegistrationRefusedException`) | `UnknownCompressionAlgorithmException.cs` |

`Resolve`/`TryResolve` rather than `Get`/`TryGet` because `CA1716` rejects `Get` on an interface member.

Named `Resolve` throws `UnknownCompressionAlgorithmException` listing what *is* registered; the `TryResolve`
form returns `false`. Both throw `ArgumentException` (or `ArgumentNullException`, for `null`) on an empty id
before looking anything up.

`CompressionHeaderKeys` (`internal static`, `Messages/CompressionHeaderKeys.cs`, issue #33) carries the
two wire keys — `mesh.compression` and `mesh.compression.length` — plus `TryReadCompressionHeaders` and
`WithoutCompressionHeaders`, mirroring `ChunkHeaderKeys` exactly. See
[protocol.md](protocol.md#compression-headers-issue-33).

`CompressionCapabilityEnvelope` (`internal static`, `Messages/CompressionCapabilityEnvelope.cs`, issue
#77) encodes the ordered algorithm-id list carried by `AdvertiseCompression` and
`CompressionCapabilityResponse`. Not the `HeaderEnvelope` codec, deliberately: that encodes a map, and
order is the payload here. See [protocol.md](protocol.md#compression-capability-frames-issue-77).

`UnknownCompressionAlgorithmException` gained a `PeerId` and a `(algorithmId, peerId, peerAlgorithmIds)`
constructor with #77 — the same exception type for "this endpoint has no strategy" and "the peer has not
advertised one", because a caller that named an algorithm does the same thing about either.

`StreamCompression` (`internal static`, `StreamCompression.cs`) is the shared stream plumbing behind both
built-ins — the bounded read loop, and the normalisation of `BrotliStream`'s `InvalidOperationException`
and `DeflateStream`'s `InvalidDataException` into the single `InvalidDataException` the contract names.

## Exception

`RegistrationRefusedException : Exception` (`sealed`, namespace `AdamSalisbury.Meshworx`,
`RegistrationRefusedException.cs`).

- Thrown by `MeshClient.ConnectAsync` when the hub replies with an `Error` frame during registration.
- Carries `RegistrationErrorCode ErrorCode` (default `0` / unset when constructed via the message-only
  or default ctors, which exist only to satisfy `CA1032`'s standard-constructor rule).
- Catch it specifically to distinguish a **refusal** (duplicate name, bad version, name too long, hub
  full, authentication failed) from a transport/other failure:

```csharp
try { await client.ConnectAsync(transport, name); }
catch (RegistrationRefusedException ex) { /* ex.ErrorCode tells you why */ }
```

## Internal types (not visible outside the assembly, listed for orientation)

- `enum MessageType : byte` (`Messages/MessageType.cs`) — the opcodes; see [protocol.md](protocol.md).
- `static class Protocol` (`Messages/Protocol.cs`) — `MinSupportedVersion = 4`, `MaxSupportedVersion = 7`
  (raised from `4` to `5` by PR #74, issue #32, to admit the header envelope; from `5` to `6` by issue #43
  to admit session resumption; and from `6` to `7` by PR #135/issue #109 to admit restored-group reporting
  on a resume; replaced the single `Version = 3` constant in PR #73; see
  [protocol.md](protocol.md#versioning)), `HeaderEnvelopeMinVersion = 5` (added by PR #74 — the lowest
  negotiated version at which `MessageHeaders` may be used), `SessionResumptionMinVersion = 6`,
  `SessionResumedGroupsMinVersion = 7` (PR #135/issue #109), `SessionTokenLength = 32` (issue #43),
  `MaxClientNameLength = 256`.
- `static class HeaderEnvelope` (`Messages/HeaderEnvelope.cs`, added by PR #74) — encodes/decodes the
  header-block wire format for the four header-bearing opcodes; see
  [protocol.md](protocol.md#message-headers).
- The reserved `MessageHeaders` well-known key classes, all `internal` — `RequestReplyHeaderKeys`,
  `DeliveryAcknowledgementHeaderKeys`, `MessageExpiryHeaderKeys`, `BackpressureHeaderKeys`,
  `MessagePriorityHeaderKeys`, `TraceContextHeaderKeys`, `ChunkHeaderKeys` (all `Messages/*.cs`) — 13 keys
  in total across the seven classes; see [known-issues.md](known-issues.md) KI-42 for the complete,
  current list and [protocol.md](protocol.md#message-headers) for the wire shapes.
- `sealed class PriorityOutboundQueue` (`PriorityOutboundQueue.cs`, `ab16567`) — the three-lane
  (high/normal/low) outbound queue every `ClientConnection` uses; see [hub.md](hub.md#priority-lanes).
- `sealed class ClientRateLimiter`, `TokenBucket`, `SharedLogThrottle` (`RateLimiting/*.cs`, issue #69) —
  per-connection inbound admission control; see [hub.md](hub.md#rate-limiting).
- `sealed class ChunkReassembler` (`ChunkReassembler.cs`, feat #93) — client-side reassembly for
  `SendLargeAsync`; see [client.md](client.md#large-message-chunking).
- `static class MeshworxActivitySource` (`Diagnostics/MeshworxActivitySource.cs`, feat #92) — the shared
  `ActivitySource` for distributed tracing; see [client.md](client.md#distributed-tracing).
- `MeshHub.ClientConnection` (carries `NegotiatedProtocolVersion`, PR #74; since PR #87,
  `IsAwaitingCapacity`/`BeginAwaitingCapacity()`/`CapacityWaitScope` for the backpressure-parking
  mechanism; and, since issue #43, a **settable** `Id` via `Rebind` plus `SessionTokenHash`),
  `MeshHub.ResumableSession` (issue #43), `MeshHub.Group`, `MeshClient.ConnectionState`, `MeshClient.PendingLookup` — nested private
  helpers documented in [hub.md](hub.md) / [client.md](client.md).

Tests reach internals via `<InternalsVisibleTo Include="AdamSalisbury.Meshworx.UnitTests" />`
(`AdamSalisbury.Meshworx.csproj:726`).
