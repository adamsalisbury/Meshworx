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
`AdamSalisbury.Meshworx` — **not** `.Messages` — added by PR #84) — controls whether
`IMeshClient.SendAsync(Guid, ReadOnlyMemory<byte>, DeliveryOptions, CancellationToken)` waits for an
end-to-end delivery acknowledgement. Two ways to obtain one:

- `DeliveryOptions.None` — a `static readonly` field, and the struct's own default value (so
  `default(DeliveryOptions)` and a caller who never touches this type both get fire-and-forget, identical
  to every other `SendAsync` overload).
- `DeliveryOptions.RequireAck(TimeSpan timeout)` — a static factory; throws `ArgumentOutOfRangeException`
  synchronously if `timeout` is not positive. `RequireAcknowledgement`/`AcknowledgementTimeout` are the
  two read-only properties it sets; there is no public constructor.

See [client.md](client.md#delivery-acknowledgement) for how to use it and
[known-issues.md](known-issues.md) KI-44/KI-45 for what its guarantee does and does not cover.

`DeliveryAcknowledgementHeaderKeys` (`internal static class`,
`Messages/DeliveryAcknowledgementHeaderKeys.cs`, added by PR #84) — the acknowledgement counterpart to
`RequestReplyHeaderKeys` above, same shape: not part of the public surface, listed here because its three
`const string` values are reserved vocabulary. `CorrelationId = "mesh.ack-id"` (both the original message
and its acknowledgement), `Request = "mesh.ack-request"` (marks the original message as wanting one),
`Ack = "mesh.ack"` (marks the acknowledgement frame itself). `MeshClient.SendAsync`'s headers overload
throws `ArgumentException` if a caller's own `MessageHeaders` contains any of the three — see
[client.md](client.md#delivery-acknowledgement) and [known-issues.md](known-issues.md) KI-42/KI-46.

## Enums

| Type | Values | Source |
|---|---|---|
| `DisconnectReason` | `RemoteDisconnect` (hub sent `Disconnect`), `ConnectionLost` (transport failed / idle timeout) | `Messages/DisconnectReason.cs` |
| `RegistrationErrorCode : byte` | `DuplicateClientName=0x01`, `UnsupportedProtocolVersion=0x02`, `ClientNameTooLong=0x03`, `HubAtCapacity=0x04`, `AuthenticationFailed=0x05` (namespace `AdamSalisbury.Meshworx`) | `RegistrationErrorCode.cs` |

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
- `static class Protocol` (`Messages/Protocol.cs`) — `MinSupportedVersion = 4`, `MaxSupportedVersion = 5`
  (raised from `4` by PR #74, issue #32, to admit the header envelope; replaced the single `Version = 3`
  constant in PR #73; see [protocol.md](protocol.md#versioning)), `HeaderEnvelopeMinVersion = 5` (added by
  PR #74 — the lowest negotiated version at which `MessageHeaders` may be used), `MaxClientNameLength = 256`.
- `static class HeaderEnvelope` (`Messages/HeaderEnvelope.cs`, added by PR #74) — encodes/decodes the
  header-block wire format for the four header-bearing opcodes; see
  [protocol.md](protocol.md#message-headers).
- `MeshHub.ClientConnection` (now also carries `NegotiatedProtocolVersion`, PR #74), `MeshHub.Group`,
  `MeshClient.ConnectionState`, `MeshClient.PendingLookup` — nested private helpers documented in
  [hub.md](hub.md) / [client.md](client.md).

Tests reach internals via `<InternalsVisibleTo Include="AdamSalisbury.Meshworx.UnitTests" />`
(`AdamSalisbury.Meshworx.csproj:726`).
