# Public value types — event args, enums, authentication & authorisation types, exception

[← back to index](../for-clanker.md) · related: [client.md](client.md) · [hub.md](hub.md) · [protocol.md](protocol.md)

The small public types that flow through events, errors and the two integrator seams (authentication and
authorisation). All are trivial and self-evident — this file is a complete inventory so you don't have to
hunt for them. `namespace AdamSalisbury.Meshworx.Messages` unless noted otherwise.

## Event args

| Type | Members | Raised by | Source |
|---|---|---|---|
| `MessageReceivedEventArgs` | `required Guid SenderId`, `required ReadOnlyMemory<byte> Data` | `IMeshClient.MessageReceived` (direct **and** broadcast) | `Messages/MessageReceivedEventArgs.cs` |
| `GroupMessageReceivedEventArgs` | `required Guid SenderId`, `required string GroupName`, `required ReadOnlyMemory<byte> Data` | `IMeshClient.GroupMessageReceived` | `Messages/GroupMessageReceivedEventArgs.cs` |
| `GroupJoinRefusedEventArgs` | `required string GroupName` | `IMeshClient.GroupJoinRefused` | `Messages/GroupJoinRefusedEventArgs.cs:6` |
| `DisconnectedEventArgs` | `required DisconnectReason Reason` | `IMeshClient.Disconnected` | `Messages/DisconnectedEventArgs.cs` |
| `ClientConnectionEventArgs` | `required Guid ClientId`, `required string ClientName` | `IMeshHub.ClientConnected` / `ClientDisconnected` (namespace `AdamSalisbury.Meshworx`) | `ClientConnectionEventArgs.cs` |

All use `required` init-only properties (C# 11) — construct with object initialisers.

> **`Data` is a view over the received frame.** It is currently backed by a fresh per-frame `byte[]`
> (see [transport.md](transport.md)), so retaining it is safe today; the robust idiom is to copy
> (`e.Data.ToArray()` / `e.Data.Span.CopyTo(...)`) if you keep it past the handler. `Span` is the usual
> access, e.g. `Encoding.UTF8.GetString(e.Data.Span)`.

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
does, at `MeshHub.cs:1324`). Trivially constructible in tests.

- `ClientName` — the name being registered under. Already validated for length, **not** yet checked for
  uniqueness, so two concurrent registrations for the same name can both reach your authenticator.
- `Credential` — exactly the bytes the client sent after its name, empty if it sent none. The library
  assigns no meaning to them.
- **Only guaranteed valid for the duration of the call** — copy it if it must outlive the invocation.
  (In the current implementation the hub already copies it out of the inbound frame, `MeshHub.cs:1323`, so
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
`MeshHub.cs:1667`):

- Invoked **once per join request**, including every re-join a client issues after reconnecting, so a
  decision is never carried across a connection and a reconnector's membership restore cannot bypass it.
  See [hub.md](hub.md#group-authorisation).
- `cancellationToken` is the **calling client's** token — cancelled when that client disconnects or the
  hub shuts down. (Note this differs from `ClientAuthenticator`, whose token is the hub's.)
- **Fails closed.** Returning `false`, throwing, cancelling from inside the callback, or exceeding
  `groupAuthorisationTimeout` all refuse the join.
- `ValueTask<bool>` — a synchronous decision (`ValueTask.FromResult(...)`) takes an explicitly
  allocation-free fast path in the hub (`MeshHub.cs:1697-1702`) and is the common case; anything else,
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
does, at `MeshHub.cs:1670`). Trivially constructible in tests.

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
- `static class Protocol` (`Messages/Protocol.cs`) — `MinSupportedVersion = 4`, `MaxSupportedVersion = 4`
  (replaced the single `Version = 3` constant in PR #73; see [protocol.md](protocol.md#versioning)),
  `MaxClientNameLength = 256`.
- `MeshHub.ClientConnection`, `MeshHub.Group`, `MeshClient.ConnectionState`, `MeshClient.PendingLookup`
  — nested private helpers documented in [hub.md](hub.md) / [client.md](client.md).

Tests reach internals via `<InternalsVisibleTo Include="AdamSalisbury.Meshworx.UnitTests" />`
(`AdamSalisbury.Meshworx.csproj:726`).
