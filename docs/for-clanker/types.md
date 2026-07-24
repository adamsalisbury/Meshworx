# Public value types — event args, enums, exception

[← back to index](../for-clanker.md) · related: [client.md](client.md) · [hub.md](hub.md) · [protocol.md](protocol.md)

The small public types that flow through events and errors. All are trivial and self-evident — this file
is a complete inventory so you don't have to hunt for them. `namespace AdamSalisbury.Meshworx.Messages`
unless noted otherwise.

## Event args

| Type | Members | Raised by | Source |
|---|---|---|---|
| `MessageReceivedEventArgs` | `required Guid SenderId`, `required ReadOnlyMemory<byte> Data` | `IMeshClient.MessageReceived` (direct **and** broadcast) | `Messages/MessageReceivedEventArgs.cs` |
| `GroupMessageReceivedEventArgs` | `required Guid SenderId`, `required string GroupName`, `required ReadOnlyMemory<byte> Data` | `IMeshClient.GroupMessageReceived` | `Messages/GroupMessageReceivedEventArgs.cs` |
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
| `RegistrationErrorCode : byte` | `DuplicateClientName=0x01`, `UnsupportedProtocolVersion=0x02`, `ClientNameTooLong=0x03`, `HubAtCapacity=0x04` (namespace `AdamSalisbury.Meshworx`) | `RegistrationErrorCode.cs` |

`RegistrationErrorCode`'s byte values are wire values — see [protocol.md](protocol.md). Do not renumber
without a version bump.

## Exception

`RegistrationRefusedException : Exception` (`sealed`, namespace `AdamSalisbury.Meshworx`,
`RegistrationRefusedException.cs`).

- Thrown by `MeshClient.ConnectAsync` when the hub replies with an `Error` frame during registration.
- Carries `RegistrationErrorCode ErrorCode` (default `0` / unset when constructed via the message-only
  or default ctors, which exist only to satisfy `CA1032`'s standard-constructor rule).
- Catch it specifically to distinguish a **refusal** (duplicate name, bad version, name too long, hub
  full) from a transport/other failure:

```csharp
try { await client.ConnectAsync(transport, name); }
catch (RegistrationRefusedException ex) { /* ex.ErrorCode tells you why */ }
```

## Internal types (not visible outside the assembly, listed for orientation)

- `enum MessageType : byte` (`Messages/MessageType.cs`) — the opcodes; see [protocol.md](protocol.md).
- `static class Protocol` (`Messages/Protocol.cs`) — `Version = 2`, `MaxClientNameLength = 256`.
- `MeshHub.ClientConnection`, `MeshHub.Group`, `MeshClient.ConnectionState`, `MeshClient.PendingLookup`
  — nested private helpers documented in [hub.md](hub.md) / [client.md](client.md).

Tests reach internals via `<InternalsVisibleTo Include="AdamSalisbury.Meshworx.UnitTests" />`
(`AdamSalisbury.Meshworx.csproj:726`).
