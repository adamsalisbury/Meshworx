# Meshworx

Flexible and unopinionated messaging library.

Meshworx connects named clients through a central **hub**. Each client registers with a
unique name, looks up other clients by name, and exchanges opaque byte-payload messages that
the hub routes to the intended recipient. The transport is pluggable — a length-prefixed TCP
transport ships in the box, and any `ITransport`/`ITransportListener` implementation can be
substituted.

- **Target framework:** .NET 10
- **Namespaces:** `AdamSalisbury.Meshworx`, `AdamSalisbury.Meshworx.Messages`,
  `AdamSalisbury.Meshworx.Transport`, `AdamSalisbury.Meshworx.Transport.Tcp`

## Architecture

| Component | Responsibility |
|---|---|
| `MeshHub` (`IMeshHub`) | Accepts client connections, tracks registered clients by id and name, and routes messages between them. |
| `MeshClient` (`IMeshClient`) | Connects to a hub, sends messages, looks clients up by name, and raises events for inbound messages and disconnects. |
| `ITransport` | A bidirectional, message-oriented channel. Implementations own their framing. |
| `ITransportListener` | Accepts inbound transport connections for the hub. |
| `TcpTransport` / `TcpTransportListener` | TCP implementation using a 4-byte big-endian length prefix per frame. |
| `InMemoryTransport` / `InMemoryTransportListener` | In-process implementation backed by channels, for hosting a hub and clients in one process and for fast, deterministic testing. |

The hub never interprets message payloads — it only reads the routing header and forwards the
body. Delivery is best-effort and fire-and-forget.

## Quick start

### Hosting a hub

```csharp
using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Logging;

using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());

var listener = new TcpTransportListener(port: 22001);
await using var hub = new MeshHub(loggerFactory.CreateLogger<MeshHub>(), listener);

await hub.StartAsync();
// ... hub is now accepting clients ...
await hub.StopAsync();
```

### Connecting a client

```csharp
using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Transport.Tcp;
using System.Text;

await using var client = new MeshClient(loggerFactory.CreateLogger<MeshClient>());

var transport = await TcpTransport.ConnectAsync("localhost", 22001);
await client.ConnectAsync(transport, clientName: "Alice");

client.MessageReceived += (_, e) =>
{
    string text = Encoding.UTF8.GetString(e.Data.Span);
    Console.WriteLine($"From {e.SenderId}: {text}");
};

Guid? bobId = await client.GetClientIdByNameAsync("Bob");
if (bobId is not null)
{
    await client.SendAsync(bobId.Value, Encoding.UTF8.GetBytes("hello Bob"));
}
```

A runnable hub and client are provided under `src/AdamSalisbury.Meshworx.TestApps`.

## Core concepts

- **Registration.** `ConnectAsync` performs a handshake: the client sends its name and
  protocol version; the hub assigns a `Guid` and returns it, or refuses with a
  `RegistrationRefusedException` carrying a `RegistrationErrorCode` (duplicate name,
  unsupported version, name too long, or hub at capacity). Client names are unique and limited
  to 256 characters.
- **Lookup.** `GetClientIdByNameAsync` resolves a name to its `Guid`, or `null` if no client is
  registered under that name. Requests are correlated, so a response from a cancelled lookup
  cannot resolve a later one.
- **Messaging.** `SendAsync(recipientId, payload)` hands the message to the hub, which delivers
  it to the recipient's `MessageReceived` event. If the recipient is unknown the message is
  silently dropped.
- **Broadcast.** `BroadcastAsync(payload)` delivers the message to every other connected client
  (never echoed back to the sender). Recipients receive it through `MessageReceived` exactly as
  they would a directly addressed message.
- **Groups.** A client `JoinGroupAsync(name)`/`LeaveGroupAsync(name)` to control its membership,
  and `SendToGroupAsync(name, payload)` delivers to every other member of that group. Groups are
  created on first join and removed once empty; a client is removed from all its groups when it
  disconnects. Recipients receive group messages through the `GroupMessageReceived` event, which —
  unlike `MessageReceived` — carries the name of the group the message was sent to.
- **Disconnect.** Calling `DisconnectAsync` is graceful and does not raise `Disconnected`. The
  `Disconnected` event fires only for unexpected endings — the hub closing the connection
  (`RemoteDisconnect`) or the transport failing (`ConnectionLost`). After it fires the client
  has reset and may reconnect from the handler.
- **Auto-reconnect.** `MeshClientReconnector` wraps a client and keeps it connected: it does a
  fail-fast initial connect, then transparently re-establishes the connection (bounded per attempt,
  retried with a delay) whenever it drops. It automatically re-joins the groups the client belonged
  to before the drop, then raises `Reconnected` so the application can restore any further state. Pass
  `restoreGroupMembership: false` to take full manual control of group restoration.

```csharp
await using var reconnector = new MeshClientReconnector(
    new MeshClient(logger),
    "Alice",
    async ct => (ITransport)await TcpTransport.ConnectAsync("localhost", 22001, ct));

await reconnector.StartAsync();
await reconnector.Client.JoinGroupAsync("news");
await reconnector.Client.SendAsync(recipientId, payload);
// After an unexpected drop the connection — and membership of "news" — is restored automatically.
```

## Configuration

### `MeshHub`

```csharp
new MeshHub(
    logger,
    listener,
    registrationTimeout: TimeSpan.FromSeconds(10),  // drop connections that never register
    maxClients: 1000,                               // refuse registration beyond this (default: unlimited)
    heartbeatInterval: TimeSpan.FromSeconds(30),    // ping idle clients (default: disabled)
    maxMissedHeartbeats: 2);                        // evict after this many silent intervals
```

When a heartbeat interval is set, the hub pings idle clients and evicts any that fail to send a
frame across the configured number of consecutive intervals, detecting half-open connections.

Observability:

- `ConnectedClientCount` — current number of registered clients.
- `IsClientRegistered(id)` — whether a specific client is registered.
- `ClientConnected` / `ClientDisconnected` — raised as clients register and leave.

### `MeshClient`

```csharp
new MeshClient(
    logger,
    idleTimeout: TimeSpan.FromSeconds(90),      // treat the hub as lost if no frame arrives in time (default: none)
    sendTimeout: TimeSpan.FromSeconds(5),        // abandon a send that stalls, as a TimeoutException (default: none)
    maxSendAttempts: 3,                          // retry a transient send failure up to this many attempts (default: 1, no retry)
    sendRetryDelay: TimeSpan.FromMilliseconds(100)); // base delay between retries, scaled linearly per attempt (default: 100 ms)
```

Set `idleTimeout` above the hub's heartbeat interval so the hub's pings keep the connection
alive; a genuinely silent hub then trips the timeout and raises `Disconnected(ConnectionLost)`.

`SendAsync`, `BroadcastAsync` and `SendToGroupAsync` honour the send policy: each send is bounded by
`sendTimeout`, and a send that fails with a transient transport error (a timeout or an I/O/socket
failure) is retried up to `maxSendAttempts`, waiting `sendRetryDelay` multiplied by the attempt number
between tries. Logic errors, a cancelled `CancellationToken`, and a closed connection are never retried.
The defaults — one attempt, no timeout — preserve the original fire-and-forget behaviour.

## Wire protocol

The TCP transport frames every message as a **4-byte big-endian length prefix** followed by the
payload (maximum 1 MiB). The first payload byte is the message type.

Protocol version: **2**.

| Type | Byte | Direction | Payload after the type byte |
|---|---|---|---|
| `RegistrationRequest` | `0x04` | client → hub | version (1 byte), UTF-8 name |
| `RegistrationComplete` | `0x01` | hub → client | assigned client id (16 bytes) |
| `Error` | `0x05` | hub → client | registration error code (1 byte) |
| `SendMessage` | `0x02` | client → hub | recipient id (16 bytes), message bytes |
| `BroadcastMessage` | `0x0B` | client → hub | message bytes |
| `JoinGroup` | `0x0C` | client → hub | UTF-8 group name |
| `LeaveGroup` | `0x0D` | client → hub | UTF-8 group name |
| `GroupMessage` | `0x0E` | client → hub | group-name length (2 bytes, big-endian), UTF-8 group name, message bytes |
| `DeliverMessage` | `0x03` | hub → client | sender id (16 bytes), message bytes |
| `DeliverGroupMessage` | `0x0F` | hub → client | sender id (16 bytes), group-name length (2 bytes, big-endian), UTF-8 group name, message bytes |
| `ClientLookupRequest` | `0x06` | client → hub | correlation id (4 bytes, big-endian), UTF-8 name |
| `ClientLookupResponse` | `0x07` | hub → client | correlation id (4 bytes), found flag (1 byte), id (16 bytes if found) |
| `Disconnect` | `0x08` | either | none |
| `Ping` | `0x09` | hub → client | none |
| `Pong` | `0x0A` | client → hub | none |

Registration error codes (`RegistrationErrorCode`): `DuplicateClientName` (`0x01`),
`UnsupportedProtocolVersion` (`0x02`), `ClientNameTooLong` (`0x03`), `HubAtCapacity` (`0x04`).

## Custom transports

Implement `ITransport` (and `ITransportListener` for the hub) to run Meshworx over any
message-oriented channel. Implementations are responsible for their own framing; the hub and
client treat each `ReceiveAsync` result as one complete message. `SendAsync` must be safe to
call concurrently; `ReceiveAsync` is single-reader.

The bundled `InMemoryTransport` runs the entire stack in one process without sockets — clients
connect by calling `InMemoryTransportListener.Connect()` instead of `TcpTransport.ConnectAsync`:

```csharp
var listener = new InMemoryTransportListener();
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

await using var client = new MeshClient(clientLogger);
await client.ConnectAsync(listener.Connect(), "Alice");
```

## Building and testing

```sh
dotnet build src/AdamSalisbury.Meshworx/AdamSalisbury.Meshworx.csproj
dotnet test  src/Tests/AdamSalisbury.Meshworx.UnitTests/AdamSalisbury.Meshworx.UnitTests.csproj
```

## Licence

See [LICENSE](LICENSE).
