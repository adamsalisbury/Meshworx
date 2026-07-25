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

- **Registration.** `ConnectAsync` performs a handshake: the client sends its name, protocol
  version and an optional opaque credential; the hub assigns a `Guid` and returns it, or refuses
  with a `RegistrationRefusedException` carrying a `RegistrationErrorCode` (duplicate name,
  unsupported version, name too long, hub at capacity, or authentication failed). Client names are
  unique and limited to 256 characters. See [Security](#security) for the authentication seam.
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
  unlike `MessageReceived` — carries the name of the group the message was sent to. **Sending to a
  group requires membership of it**: the hub drops a group message from a client that has not joined.
  Who may join is the hub's decision — see [Security](#security) for the authorisation seam; without
  one, any client may join any group.
- **Disconnect.** Calling `DisconnectAsync` is graceful and does not raise `Disconnected`. The
  `Disconnected` event fires only for unexpected endings — the hub closing the connection
  (`RemoteDisconnect`) or the transport failing (`ConnectionLost`). After it fires the client
  has reset and may reconnect from the handler. A remote drop landing at the same moment as the
  `DisconnectAsync` call does not change that: the disconnect the application asked for wins and
  the event stays silent. The one exception is a `DisconnectAsync` that arrives after the client
  has already published its disconnected state, by which point the event is committed.
- **Auto-reconnect.** `MeshClientReconnector` wraps a client and keeps it connected: it does a
  fail-fast initial connect, then transparently re-establishes the connection (bounded per attempt,
  retried with a delay) whenever it drops. It automatically re-joins the groups the client belonged
  to before the drop, then raises `Reconnected` so the application can restore any further state. Pass
  `restoreGroupMembership: false` to take full manual control of group restoration. Restoration re-joins
  over the wire, so a hub with a `GroupAuthoriser` authorises each re-join afresh and may refuse it;
  a refused group is dropped from the client's membership rather than retried. `Reconnected` means
  the connection is up again rather than that the reconnector re-established it, and it may fire more
  than once for a single drop, so keep your handlers idempotent.

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
    maxMissedHeartbeats: 2,                         // evict after this many silent intervals
    authenticator: authenticator,                   // decide who may register (default: none — see Security)
    maxConcurrentAuthentications: 64,               // cap concurrent authenticator calls (default: 64)
    groupAuthoriser: groupAuthoriser,               // decide who may join a group (default: none — see Security)
    groupAuthorisationTimeout: TimeSpan.FromSeconds(10)); // refuse a join whose authoriser hangs (default: 10s)
```

When a heartbeat interval is set, the hub pings idle clients and evicts any that fail to send a
frame across the configured number of consecutive intervals, detecting half-open connections.

The count is of silent intervals, not of unanswered pings: with the default `maxMissedHeartbeats: 2`
and a 30-second interval, a client that goes completely silent is pinged at 30 seconds and evicted at
60 seconds. Any frame from the client — a pong, or ordinary traffic — resets the count. Setting
`maxMissedHeartbeats: 1` evicts on the first silent interval without probing first — there is no
interval left in which a ping could be answered — so a client that only receives is evicted every
interval. Use 2 or more unless clients are expected to send continuously; the hub logs a warning at
construction if you set 1 alongside a heartbeat interval.

Observability:

- `ConnectedClientCount` — current number of registered clients.
- `IsClientRegistered(id)` — whether a specific client is registered.
- `ClientConnected` / `ClientDisconnected` — raised as clients register and leave.

### `MeshClient`

```csharp
new MeshClient(
    logger,
    idleTimeout: TimeSpan.FromSeconds(90),      // treat the hub as lost if no frame arrives in time (default: none)
    sendTimeout: TimeSpan.FromSeconds(5),        // cancel a send that stalls, surfacing a TimeoutException (default: none)
    maxSendAttempts: 3,                          // retry a transient send I/O failure up to this many attempts (default: 1, no retry)
    sendRetryDelay: TimeSpan.FromMilliseconds(100)); // base delay between retries, scaled linearly per attempt (default: 100 ms)
```

Set `idleTimeout` above the hub's heartbeat interval so the hub's pings keep the connection
alive; a genuinely silent hub then trips the timeout and raises `Disconnected(ConnectionLost)`.

`SendAsync`, `BroadcastAsync` and `SendToGroupAsync` honour the send policy: each send is bounded by
`sendTimeout` (a stalled send is cancelled — releasing the transport rather than blocking the
connection — and surfaces as a `TimeoutException`), and a send that fails with a transient transport I/O
error (an `IOException` or `SocketException`) is retried up to `maxSendAttempts`, waiting `sendRetryDelay`
multiplied by the attempt number between tries. A timeout is not retried, since a cancelled send may have
partially written; logic errors, a cancelled `CancellationToken`, and a closed connection are never
retried either. The defaults — one attempt, no timeout — preserve the original fire-and-forget behaviour.

## Security

Meshworx has no built-in authentication or encryption by default. Treat these as decisions you
must make deliberately.

- **Authentication.** The hub admits any peer that completes the handshake unless you supply a
  `ClientAuthenticator`. It is invoked for every registration with the client's name and the opaque
  credential it sent, and returning `false` refuses the client with
  `RegistrationErrorCode.AuthenticationFailed`:

  ```csharp
  ClientAuthenticator authenticator = (context, _) =>
  {
      bool ok = CredentialStore.IsValid(context.ClientName, context.Credential.Span);
      return ValueTask.FromResult(ok);
  };

  await using var hub = new MeshHub(logger, listener, authenticator: authenticator);
  ```

  The client supplies its credential through `ConnectAsync(transport, name, credential)` (and
  `MeshClientReconnector`'s `credential` parameter, which re-sends it on every reconnect).

  The authenticator runs on **unauthenticated input**, once per accepted connection, so any peer that
  can reach the port can cause it to run. Compare credentials in constant time, and keep the callback
  cheap or externally rate-limited. The hub caps how many callbacks run at once —
  `maxConcurrentAuthentications`, 64 by default — so a connection flood cannot turn a deliberately
  expensive credential check into a denial of service; a connection that cannot get a slot within
  `registrationTimeout` is refused with `AuthenticationFailed`. An authenticator that throws, hangs
  past `registrationTimeout`, or cancels is treated as a refusal rather than faulting the hub.

- **Authorisation: groups.** Authentication answers *who this peer is*; authorisation answers *what it
  may do*. Once admitted, a client can still reach every other client by direct send, broadcast and
  lookup — but groups are an enforceable boundary:

  - **Sending to a group always requires membership of it**, with or without the callback below. The
    hub drops a group message from a non-member rather than fanning it out, so a client cannot inject a
    frame into a group it never joined.
  - **Who may join is yours to decide.** Supply a `GroupAuthoriser` and it is consulted for every join.
    Returning `false` refuses it, and the client is told so through its `GroupJoinRefused` event:

    ```csharp
    GroupAuthoriser groupAuthoriser = (context, _) =>
        ValueTask.FromResult(TenantDirectory.MayJoin(context.ClientName, context.GroupName));

    await using var hub = new MeshHub(
        logger, listener, authenticator: authenticator, groupAuthoriser: groupAuthoriser);
    ```

  The authoriser composes with the authenticator rather than replacing it: `context.ClientName` is the
  name that passed authentication, so it identifies the client exactly as strongly as your
  `ClientAuthenticator` does — **with no authenticator configured the name is self-asserted and is not
  an identity**. Authorise on that name and `context.ClientId`, and treat `context.GroupName` as
  untrusted input, matching it against known groups rather than parsing meaning out of it.

  Every join is authorised on its own, including the re-joins that follow a reconnect, so a decision is
  never carried across a connection and `MeshClientReconnector`'s membership restoration cannot
  reinstate a membership you would now refuse. A refusal is not retried, so an authoriser that fails
  closed on a transient outage costs the client its membership until it asks again.

  The callback runs on input from an already-admitted client, driven from that client's own receive
  loop, which reads nothing further from it until the callback returns — so a slow decision stalls only
  the client that asked, and concurrent invocations are bounded by the number of connected clients.
  One that throws, cancels, or outruns `groupAuthorisationTimeout` refuses the join: the decision fails
  closed.

  **Without a `GroupAuthoriser` the hub authorises no joins and any client may join any group**, so
  groups are then a routing convenience and must not be relied on for isolation.

- **Network exposure.** The `TcpTransportListener(int port)` convenience constructor binds to
  `IPAddress.Loopback`, so a hub created that way is not reachable from other hosts. To listen on a
  public interface, pass an explicit `IPEndPoint` — and only do so behind an authenticator, a
  network boundary, or both.

- **Transport encryption (TLS).** The TCP transport runs cleartext by default and secured when you
  give it TLS options. Pass `SslServerAuthenticationOptions` to the listener and
  `SslClientAuthenticationOptions` to `TcpTransport.ConnectAsync`; the framing is identical either
  way, so nothing else changes.

  ```csharp
  // Hub
  var listener = new TcpTransportListener(
      new IPEndPoint(IPAddress.Any, 9000),
      new SslServerAuthenticationOptions { ServerCertificate = hubCertificate });

  await using var hub = new MeshHub(logger, listener);
  await hub.StartAsync();

  // Client
  await using var client = new MeshClient(clientLogger);
  await client.ConnectAsync(
      await TcpTransport.ConnectAsync("hub.example.com", 9000, new SslClientAuthenticationOptions()),
      "Alice");
  ```

  For **mutual TLS**, set `ClientCertificateRequired = true` and a
  `RemoteCertificateValidationCallback` on the server options, and supply `ClientCertificates` on
  the client options. That authenticates the peer at the transport, which composes with — and is
  independent of — the application-level `ClientAuthenticator` above.

  Practical notes:

  - `TargetHost` defaults to the host passed to `ConnectAsync`, which is what the hub's certificate
    is validated against. Both option objects are copied on the way in, so reassigning a property on
    your instance afterwards does not change a live listener or connection. The copy is shallow, so
    mutating a shared object you passed in — the `ClientCertificates` collection, the
    `CertificateChainPolicy` — still will; treat those as immutable once handed over.
  - Leave `EnabledSslProtocols` unset so the platform negotiates its best available version.
    `AllowRenegotiation` and `CertificateRevocationCheckMode` are likewise passed through untouched,
    so they keep their platform defaults — notably, revocation is **not** checked unless you ask for
    it.
  - A `RemoteCertificateValidationCallback` that always returns `true` accepts any certificate from
    anyone and reduces TLS to obfuscation. Validate or pin properly.
  - `TcpTransport.IsEncrypted` reports whether a connection actually negotiated TLS — worth
    asserting in a start-up check.
  - `MeshClientReconnector` needs no change: its transport factory just calls the TLS overload, so
    every reconnect renegotiates —
    `async ct => (ITransport)await TcpTransport.ConnectAsync("hub.example.com", 9000, tlsOptions, ct)`.
  - Handshakes run **off** the accept path, and accepting is never gated on a handshake bound, so a
    flood of peers that connect and then stay silent cannot stop the listener admitting anyone. A
    connection counts against `maxConcurrentTlsHandshakes` (64) only once its peer has actually sent
    something, which bounds handshake CPU without letting silent peers hold the budget; sixteen times
    that many may be waiting to negotiate, beyond which new connections are refused rather than
    queued. Every negotiation is bounded by `tlsHandshakeTimeout` (10 seconds). A handshake that
    fails or times out drops that connection only; the hub never sees it.
  - A transient accept failure does not retire the listener — the pump pauses briefly and carries on,
    so a temporary descriptor shortage cannot leave the hub silently accepting nothing for the rest
    of its life.

- **Confidentiality.** Without TLS options the bundled TCP transport is cleartext: client names,
  assigned ids, group names and every message payload cross the wire in the clear, and an on-path
  attacker can modify them. Configure TLS as above, or run inside an already-encrypted channel
  (VPN, service-mesh mTLS, a TLS-terminating proxy), whenever traffic crosses an untrusted segment.

- **Sender identity is hop-by-hop, not end to end.** TLS secures each client–hub connection
  separately. The sender id in a delivered message is asserted by the hub, not signed by the sending
  client, so a recipient is trusting the hub. A compromised hub can forge any sender. Sign payloads
  at the application layer if you need end-to-end authenticity.

## Wire protocol

The TCP transport frames every message as a **4-byte big-endian length prefix** followed by the
payload (maximum 1 MiB). The first payload byte is the message type. Enabling TLS does not change
any of this — the same frames simply travel inside the TLS record layer.

Protocol version: **3**.

| Type | Byte | Direction | Payload after the type byte |
|---|---|---|---|
| `RegistrationRequest` | `0x04` | client → hub | version (1 byte), name length (2 bytes, big-endian), UTF-8 name, opaque credential (remaining bytes) |
| `RegistrationComplete` | `0x01` | hub → client | assigned client id (16 bytes) |
| `Error` | `0x05` | hub → client | registration error code (1 byte) |
| `SendMessage` | `0x02` | client → hub | recipient id (16 bytes), message bytes |
| `BroadcastMessage` | `0x0B` | client → hub | message bytes |
| `JoinGroup` | `0x0C` | client → hub | UTF-8 group name |
| `LeaveGroup` | `0x0D` | client → hub | UTF-8 group name |
| `GroupMessage` | `0x0E` | client → hub | group-name length (2 bytes, big-endian), UTF-8 group name, message bytes |
| `DeliverMessage` | `0x03` | hub → client | sender id (16 bytes), message bytes |
| `DeliverGroupMessage` | `0x0F` | hub → client | sender id (16 bytes), group-name length (2 bytes, big-endian), UTF-8 group name, message bytes |
| `GroupJoinRefused` | `0x10` | hub → client | UTF-8 group name |
| `ClientLookupRequest` | `0x06` | client → hub | correlation id (4 bytes, big-endian), UTF-8 name |
| `ClientLookupResponse` | `0x07` | hub → client | correlation id (4 bytes), found flag (1 byte), id (16 bytes if found) |
| `Disconnect` | `0x08` | either | none |
| `Ping` | `0x09` | hub → client | none |
| `Pong` | `0x0A` | client → hub | none |

`GroupJoinRefused` is an addition within version 3 rather than a new protocol version: it travels only
from hub to client, and a client that does not recognise it ignores it, so it changes nothing for a peer
that never sees one. A hub drops a `GroupMessage` from a client that is not a member of the target group,
and does so silently — a correct client only sends to groups it has joined, and it learns of a refused
join from `GroupJoinRefused`.

Registration error codes (`RegistrationErrorCode`): `DuplicateClientName` (`0x01`),
`UnsupportedProtocolVersion` (`0x02`), `ClientNameTooLong` (`0x03`), `HubAtCapacity` (`0x04`),
`AuthenticationFailed` (`0x05`).

A registration frame whose declared name length is zero, or which runs past the payload, is malformed:
the hub drops the connection without replying.

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
