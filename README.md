# Meshworx

Flexible and unopinionated messaging library.

Meshworx connects named clients through a central **hub**. Each client registers with a
unique name, looks up other clients by name, and exchanges opaque byte-payload messages that
the hub routes to the intended recipient. The transport is pluggable — a length-prefixed TCP
transport ships in the box, and any `ITransport`/`ITransportListener` implementation can be
substituted.

- **Target framework:** .NET 10
- **Namespaces:** `AdamSalisbury.Meshworx`, `AdamSalisbury.Meshworx.Messages`,
  `AdamSalisbury.Meshworx.Transport`, `AdamSalisbury.Meshworx.Transport.Tcp`,
  `AdamSalisbury.Meshworx.Transport.WebSocket`,
  `AdamSalisbury.Meshworx.Extensions.DependencyInjection`

## Architecture

| Component | Responsibility |
|---|---|
| `MeshHub` (`IMeshHub`) | Accepts client connections, tracks registered clients by id and name, and routes messages between them. |
| `MeshClient` (`IMeshClient`) | Connects to a hub, sends messages, looks clients up by name, and raises events for inbound messages and disconnects. |
| `ITransport` | A bidirectional, message-oriented channel. Implementations own their framing. |
| `ITransportListener` | Accepts inbound transport connections for the hub. |
| `TcpTransport` / `TcpTransportListener` | TCP implementation using a 4-byte big-endian length prefix per frame. |
| `WebSocketTransport` / `WebSocketTransportListener` | WebSocket implementation reachable from a browser and through proxies and firewalls that block arbitrary TCP ports; one WebSocket binary message carries one Meshworx frame. |
| `InMemoryTransport` / `InMemoryTransportListener` | In-process implementation backed by channels, for hosting a hub and clients in one process and for fast, deterministic testing. |
| `AddMeshHub` / `AddMeshClient` | `IServiceCollection` extension methods, in the `AdamSalisbury.Meshworx.Extensions.DependencyInjection` package, that register a hub or client for dependency injection and the generic host. |

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
- **Message headers.** `SendAsync(recipientId, payload, headers)` and
  `SendToGroupAsync(name, payload, headers)` accept a `MessageHeaders` — a small, string-keyed bag of
  metadata (a correlation id, a content-type hint, and the like) that travels alongside the payload
  without the hub ever inspecting it. `MessageReceivedEventArgs.Headers` and
  `GroupMessageReceivedEventArgs.Headers` expose what the sender attached, defaulting to
  `MessageHeaders.Empty` when there were none. Requires both ends to have negotiated protocol version
  5 or higher — see [Wire protocol](#wire-protocol) for the frame layout and the fallback behaviour
  when a group has members on different negotiated versions.
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

### Event handlers

Every event Meshworx raises — the reconnector's `Reconnected`, the client's `MessageReceived`,
`GroupMessageReceived`, `GroupJoinRefused` and `Disconnected`, and the hub's `ClientConnected` and
`ClientDisconnected` — is an ordinary `EventHandler` or `EventHandler<T>`, raised synchronously and
wrapped in a catch that logs whatever a handler throws rather than letting it propagate.

Where they are raised from differs, and it matters. The client's four and the reconnector's
`Reconnected` each come from the single loop that owns that connection, so for any one client those
handlers run one at a time. The hub's two do not: they are raised from each client's handler task, one
per accepted connection, so `ClientConnected` and `ClientDisconnected` handlers can be invoked
concurrently for different clients and must be thread-safe.

That containment reaches only as far as the handler's first suspension. An `async void` handler
returns to whatever raised the event at that point, so everything it does afterwards runs outside that
`try`/`catch`: an exception thrown once the handler has suspended is rethrown on the thread pool, or
on whatever synchronisation context the handler captured, where nothing observes it and it can bring
the process down. The raiser carries on without waiting either, so an `async void` handler's work may
still be in flight when the next event is raised.

Keep handlers synchronous. Where one must do asynchronous work, start that work from the handler and
contain its failures inside the task you start:

```csharp
reconnector.Reconnected += (_, _) => _ = RestoreStateAsync();

async Task RestoreStateAsync()
{
    try
    {
        await reconnector.Client.SendAsync(serverId, resumePayload);
    }
    catch (Exception ex)
    {
        // The containment boundary: nothing else can observe this task.
        logger.LogError(ex, "Restoring application state after a reconnect failed");
    }
}
```

The event signature carries no completion for the raiser to await, so the handler is the only place
that failure can be caught. `Reconnected` may fire more than once for a single drop, so the work it
starts must be idempotent and safe to overlap with a run already under way — and note that `retryDelay`
does not pace it, since that delay applies only between *failed* connect attempts. A hub that accepts a
connection and then immediately drops it re-raises `Reconnected` as fast as a connection round trip, so
bound the work yourself if it is expensive.

## Configuration

### `MeshHub`

```csharp
new MeshHub(
    logger,
    listener,
    registrationTimeout: TimeSpan.FromSeconds(10),  // drop connections that never register
    maxClients: 1000,                               // refuse registration beyond this (default: 1000)
    heartbeatInterval: TimeSpan.FromSeconds(30),    // ping idle clients (default: 30 seconds)
    maxMissedHeartbeats: 2,                         // evict after this many silent intervals
    authenticator: authenticator,                   // decide who may register (default: none — see Security)
    maxConcurrentAuthentications: 64,               // cap concurrent authenticator calls (default: 64)
    groupAuthoriser: groupAuthoriser,               // decide who may join a group (default: none — see Security)
    groupAuthorisationTimeout: TimeSpan.FromSeconds(10), // refuse a join whose authoriser hangs (default: 10s)
    maxConnectionsPerRemoteEndpoint: 100);          // cap connections from one address (default: 100)
```

Every one of these has a finite default, so a hub constructed with no arguments at all is still
bounded: at most 1000 registered clients, at most 100 concurrent connections from any single remote
address, and idle clients evicted rather than held open forever. `maxClients` and
`maxConnectionsPerRemoteEndpoint` both accept `int.MaxValue` to opt back into no limit, and
`heartbeatInterval` accepts `Timeout.InfiniteTimeSpan` to disable idle eviction entirely — only do
this if something else bounds how long a connection may sit unused.

The hub pings idle clients and evicts any that fail to send a frame across the configured number of
consecutive intervals, detecting half-open connections.

The count is of silent intervals, not of unanswered pings: with the default `maxMissedHeartbeats: 2`
and the default 30-second interval, a client that goes completely silent is pinged at 30 seconds and
evicted at 60 seconds. Any frame from the client — a pong, or ordinary traffic — resets the count.
Setting `maxMissedHeartbeats: 1` evicts on the first silent interval without probing first — there is
no interval left in which a ping could be answered — so a client that only receives is evicted every
interval. Use 2 or more unless clients are expected to send continuously; the hub logs a warning at
construction if you set 1 without also disabling idle eviction.

`maxConnectionsPerRemoteEndpoint` bounds connections rather than registered clients, and is checked in
the accept loop before any handshake — a flood of connections from one address that never complete
registration is refused there rather than sailing past `maxClients`, which only counts clients that
have actually registered. It is only enforced for a transport that reports where it connected from
(the bundled `TcpTransport` does); a transport with no meaningful remote address, such as the
in-process one, is never capped by it. An IPv6 address is grouped with every other address in its /64
network prefix before the cap applies — a single host is routinely handed an entire /64, so without
this a single source could defeat the cap by connecting from a different address within it each time.

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
    frame into a group it never joined. Membership is the single capability — there is no send-only
    permission, so a client that previously published to a group without joining must now join it, and
    will then also receive that group's traffic.
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
  the client that asked. One that throws, cancels, or outruns `groupAuthorisationTimeout` refuses the
  join: the decision fails closed.

  Two consequences worth designing for. **`groupAuthorisationTimeout` bounds how long the hub waits, not
  how long your callback runs** — a callback that outruns it is abandoned and carries on, so a client
  that keeps asking after each refusal can leave invocations piling up. Across clients the ceiling is
  the number of connected clients, which is `maxClients` only if you set one. An authoriser that holds a
  resource per call — a database connection, an HTTP client — should therefore bound its own
  concurrency; a mass reconnect re-joins every group at once. And keep the timeout comfortably below
  `heartbeatInterval × maxMissedHeartbeats`, or a slow-but-working authoriser will have the hub evict
  the very client whose join it is still deciding.

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

Protocol version: negotiated between **4** (minimum) and **5** (maximum). Registration advertises a
range rather than a single value — `[versionMin, versionMax]` — and the hub picks the highest version
common to its own supported range and the client's, so a newer client connecting to an older hub (or
vice versa) negotiates down instead of being refused outright. Only the shared feature set of the
negotiated version is guaranteed to work; `IMeshClient.NegotiatedProtocolVersion` reports what was
agreed for the current connection.

| Type | Byte | Direction | Payload after the type byte |
|---|---|---|---|
| `RegistrationRequest` | `0x04` | client → hub | version min (1 byte), version max (1 byte), name length (2 bytes, big-endian), UTF-8 name, opaque credential (remaining bytes) |
| `RegistrationComplete` | `0x01` | hub → client | assigned client id (16 bytes), negotiated version (1 byte) |
| `Error` | `0x05` | hub → client | registration error code (1 byte) |
| `SendMessage` | `0x02` | client → hub | recipient id (16 bytes), message bytes |
| `SendMessageWithHeaders` | `0x11` | client → hub | recipient id (16 bytes), header-block length (2 bytes, big-endian), header block, message bytes |
| `BroadcastMessage` | `0x0B` | client → hub | message bytes |
| `JoinGroup` | `0x0C` | client → hub | UTF-8 group name |
| `LeaveGroup` | `0x0D` | client → hub | UTF-8 group name |
| `GroupMessage` | `0x0E` | client → hub | group-name length (2 bytes, big-endian), UTF-8 group name, message bytes |
| `GroupMessageWithHeaders` | `0x13` | client → hub | group-name length (2 bytes, big-endian), UTF-8 group name, header-block length (2 bytes, big-endian), header block, message bytes |
| `DeliverMessage` | `0x03` | hub → client | sender id (16 bytes), message bytes |
| `DeliverMessageWithHeaders` | `0x12` | hub → client | sender id (16 bytes), header-block length (2 bytes, big-endian), header block, message bytes |
| `DeliverGroupMessage` | `0x0F` | hub → client | sender id (16 bytes), group-name length (2 bytes, big-endian), UTF-8 group name, message bytes |
| `DeliverGroupMessageWithHeaders` | `0x14` | hub → client | sender id (16 bytes), group-name length (2 bytes, big-endian), UTF-8 group name, header-block length (2 bytes, big-endian), header block, message bytes |
| `GroupJoinRefused` | `0x10` | hub → client | UTF-8 group name |
| `ClientLookupRequest` | `0x06` | client → hub | correlation id (4 bytes, big-endian), UTF-8 name |
| `ClientLookupResponse` | `0x07` | hub → client | correlation id (4 bytes), found flag (1 byte), id (16 bytes if found) |
| `Disconnect` | `0x08` | either | none |
| `Ping` | `0x09` | hub → client | none |
| `Pong` | `0x0A` | client → hub | none |

`GroupJoinRefused` is an addition within an existing protocol version rather than a new one: it travels
only from hub to client, and a client that does not recognise it ignores it, so it changes nothing for a
peer that never sees one. A hub drops a `GroupMessage` from a client that is not a member of the target
group, and does so silently — a correct client only sends to groups it has joined, and it learns of a
refused join from `GroupJoinRefused`.

### Header block format

A header block (used by `SendMessageWithHeaders`, `DeliverMessageWithHeaders`,
`GroupMessageWithHeaders` and `DeliverGroupMessageWithHeaders`) is a flat, back-to-back run of entries
— `[keyLength(1 byte)][UTF-8 key][valueLength(2 bytes, big-endian)][UTF-8 value]` — read until exactly
as many bytes as the preceding block-length field declared have been consumed. There is no entry count:
the block's own length is the only thing bounding it. A key longer than 255 bytes once UTF-8 encoded,
or a value longer than 65535 bytes, cannot be represented.

Headers exist so cross-cutting metadata (a correlation id, a content-type hint, trace context, and the
like) can travel with a message without the hub ever parsing the message body: the hub reads only the
header block's length, never its contents, and forwards the body untouched. A message with no headers
uses the plain frame (`SendMessage`/`DeliverMessage`/`GroupMessage`/`DeliverGroupMessage`) exactly as
before, so it costs nothing extra on the wire — the header-bearing opcodes and their length-prefixed
block are only ever used when there is at least one header to carry.

Headers require both ends of a connection to have negotiated protocol version 5 or higher. On a
connection negotiated below that, `MeshClient.SendAsync`/`SendToGroupAsync` throw
`NotSupportedException` if called with a non-empty `MessageHeaders` rather than silently dropping them.
For a group message, each member's own negotiated version decides what it receives: a member on version
5 or higher gets the header-bearing frame with the header block intact, while a member still on an
older version gets the plain frame with the header block stripped, since it would not recognise the
header-bearing opcode.

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

The bundled `WebSocketTransport`/`WebSocketTransportListener` reaches a hub over `ws://` or
`wss://` — the only way to connect from a browser, and one that traverses proxies and firewalls
that block arbitrary TCP ports. One WebSocket binary message carries exactly one Meshworx frame,
so no separate length prefix is needed; the 1 MiB payload cap and the wire protocol above are
otherwise unchanged:

```csharp
// Hub
var listener = new WebSocketTransportListener(port: 22002);
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

// Client
await using var client = new MeshClient(clientLogger);
await client.ConnectAsync(
    await WebSocketTransport.ConnectAsync(new Uri("ws://localhost:22002/")), "Alice");
```

Securing it with TLS (`wss://`) follows the same shape as the TCP transport — pass
`SslServerAuthenticationOptions` to the listener, and configure the client's
`ClientWebSocketOptions` (for a certificate validation callback or a client certificate) through
`WebSocketTransport.ConnectAsync`'s `configureOptions` callback:

```csharp
// Hub
var listener = new WebSocketTransportListener(
    new IPEndPoint(IPAddress.Any, 22002),
    tlsOptions: new SslServerAuthenticationOptions { ServerCertificate = hubCertificate });

// Client
await WebSocketTransport.ConnectAsync(
    new Uri("wss://hub.example.com:22002/"),
    options => options.RemoteCertificateValidationCallback = MyValidationCallback);
```

Negotiation — the TLS handshake where configured, then parsing the HTTP upgrade request — runs
off the accept path, exactly as the TCP transport's TLS handshake does, so one slow or hostile
peer cannot head-of-line block every other client waiting to connect.

## Dependency injection and hosting

The `AdamSalisbury.Meshworx.Extensions.DependencyInjection` package registers a hub or client
with `Microsoft.Extensions.DependencyInjection` and runs it alongside a generic host or ASP.NET
Core application, instead of the application managing `StartAsync`/`StopAsync` and disposal by
hand.

### Hosting a hub

```csharp
using AdamSalisbury.Meshworx;

builder.Services.AddMeshHub(options =>
{
    options.Port = 22001;
    options.MaxClients = 1000;
});
```

`AddMeshHub` registers a singleton `IMeshHub` — built from a `TcpTransportListener` on
`MeshHubOptions.Port` by default, or from `MeshHubOptions.Listener` when one is supplied — and a
hosted service that calls `StartAsync` when the host starts and `StopAsync` when it begins a
graceful shutdown, draining connected clients before the process exits. An overload binds
`MeshHubOptions` from an `IConfiguration` section:

```csharp
builder.Services.AddMeshHub(builder.Configuration.GetSection("MeshHub"));
```

Every `MeshHubOptions` property mirrors a `MeshHub` constructor parameter and carries the same
default — see [Configuration](#configuration) above. An out-of-range `Port` fails host start
with an `OptionsValidationException` rather than surfacing later as a socket error.

### Hosting a client

```csharp
using AdamSalisbury.Meshworx;

builder.Services.AddMeshClient("Alice", options =>
{
    options.Host = "localhost";
    options.Port = 22001;
    options.UseReconnector = true;
});
```

`AddMeshClient` registers the client as a keyed `IMeshClient`, resolved by the name it was added
with (`serviceProvider.GetRequiredKeyedService<IMeshClient>("Alice")`), and a hosted service that
connects it when the host starts and disconnects it on shutdown. Setting
`MeshClientOptions.UseReconnector` wraps the client in a `MeshClientReconnector` instead — the
keyed `IMeshClient` is then the reconnector's managed client, so callers use the same API either
way, and the reconnector (also resolvable by the same key, as `MeshClientReconnector`) is what
starts on host start and stops on host stop. By default the client connects over TCP to
`MeshClientOptions.Host`/`Port`; set `MeshClientOptions.TransportFactory` to use TLS or another
transport. As with the hub, an `IConfiguration` overload binds `MeshClientOptions` from a section,
and options are validated the same way on host start.

### Health checks

The same package registers health checks against `Microsoft.Extensions.Diagnostics.HealthChecks`,
so orchestrators and load balancers get a liveness/readiness signal without any glue code:

```csharp
builder.Services.AddHealthChecks()
    .AddMeshHub()
    .AddMeshClient("Alice");
```

`AddMeshHub` reports `Unhealthy` while the hub is not running, `Degraded` once it has reached
`MeshHubOptions.MaxClients` — still serving existing clients but refusing new ones — and `Healthy`
otherwise. `AddMeshClient` reports `Healthy` while the named client is connected and `Unhealthy`
otherwise, including while a `MeshClientReconnector` is still retrying. Both require the
corresponding `AddMeshHub`/`AddMeshClient` call to have registered the hub or client first, and
both accept an optional `name` — `AddMeshHub` defaults to `"meshhub"`, `AddMeshClient` to
`"meshclient:{clientName}"`.

## Observability

`AdamSalisbury.Meshworx` publishes first-class metrics through a `System.Diagnostics.Metrics.Meter`
named `AdamSalisbury.Meshworx`, so any OpenTelemetry, Prometheus or other exporter that already knows
how to collect from a named meter picks them up with no glue code:

```csharp
using OpenTelemetry.Metrics;

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("AdamSalisbury.Meshworx"));
```

Each `MeshHub` and each `MeshClientReconnector` owns its own `Meter` instance, disposed alongside it,
so instruments stop reporting the moment the component they describe is torn down rather than going on
publishing stale data. Every instrument below is under that one meter name regardless of which
component recorded it:

| Instrument | Kind | Tags | Description |
|---|---|---|---|
| `meshworx.hub.clients.connected` | up/down counter | — | Clients currently registered with the hub. |
| `meshworx.hub.messages.routed` | counter | `direction`: `direct`, `broadcast`, `group` | Messages the hub has routed. A broadcast or group send counts once per call that reaches at least one recipient, not once per recipient — it is the message the hub routed, not the number of deliveries it fanned out to — and not at all when there was nobody to receive it (the sender was the only client, or the group's only member). |
| `meshworx.hub.bytes.routed` | counter | `direction`: `direct`, `broadcast`, `group` | Message payload bytes the hub has routed, tagged the same way. |
| `meshworx.hub.messages.dropped` | counter | `reason`: `unknown-recipient`, `queue-full` | Messages the hub could not deliver: a direct send to a Guid nobody is registered under, or a write to a recipient's outbound queue that was already full. |
| `meshworx.hub.outbound_queue.depth` | observable gauge | — | The total number of frames currently queued for delivery, summed across every connected client's outbound queue. A single aggregate rather than one series per client, since tagging by client id would give the gauge unbounded cardinality over the hub's lifetime. |
| `meshworx.client.reconnects` | counter | — | The number of times a `MeshClientReconnector` has re-established a connection after an unexpected drop. Does not count the initial connection `StartAsync` makes. |

No protocol or payload change accompanies any of this — the instruments only observe routing and
connection lifecycle events that already happen.

## Building and testing

```sh
dotnet build src/AdamSalisbury.Meshworx/AdamSalisbury.Meshworx.csproj
dotnet test  src/Tests/AdamSalisbury.Meshworx.UnitTests/AdamSalisbury.Meshworx.UnitTests.csproj
```

## Licence

See [LICENSE](LICENSE).
