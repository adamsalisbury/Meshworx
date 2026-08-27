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
  `AdamSalisbury.Meshworx.Transport.WebSocket`, `AdamSalisbury.Meshworx.Transport.Unix`,
  `AdamSalisbury.Meshworx.Transport.NamedPipes`, `AdamSalisbury.Meshworx.Transport.Quic`,
  `AdamSalisbury.Meshworx.Transport.Framing`, `AdamSalisbury.Meshworx.Extensions.DependencyInjection`

## Architecture

| Component | Responsibility |
|---|---|
| `MeshHub` (`IMeshHub`) | Accepts client connections, tracks registered clients by id and name, and routes messages between them. |
| `MeshClient` (`IMeshClient`) | Connects to a hub, sends messages, looks clients up by name, and raises events for inbound messages and disconnects. |
| `ITransport` | A bidirectional, message-oriented channel. Implementations own their framing. |
| `ITransportListener` | Accepts inbound transport connections for the hub. |
| `TcpTransport` / `TcpTransportListener` | TCP implementation using a 4-byte big-endian length prefix per frame. |
| `WebSocketTransport` / `WebSocketTransportListener` | WebSocket implementation reachable from a browser and through proxies and firewalls that block arbitrary TCP ports; one WebSocket binary message carries one Meshworx frame. |
| `UnixSocketTransport` / `UnixSocketTransportListener` | Unix domain socket implementation for fast, local, same-host inter-process communication on Linux and macOS; shares the same length-prefixed framing as TCP. |
| `NamedPipeTransport` / `NamedPipeTransportListener` | Windows named-pipe implementation for the same same-host inter-process case on Windows; also shares the length-prefixed framing. |
| `QuicTransport` / `QuicTransportListener` | QUIC implementation (`System.Net.Quic`) using one bidirectional stream per connection; TLS 1.3 is mandatory rather than optional, and the same length-prefixed framing runs over the stream. |
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
- **Session resumption.** Off by default. Give the hub a `sessionResumptionWindow` and each registering
  client is issued an opaque resumption token; presenting it on a later connect reclaims the same
  `Guid` and the group memberships that identity held, so peers holding the id from before the drop go
  on reaching it. `IMeshClient.SessionResumed` reports whether the last connect reclaimed an identity.
  Everything about it degrades to an ordinary fresh registration — an expired token, a hub with the
  feature off, a connection negotiated below protocol version 6. Requires version 6. See
  [Session resumption](#session-resumption).
- **Offline delivery (store and forward).** Off by default: a message to a recipient the hub does not
  recognise is dropped. Give the hub an `IOfflineStore` and a **direct** message addressed to a client
  that has disconnected is held against that client's *name* instead, and delivered in arrival order
  when that name next registers. `InMemoryOfflineStore` is the bounded, process-local default;
  implement the interface yourself to back it with something durable. Broadcast and group sends never
  store — a disconnected client is not a member of anything to fan out to. See
  [Offline delivery](#offline-delivery) for the bounds and the limits of how long a departed client's
  id keeps resolving.
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
    maxConnectionsPerRemoteEndpoint: 100,           // cap connections from one address (default: 100)
    notifyOnQueueSaturation: false,                 // tell a direct sender its message was dropped (default: false)
    backpressureAwaitTimeout: TimeSpan.FromSeconds(30), // bound an AwaitCapacity park (default: 30s)
    offlineStore: null,                             // hold messages for disconnected clients (default: none)
    offlineStoreTimeout: TimeSpan.FromSeconds(10),  // bound a single offline store call (default: 10s)
    sessionResumptionWindow: null,                  // how long an identity may be reclaimed (default: off)
    maxInboundMessagesPerSecond: 200,               // per-client frame-rate budget (default: 200)
    maxInboundBytesPerSecond: 4 * 1024 * 1024,      // per-client byte-volume budget (default: 4 MiB)
    maxFanOutMessagesPerSecond: 20,                 // how often a client may broadcast/group-send (default: 20)
    maxFanOutDeliveriesPerSecond: 20_000);          // total deliveries those sends may cause (default: 20,000)
```

Every one of these has a finite default, so a hub constructed with no arguments at all is still
bounded: at most 1000 registered clients, at most 100 concurrent connections from any single remote
address, idle clients evicted rather than held open forever, and a registered client's inbound
traffic itself rate-limited. `maxClients` and `maxConnectionsPerRemoteEndpoint` both accept
`int.MaxValue` to opt back into no limit, and `heartbeatInterval` accepts `Timeout.InfiniteTimeSpan`
to disable idle eviction entirely — only do this if something else bounds how long a connection may
sit unused.

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

`maxInboundMessagesPerSecond` and `maxInboundBytesPerSecond` bound a single registered client's
inbound traffic — every frame type, including an empty one, charged independently by count and by
volume, each with a burst allowance equal to one second's own budget. `maxFanOutMessagesPerSecond`
applies on top of both, specifically to `BroadcastMessage` and `GroupMessage` frames: a single one of
these fans out to every recipient (every other client for a broadcast, every other member for a
group), so its cost to the hub is not the sender's alone to spend, and this bounds how often a client
may trigger one at all. A frequency budget alone does not bound the amplification that results from
it, though — at a given frequency, the number of deliveries a fan-out causes grows with the size of
the client population, without limit, unless something else catches it. `maxFanOutDeliveriesPerSecond`
is that something else: charged by the actual number of recipients each fan-out reaches rather than by
the frame, it keeps the hub's worst-case fan-out cost bounded by a figure that does not move just
because the population — or `maxFanOutMessagesPerSecond` itself — does. Its default, 20,000, is the
worst case the other defaults already implied, so a hub built with every default unchanged sees no new
limit from it in practice. A frame that exceeds any of these budgets is dropped without being
processed or queued, and without telling the sender — the same silent-drop behaviour the hub already
uses when a recipient's outbound queue is full. All four accept `int.MaxValue` to opt out.

These budgets are per connection, not per identity: each is created fresh, at full allowance, when a
client registers, and is discarded when it disconnects. A client that disconnects and reconnects gets
a new budget rather than resuming a spent one — deliberately, rather than by oversight. Keying a budget
on something that outlives the connection (a remote address, say) would need that state to persist
indefinitely to be effective, which is exactly the unbounded-growth shape `maxConnectionsPerRemoteEndpoint`'s
own bookkeeping is careful to avoid by discarding an address's entry the moment its connection count
returns to zero. Solving that properly is a distinct problem from the one these budgets solve, and is
better addressed by an authenticator, a network boundary, or a proxy that already rate-limits
connections — not by this library reintroducing the growth pattern it has already designed against
elsewhere. What these budgets do guarantee is that a client cannot exceed them without reconnecting to
reset them, which is a real and deliberate constraint, not a coincidental gap.

Observability:

- `ConnectedClientCount` — current number of registered clients.
- `IsClientRegistered(id)` — whether a specific client is registered.
- `ClientConnected` / `ClientDisconnected` — raised as clients register and leave.

### Session resumption

Every `ConnectAsync` mints a brand-new `Guid`, so after a drop every peer holding the old one is
addressing nothing, and the client's group memberships are gone. Session resumption fixes both — it is
off until you give the hub a window:

```csharp
await using var hub = new MeshHub(
    logger,
    listener,
    sessionResumptionWindow: TimeSpan.FromMinutes(5));
```

With it on, the hub issues each registering client an opaque 32-byte token alongside its id. On a later
connect the client presents the token, and if the identity is still reclaimable the hub gives it back:
same `Guid`, same group memberships, and a fresh token for next time. `MeshClient` does all of this
itself — there is nothing to call — and reports the outcome on `IMeshClient.SessionResumed`.

- **It never fails a connect.** An expired window, a token already spent, a hub with the feature off, a
  connection negotiated below version 6, or no answer at all: every one of them leaves the client
  connected on the fresh identity it just registered with, with `SessionResumed` false. Resumption is
  an optimisation over reconnecting, never a precondition for it.
- **Restored groups are re-authorised, not reinstated.** If you have a `GroupAuthoriser`, it is asked
  again for every group being restored and may refuse — a resumption cannot resurrect a membership you
  would now decline. A refused group is simply not restored.
- **The token is a bearer credential for an identity.** Anyone holding it can reclaim the `Guid` and
  group memberships of the name it was issued to (and only that name — a token presented by a client
  registered under a different name is refused). It goes over the wire once, in the registration reply,
  so **use an encrypted transport** if the network is not trusted; the same applies to the credential
  your `ClientAuthenticator` checks. The hub retains only a SHA-256 hash of each token, and each token
  is single-use — a successful resumption issues a new one and invalidates the old.
- **A live session cannot be taken over.** A token reclaims an identity nobody is currently using; it
  is refused while the connection that holds it is still up.
- **The window starts when the client disconnects**, and the session table is bounded by `maxClients`.
  If it is full, further clients are simply not issued tokens rather than evicting somebody else's
  reclaimable session.
- **Nothing survives a hub restart**, and a resumption is only meaningful on the hub that issued the
  token — there is no shared session state across hubs.

### Offline delivery

Store-and-forward is off unless you give the hub somewhere to put things:

```csharp
await using var hub = new MeshHub(
    logger,
    listener,
    offlineStore: new InMemoryOfflineStore(
        maxMessagesPerClient: 100,                  // per name (default: 100)
        maxBytesPerClient: 1024 * 1024,             // per name, body + headers (default: 1 MiB)
        timeToLive: TimeSpan.FromMinutes(5),        // discard undelivered after this (default: 5 minutes)
        maxClients: 1000));                         // distinct names holding messages (default: 1000)
```

A direct message addressed to a client the hub does not currently have connected is offered to the
store, keyed by the name that client last registered under, and everything held for that name is
delivered — oldest first — the next time it registers. What you need to know before relying on it:

- **Direct sends only.** Broadcast and group sends never store. Group membership is dropped when a
  client disconnects, so there is nothing to fan out to.
- **A departed client's id keeps resolving only until its name comes back.** Senders address a
  recipient by the `Guid` they looked up, so the hub remembers which name last held each id. That
  association is dropped the moment the name registers again — under a new id, which the old sender
  does not know — after which messages to the stale id are dropped as unknown-recipient rather than
  held for a client that is sitting there connected. The retention table is itself bounded by
  `maxClients`; past that, further disconnects are not retained.
- **A full store refuses rather than evicting.** `InMemoryOfflineStore` keeps what it already accepted
  and turns the new message away, which the hub counts as a drop. Implement `IOfflineStore` if you want
  the opposite policy.
- **Per-message time-to-live still applies on top.** A message sent with a TTL that lapses while it is
  held is dropped on its way out, exactly as it would have been had the recipient been connected and
  slow.
- **More may be held than a returning client's outbound queue can take at once** (it holds 1024
  frames). The overflow is dropped and counted, not held back for later.
- **Nothing survives a hub restart** with the in-memory store. Back it with a durable `IOfflineStore`
  if it must.
- **The store sits on a live connection's path.** It is called from the sending client's receive loop
  and from the returning client's registration, so a slow implementation delays those; each call is
  bounded by `offlineStoreTimeout`, after which the message takes the ordinary drop.

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

- **Inbound flood and amplification.** Authentication answers who a peer is; it does not limit what a
  registered client can then do. Without a rate limit, one client streaming `BroadcastMessage` or
  `GroupMessage` frames turns each inbound frame into a send to every recipient, so a single connection
  could compel the hub to emit a multiple of its own input volume and churn every recipient's outbound
  queue — and that multiple grows with the client population, without limit, unless something bounds it
  independently of frequency. `maxInboundMessagesPerSecond` and `maxInboundBytesPerSecond` bound a
  client's general inbound rate and volume; `maxFanOutMessagesPerSecond` bounds how often it may trigger
  a broadcast or group send at all; `maxFanOutDeliveriesPerSecond` bounds the total deliveries those
  sends may cause, charged by actual recipient count rather than by frame. All four have finite defaults
  and are enforced before a frame is otherwise looked at; see Configuration above for the figures and
  `int.MaxValue` opt-outs.

  These budgets bound a client for as long as it stays connected; they are discarded, not carried
  forward, when it disconnects, so a client that repeatedly registers, floods, and disconnects again
  is throttled within each connection but not across the cycle as a whole. See Configuration above for
  why: the alternative is state that must persist indefinitely to be effective, which is precisely the
  unbounded-growth shape the hub's other per-address bookkeeping is deliberately built to avoid. Guard
  against reconnect-driven flooding with an authenticator, a network boundary, or a connection-rate
  limit in front of the hub, exactly as you already would for `maxConnectionsPerRemoteEndpoint`.

- **Network exposure.** The `TcpTransportListener(int port)` convenience constructor binds to
  `IPAddress.Loopback`, so a hub created that way is not reachable from other hosts. To listen on a
  public interface, pass an explicit `IPEndPoint` — and only do so behind an authenticator, a
  network boundary, or both.

- **Local IPC access control.** `UnixSocketTransportListener` and `NamedPipeTransportListener`
  never cross the network at all, so their access boundary is the operating system's own filesystem
  permissions on the socket path or pipe name, not anything Meshworx enforces at the wire level. Both
  default to the tightest sensible permission for that boundary rather than leaving it to chance:
  `UnixSocketTransportListener` hardens its socket file to owner read/write only immediately after
  binding (an optional `socketFileMode` constructor parameter widens this if you genuinely need
  another local account to connect), and `NamedPipeTransportListener` creates its pipe with a security
  descriptor restricted to the current user (an optional `pipeSecurity` constructor parameter
  overrides this) — Windows' own unrestricted default for a named pipe additionally grants read access
  to the Everyone group and the anonymous account, which the explicit default here avoids. Neither
  transport offers a TLS option, since encrypting traffic that never leaves the host adds nothing.

- **QUIC is TLS-only.** Unlike TCP and WebSocket, `QuicTransportListener` and `QuicTransport.ConnectAsync`
  cannot be used cleartext at all — QUIC mandates TLS 1.3 at the protocol level, so both always take
  TLS options. Because it is a genuine network transport (unlike the two local-IPC ones above), it also
  reports a real `RemoteEndPoint` and so participates in `MeshHub`'s per-remote-endpoint connection cap
  the same way TCP and WebSocket do.

- **QUIC's negotiation pool has its own, separate per-source cap.** `QuicTransportListener` waits for
  each connection's first stream off the accept path, bounded by `maxConcurrentNegotiations` (default
  64) — but unlike the TCP and WebSocket listeners, there is no cheap way to tell a QUIC peer that will
  eventually open a stream apart from one that never will before actually waiting for it, so a single
  source completing real handshakes and never sending anything could otherwise occupy that entire pool
  alone. `maxConcurrentNegotiationsPerSource` (default one eighth of `maxConcurrentNegotiations`) caps
  how much of it any one source may hold, independently of the global limit.

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
any of this — the same frames simply travel inside the TLS record layer. The Unix domain socket,
named-pipe and QUIC transports use the identical framing — each runs over a stream-oriented channel
exactly as TCP does (QUIC's single bidirectional stream included), so all four share one internal
`StreamFramer` helper rather than reimplementing the length prefix and its bounds checking four
times. The WebSocket transport is the exception: one WebSocket binary message already delimits one
Meshworx frame, so it needs no length prefix of its own.

Protocol version: negotiated between **4** (minimum) and **6** (maximum). Registration advertises a
range rather than a single value — `[versionMin, versionMax]` — and the hub picks the highest version
common to its own supported range and the client's, so a newer client connecting to an older hub (or
vice versa) negotiates down instead of being refused outright. Only the shared feature set of the
negotiated version is guaranteed to work; `IMeshClient.NegotiatedProtocolVersion` reports what was
agreed for the current connection.

| Type | Byte | Direction | Payload after the type byte |
|---|---|---|---|
| `RegistrationRequest` | `0x04` | client → hub | version min (1 byte), version max (1 byte), name length (2 bytes, big-endian), UTF-8 name, opaque credential (remaining bytes) |
| `RegistrationComplete` | `0x01` | hub → client | assigned client id (16 bytes), negotiated version (1 byte), and — only when the negotiated version is 6 or higher and the hub has session resumption enabled — token length (2 bytes, big-endian) and the resumption token |
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
| `QueueSaturated` | `0x15` | hub → client | recipient id (16 bytes) — the direct-send recipient whose queue was full |
| `ResumeSession` | `0x16` | client → hub | resumption token |
| `SessionResumed` | `0x17` | hub → client | reclaimed client id (16 bytes), token length (2 bytes, big-endian), renewed resumption token |
| `SessionResumeRefused` | `0x18` | hub → client | none |

`GroupJoinRefused` and `QueueSaturated` are additions within an existing protocol version rather than new
ones: they travel only from hub to client, and a client that does not recognise them ignores them, so
they change nothing for a peer that never sees one. Session resumption could not take that route —
`ResumeSession` travels client to hub, where an older hub would silently drop it — so it is gated on
**version 6** instead, and both ends check the negotiated version before using any of the three opcodes.
It is deliberately a *post*-registration exchange: the client has to send its registration frame before
it knows what version was negotiated, so a token spliced into that frame would have been misparsed as
credential bytes by any hub that predates the feature. A hub drops a `GroupMessage` from a client that is not a member of the target
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

When a hub and its clients all run on the same host — a sidecar process, or a multi-process
desktop or daemon layout — `UnixSocketTransport`/`UnixSocketTransportListener` (Linux and macOS)
and `NamedPipeTransport`/`NamedPipeTransportListener` (Windows) avoid the network stack overhead
and open port a loopback TCP listener would otherwise cost. Both share the same length-prefixed
framing as TCP:

```csharp
// Hub (Linux/macOS)
var listener = new UnixSocketTransportListener("/tmp/meshworx.sock");
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

// Client (Linux/macOS)
await using var client = new MeshClient(clientLogger);
await client.ConnectAsync(await UnixSocketTransport.ConnectAsync("/tmp/meshworx.sock"), "Alice");
```

```csharp
// Hub (Windows)
var listener = new NamedPipeTransportListener("meshworx");
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

// Client (Windows)
await using var client = new MeshClient(clientLogger);
await client.ConnectAsync(await NamedPipeTransport.ConnectAsync("meshworx"), "Alice");
```

`UnixSocketTransportListener` deletes a stale socket file left behind by a previous instance
before binding (and its own file on clean disposal), so restarting a crashed hub does not fail
with "address already in use". `NamedPipeTransportListener.StartAsync` and
`NamedPipeTransport.ConnectAsync` throw `PlatformNotSupportedException` on any operating system
other than the one each is built for — check `OperatingSystem.IsWindows()` before choosing between
the two at run time if the same binary needs to run cross-platform. Neither transport has a TLS
option: access is controlled by filesystem permissions on the socket path or pipe name, not by
encryption, which is appropriate only when every peer that can reach the path is already trusted —
exactly the same trust boundary the operating system itself enforces for local IPC.

`QuicTransport`/`QuicTransportListener` reach a hub over QUIC (`System.Net.Quic`), giving TLS 1.3,
faster connection setup, and head-of-line-blocking resistance versus TCP. Unlike the TCP and
WebSocket transports, TLS is mandatory rather than optional — QUIC requires it at the protocol
level — so both ends always take TLS options:

```csharp
// Hub
var listener = new QuicTransportListener(
    new IPEndPoint(IPAddress.Any, 22003),
    new SslServerAuthenticationOptions { ServerCertificate = hubCertificate });
await using var hub = new MeshHub(logger, listener);
await hub.StartAsync();

// Client
await using var client = new MeshClient(clientLogger);
await client.ConnectAsync(
    await QuicTransport.ConnectAsync("hub.example.com", 22003, new SslClientAuthenticationOptions()),
    "Alice");
```

**Platform requirements.** Both `QuicTransportListener.StartAsync` and `QuicTransport.ConnectAsync`
throw `PlatformNotSupportedException` unless `QuicListener.IsSupported`/`QuicConnection.IsSupported`
are `true` — check either before relying on this transport. That typically means the native
`msquic` library is present and the platform's TLS stack supports TLS 1.3: on Debian/Ubuntu, install
it with `apt install libmsquic`; it is not guaranteed to be preinstalled on every runner or host, so
CI and deployment images should install it explicitly rather than assume it.

Meshworx uses exactly one bidirectional QUIC stream per connection — matching the one-channel-per-
client shape `ITransport` models — rather than the several concurrent streams a single QUIC
connection can multiplex; that capability is what makes QUIC a natural fit for a future large-message
or multi-channel feature, not something this transport itself needs yet. One consequence worth
knowing: a QUIC stream is not visible to the receiving end until data actually arrives on it —
opening one is a purely local operation — so `QuicTransportListener.AcceptAsync` will not return a
connection until the client has sent at least one frame. This is never an issue in the normal
Meshworx flow, since `MeshClient.ConnectAsync` sends the registration frame immediately once handed
a transport, but it matters if you drive `QuicTransport`/`QuicTransportListener` directly: call
`SendAsync` before waiting on the listener's `AcceptAsync`, not after, or the two ends deadlock
waiting on each other.

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

## Large messages

### Message size limits

A single frame is capped at 1 MiB (1,048,576 bytes), and a delivery frame carries the sender's id and,
for a group message, the group name. Those bytes come out of the same budget, so the largest **body**
each send shape can carry differs:

| Send | Largest body |
|---|---|
| `SendAsync` | 1 MiB − 17 bytes |
| `BroadcastAsync` | 1 MiB − 17 bytes |
| `SendToGroupAsync` | 1 MiB − 19 bytes − the group name |

A direct send is size-neutral, because the recipient id in the inbound frame is replaced by the sender
id on the way out. A broadcast or group send has no such field to give back, so its delivery frame is
16 bytes larger than the frame the sender wrote — which is why a fan-out body just inside the client's
own limit can still be too large to deliver. The hub drops such a message and counts it as
`frame-too-large`; the sending client is not told, because there is no wire frame to tell it with.

Use `SendLargeAsync` for anything near these limits.

### Chunking beyond the cap

`SendLargeAsync` sends a payload of any size by splitting it across
as many frames as it needs, with the receiving client reassembling it:

```csharp
await client.SendLargeAsync(recipientId, fortyMegabytes);
```

The recipient raises `MessageReceived` **once**, when the last chunk arrives and the whole message has
been rebuilt. A subscriber never sees a partial message and needs no code to tell a chunked message
from an ordinary one. Headers passed to `SendLargeAsync` ride on every chunk and are delivered once,
with the reassembled whole.

**The hub is not involved.** It routes each chunk as an ordinary opaque frame and never reassembles or
buffers one, so a 40 MiB transfer costs it exactly what the same volume of small messages would.
Reassembly is purely an endpoint concern, carried in the header block the hub already passes through.

Because reassembly means holding memory on behalf of a peer that may never finish, it is bounded on
both axes, configurable on `MeshClient`'s constructor:

| Parameter | Default | Bounds |
|---|---|---|
| `maxReassemblyBytes` | 64 MiB | Total memory held across every part-received message at once. A chunk that would breach it is dropped and its transfer abandoned. |
| `chunkTransferTimeout` | 1 minute | How long an incomplete transfer may sit without a further chunk before it is discarded and its memory reclaimed. |

A transfer that breaches either bound is dropped **without telling the sender** — matching how a full
outbound queue is already handled, and deliberately: a receiver that reported which chunks it refused
would hand an unauthenticated peer a probe for its remaining budget. Pair large sends with an
application-level acknowledgement if you need delivery confirmation.

Chunking requires the header envelope to carry its reassembly metadata, so a connection that negotiated
below protocol version 5 cannot send one. That throws rather than degrading quietly — unlike trace
context, which is an optional extra; here the caller has explicitly asked to send something that cannot
go any other way.

## Compression

Compression is an agreement between two **endpoints**. The hub routes a compressed body exactly as it
routes any other and never learns that a body was compressed at all — the same arrangement the codec
layer already has.

Which algorithms an endpoint understands is a registry rather than a fixed enum, so a consumer can use
an algorithm this library has never heard of without waiting for a release:

```csharp
public sealed class ZstdCompressionStrategy : ICompressionStrategy
{
    public string AlgorithmId => "zstd";

    public ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte> payload) => /* ... */;

    public ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> payload, int maxDecompressedBytes) => /* ... */;
}
```

Register it on the client, alongside the built-ins:

```csharp
var client = new MeshClient(logger);
((CompressionStrategyRegistry)client.CompressionStrategies).Register(new ZstdCompressionStrategy());
```

or through `AddMeshClient`:

```csharp
services.AddMeshClient("Alice", options =>
    options.CompressionStrategies.Register(new ZstdCompressionStrategy()));
```

Every client starts with two built-in strategies, registered under their HTTP content-coding names:

| Algorithm id | Strategy | Notes |
|---|---|---|
| `br` | `BrotliCompressionStrategy` | Registered first, and so preferred: it compresses this library's typical payloads better. |
| `deflate` | `DeflateCompressionStrategy` | Cheaper per byte and understood by essentially everything, which matters when the peer is not a .NET process. |

Registration order is the preference order. Registering under an id that is already present replaces
that strategy **in place**, keeping its position, so a built-in can be swapped for a tuned version of the
same algorithm; `Remove` drops one, and `Clear` leaves an endpoint understanding only what you put in it.

`Decompress` takes an explicit output ceiling and throws `InvalidDataException` once the output exceeds
it. That is deliberately part of the contract rather than left to each implementation: a compressed body
from a peer is attacker-controlled, and a few kilobytes on the wire can otherwise expand to gigabytes in
memory. Resolving an algorithm id nothing is registered for throws
`UnknownCompressionAlgorithmException`, naming the id and listing what *is* registered.

### Compressing a message

Compression is opt-in per send, through `DeliveryOptions`:

```csharp
// The best algorithm this client has registered — Brotli, by default.
await client.SendAsync(recipientId, telemetryBatch, DeliveryOptions.Compressed());

// A specific one.
await client.SendAsync(recipientId, telemetryBatch, DeliveryOptions.Compressed("zstd"));

// Combined with anything else DeliveryOptions carries.
await client.SendAsync(
    recipientId,
    telemetryBatch,
    DeliveryOptions.RequireAck(TimeSpan.FromSeconds(5)).WithCompression());
```

The recipient decompresses before raising `MessageReceived`, so a subscriber sees the bytes that were
sent and needs no code to tell a compressed message from an ordinary one — the compression headers are
stripped along with the compression.

**Opting in can never make a message larger.** A body under 256 bytes is sent as-is without an attempt,
and a body whose compressed form is not actually smaller is sent uncompressed too. Neither case is an
error, and neither changes what the recipient sees.

The two halves of "which algorithm" behave differently on purpose:

| Request | Not available locally |
|---|---|
| `Compressed()` — best available | Sends uncompressed. A preference, not a requirement. |
| `Compressed("zstd")` — named | Throws `UnknownCompressionAlgorithmException` before anything is sent. |

On the receiving side, a message compressed with an algorithm this client has no strategy for is
**dropped and logged**, and the connection carries on — an algorithm mismatch is a configuration
difference between two endpoints, not a protocol violation. Advertising each endpoint's registered
algorithms so a sender can avoid the mismatch in the first place is the next change in this milestone.

The sender puts the uncompressed length in a header alongside the algorithm id. The receiver bounds
decompression at exactly that length, refuses a declared length past `maxDecompressedBytes` (64 MiB by
default) before decompressing anything, and drops a body that restores to a different length than was
declared — which is what makes a truncated body an error rather than a silently short message.

Compression applies to the direct `SendAsync` overload that takes `DeliveryOptions`. Group, topic and
broadcast sends, and `SendLargeAsync`'s chunked transfers, are not compressed; chunked compression is a
later change in this milestone.

## Typed messages

The core library is byte-oriented and stays that way — the hub routes opaque bodies and takes no
serialization dependency. The optional `AdamSalisbury.Meshworx.Serialization` package adds a codec
layer on top of it, so an application can exchange typed values instead of hand-rolling
`Encoding.UTF8.GetBytes`/`GetString` at every call site.

```csharp
// Send a value — serialized, and tagged with the codec's content type.
await client.SendAsync(recipientId, new Order(42, "Widget"), JsonMessageSerializer.Default);

// Receive one.
client.MessageReceived += (_, e) =>
{
    if (e.TryDeserialize(JsonMessageSerializer.Default, out Order? order))
    {
        Handle(order);
    }
};
```

Typed overloads are provided for `SendAsync`, `SendToGroupAsync`, `RequestAsync` and `ReplyAsync`,
each a thin wrapper over the byte-oriented method of the same name. `BroadcastAsync` has no typed
overload: it takes no headers, so a broadcast body cannot carry the content type that makes it
decodable at the other end — serialize explicitly and broadcast the bytes if you need this.

Every typed send writes the codec's `IMessageSerializer.ContentType` to the `mesh.content-type`
header, and `TryDeserialize` checks it before decoding. That check is what makes a connection
carrying more than one kind of traffic safe: bytes alone carry no format, so a codec asked to decode
another codec's output would otherwise either throw or — worse — succeed and produce a plausible but
wrong value. A message with no content-type header at all is accepted, since its absence means a
byte-oriented sender and the caller reaching for a codec is asserting it knows the format.

`JsonMessageSerializer` is the out-of-the-box implementation. Swap in a denser codec — MessagePack,
Protobuf, anything — by implementing `IMessageSerializer`; nothing in the core library or the hub
changes, and the same send and receive extensions work unaltered.

## Typed contracts

`AdamSalisbury.Meshworx.Contracts` takes the codec layer a step further: declare an interface, and a
source generator emits a client proxy and a dispatcher for it at compile time.

```csharp
[MeshContract]
public interface IOrderService
{
    Task SubmitAsync(int orderId, string productCode, CancellationToken cancellationToken = default);

    Task<int> GetTotalAsync(int orderId, CancellationToken cancellationToken = default);
}
```

That generates `OrderServiceProxy` (implementing `IOrderService`) and `OrderServiceDispatcher`:

```csharp
// Calling side — an ordinary interface call that happens to cross the network.
IOrderService orders = new OrderServiceProxy(client, JsonMessageSerializer.Default, recipientId);
await orders.SubmitAsync(42, "WIDGET");

// Receiving side. The client is a constructor argument because this contract declares a method
// returning a value, and that method's reply has to go back somewhere.
var dispatcher = new OrderServiceDispatcher(new OrderService(), JsonMessageSerializer.Default, client);
client.MessageReceived += async (_, e) => await dispatcher.TryDispatchAsync(e);
```

A contract whose methods all return `Task` is entirely one-way, so its dispatcher takes no client.

**No reflection at run time.** The generator has every signature at compile time, so argument packing
is a generated record and method selection a generated switch. A mistyped call is a build error, not a
message that silently fails to dispatch.

A method returning `Task` is a one-way send; one returning `Task<T>` goes out as a request and its
reply is decoded back into `T`, correlated by the core library's own request/response helper rather
than by a scheme the generator invents. Both carry the codec's content type, like every other typed
send.

`TryDispatchAsync` returns `false` for a message that is not this contract's, so a connection carrying
several contracts can offer each message to every dispatcher in turn. What makes that safe is the wire
identity: `mesh.contract.method` carries the **fully qualified** method name —
`Acme.Orders.IOrderService.SubmitAsync` — so two contracts that happen to declare a method of the same
name cannot claim each other's messages. It follows that renaming a contract's namespace or interface
changes what goes on the wire, and both endpoints must be rebuilt together.

`TryDispatchAsync` is meant to be called from a `MessageReceived` handler, so it is total over its
input: a body this contract's codec cannot decode, and a request that owes a reply but arrived as an
ordinary send, are both declined rather than thrown into the receive loop — and the second is declined
*before* the implementation runs, so a handler's side effects are never committed by a call that cannot
be answered.

Contract methods must return `Task` or `Task<T>` and may take a trailing `CancellationToken` (which is
not serialized — it travels no further than the calling process). Anything the generator cannot express
is a build error naming the member and the reason, rather than a member silently skipped:

| ID | Reported for |
|---|---|
| `MESH001` | A return type that is not `Task` or `Task<T>` |
| `MESH002` | A `ref`, `out` or `in` parameter — meaningless across a network boundary |
| `MESH003` | A generic method — an open type parameter has no shape to serialize |
| `MESH004` | An overloaded method name — a method is identified on the wire by name alone |
| `MESH005` | A property or event — neither is expressible as a one-way message |
| `MESH006` | A `CancellationToken` that is not the last parameter |
| `MESH007` | A generic contract interface — an open type parameter has no single identity to name |
| `MESH008` | A contract interface with base interfaces — a contract must be self-contained |
| `MESH009` | A nested contract interface — a contract must be declared directly in a namespace |
| `MESH010` | The generator itself failed — reported against the contract that caused it |

A static interface member is not part of the wire contract and is skipped rather than diagnosed.

The hub knows nothing about any of this. A contract call is an ordinary message with an ordinary
header block naming the method, routed exactly as every other message is.

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
| `meshworx.hub.messages.dropped` | counter | `reason`: `unknown-recipient`, `queue-full`, `expired`, `offline-queue-full`, `frame-too-large` | Messages the hub could not deliver: a direct send to a Guid nobody is registered under, a write to a recipient's outbound queue that was already full, a frame whose time-to-live had lapsed by the time it was dequeued for sending, a message the configured offline store refused to hold, or a fan-out whose delivery frame would exceed the 1 MiB frame cap (see [Message size limits](#message-size-limits)). |
| `meshworx.hub.messages.offline_queued` | counter | — | Messages held in the offline store for a disconnected client instead of being dropped. They are counted as `routed` (`direction=direct`) later, when the client returns and they are queued for it. |
| `meshworx.hub.outbound_queue.depth` | observable gauge | — | The total number of frames currently queued for delivery, summed across every connected client's outbound queue. A single aggregate rather than one series per client, since tagging by client id would give the gauge unbounded cardinality over the hub's lifetime. |
| `meshworx.client.reconnects` | counter | — | The number of times a `MeshClientReconnector` has re-established a connection after an unexpected drop. Does not count the initial connection `StartAsync` makes. |

No protocol or payload change accompanies any of this — the instruments only observe routing and
connection lifecycle events that already happen.

### Distributed tracing

Clients propagate [W3C Trace Context](https://www.w3.org/TR/trace-context/) so a logical operation
stays traceable as it hops client → hub → client. Spans come from an `ActivitySource` named
`AdamSalisbury.Meshworx`:

```csharp
using OpenTelemetry.Trace;

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddSource("AdamSalisbury.Meshworx"));
```

That registration *is* the opt-in. Until something listens to the source, `StartActivity` returns
`null`, no `Activity` is allocated, no trace headers are written, and every frame is byte-for-byte
what it was before tracing existed. There is no flag to set and nothing to switch on in the hub.

| Span | Kind | Raised by |
|---|---|---|
| `Meshworx.Send` | `Producer` | The sending client, around handing a message to the transport. Tagged with `meshworx.recipient_id` or `meshworx.group_name`, and `meshworx.message_size`. |
| `Meshworx.Receive` | `Consumer` | The receiving client, around delivery to the application — so the span covers the handler's own work, which is usually what the trace is being read to explain. Tagged with `meshworx.sender_id`, and `meshworx.group_name` for a group message. |

Context travels in the header envelope under the standard `traceparent` and `tracestate` keys — the
W3C names verbatim, not `mesh.`-prefixed ones, so a peer bridging Meshworx to HTTP, gRPC or a broker
finds the names it already knows. Both are reserved: setting either by hand throws, as with every
other header a built-in helper writes.

**The hub does not participate in the trace.** It passes the header block through unchanged, as it
does for every header it has no behaviour for, so context survives the routing hop without the hub
reading it. A send made inside your own `Activity` joins that trace whether or not anything is
listening to this library's source specifically.

Two deliberate degradations, both chosen so observability can never break delivery:

- A connection that negotiated below protocol version 5 cannot carry a header block at all. Trace
  context is dropped and the message goes out exactly as it always did, rather than the send starting
  to throw the moment a listener is attached.
- A malformed `traceparent` from a peer costs the causal link, not the delivery. The message is still
  raised, under a span that starts a new trace.

## Building and testing

```sh
dotnet build src/AdamSalisbury.Meshworx/AdamSalisbury.Meshworx.csproj
dotnet test  src/Tests/AdamSalisbury.Meshworx.UnitTests/AdamSalisbury.Meshworx.UnitTests.csproj
```

## Licence

See [LICENSE](LICENSE).
