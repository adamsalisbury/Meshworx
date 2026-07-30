using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using AdamSalisbury.Meshworx.Diagnostics;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.RateLimiting;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Framing;
using Microsoft.Extensions.Logging;

namespace AdamSalisbury.Meshworx;

public sealed class MeshHub : IMeshHub, IAsyncDisposable
{
    private static readonly TimeSpan DefaultRegistrationTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan DefaultGroupAuthorisationTimeout = TimeSpan.FromSeconds(10);
    private const int DefaultMaxConcurrentAuthentications = 64;

    // A hub with no configured ceiling used to admit clients without limit. That is never a safe
    // default: an unauthenticated peer could open connections until the process ran out of sockets,
    // threads or memory. 1000 is the figure the README has always used as its worked example, so it
    // is what integrators already expect a "sensible" cap to look like. Pass int.MaxValue explicitly
    // to opt back into the old unlimited behaviour.
    private const int DefaultMaxClients = 1000;

    // Idle eviction used to be off unless an integrator opted in, so a registered connection that
    // never sent another frame held its handler task, socket and outbound queue forever. 30 seconds
    // matches the README's own worked example for heartbeatInterval, so a hub that never touches this
    // parameter now gets exactly the behaviour the documentation already described as the sensible
    // choice. Pass Timeout.InfiniteTimeSpan explicitly to disable idle eviction entirely.
    private static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromSeconds(30);

    // An unauthenticated peer that can open sockets can open many of them from the same source, each
    // claiming a handler task, a socket and an outbound queue before ever completing registration.
    // This bounds how many connections the accept loop admits from a single remote address at once,
    // independently of maxClients — which only limits registered clients, not connections still mid
    // handshake. Pass int.MaxValue explicitly to opt out.
    private const int DefaultMaxConnectionsPerRemoteEndpoint = 100;

    // A registered client is trusted to have completed the handshake, but "registered" is not
    // "well-behaved": nothing before this stops it streaming frames as fast as the transport accepts
    // them. 200 messages/second is generous for anything a real client legitimately sends — the
    // library has no built-in notion of a message rate higher than a person or a typical background
    // sync could produce — while still giving a hot loop a firm ceiling. Pass int.MaxValue to opt out.
    private const int DefaultMaxInboundMessagesPerSecond = 200;

    // Every inbound frame is also charged against a byte-volume budget, independently of how many
    // frames it took to spend it: a client sending few but maximum-size (1 MiB) frames is exactly as
    // capable of saturating hub egress as one sending many small ones. 4 MiB/second allows four
    // full-size frames a second at the steady state, which comfortably covers real traffic while
    // still bounding it. Pass int.MaxValue to opt out.
    private const int DefaultMaxInboundBytesPerSecond = 4 * 1024 * 1024;

    // BroadcastMessage and GroupMessage are not like other frames: one inbound frame becomes a send to
    // every recipient, so the cost of admitting it scales with the size of the hub's client population
    // rather than staying fixed. This budget bounds how often a client may trigger one of the two at
    // all, on top of the two general budgets above. 20/second is a fraction of the general message
    // budget, reflecting that fan-out traffic is inherently less frequent in legitimate use than
    // one-to-one messaging. Pass int.MaxValue to opt out.
    private const int DefaultMaxFanOutMessagesPerSecond = 20;

    // A frequency budget alone does not bound the amplification that results from it: at the frequency
    // above, a hub with a population of 1,000 still sees up to 20,000 deliveries a second from one
    // client, and that grows without limit as the population — or the frequency budget itself — grows.
    // This charges by the actual number of recipients a fan-out reaches rather than by the frame, so
    // the hub's worst-case fan-out cost stays bounded by a figure that does not move with either of
    // those. 20,000 matches the worst case the two defaults above already implied, so a hub built with
    // every default unchanged sees no new limit in practice. Pass int.MaxValue to opt out.
    private const int DefaultMaxFanOutDeliveriesPerSecond = 20_000;

    // How long RouteMessageWithHeaders awaits free capacity on a saturated recipient queue before giving
    // up and falling back to the drop-on-full behaviour, for a sender that opted into
    // DeliveryOptions.AwaitCapacity. Bounds the worst case so a recipient that never drains cannot stall
    // the sending client's receive loop indefinitely; 30 seconds mirrors the registration and group
    // authorisation timeouts' own default.
    private static readonly TimeSpan DefaultBackpressureAwaitTimeout = TimeSpan.FromSeconds(30);

    // How long the hub waits on the configured IOfflineStore before giving up on a single call. Every
    // other integrator seam on this hub is time-bounded for the same reason: the store is called from a
    // sender's receive loop and from a registration, so a durable one that hangs would park a live
    // connection — and a parked connection looks idle to the heartbeat monitor, which would then evict
    // the very client the store exists to serve. 10 seconds matches the registration and group
    // authorisation timeouts.
    private static readonly TimeSpan DefaultOfflineStoreTimeout = TimeSpan.FromSeconds(10);

    // Applied only after a failed AcceptAsync, never after a successful one, so a healthy burst of
    // incoming connections is never throttled. Matches TcpTransportListener's own AcceptRetryDelay: a
    // persistent accept failure — descriptor exhaustion, most notably — must not spin this loop hot,
    // whatever ITransportListener implementation is plugged in. A per-listener implementation may already
    // pace its own retries (the TLS handshake pump does), but ITransportListener is a public extension
    // point, so this loop cannot assume every implementation does.
    private static readonly TimeSpan AcceptRetryDelay = TimeSpan.FromMilliseconds(50);

    private readonly ILogger<MeshHub> _logger;
    private readonly ITransportListener _listener;
    private readonly TimeSpan _registrationTimeout;
    private readonly TimeSpan? _heartbeatInterval;
    private readonly int _maxMissedHeartbeats;
    private readonly ClientAuthenticator? _authenticator;
    private readonly GroupAuthoriser? _groupAuthoriser;
    private readonly TimeSpan _groupAuthorisationTimeout;
    private readonly int _maxConnectionsPerRemoteEndpoint;
    private readonly int _maxInboundMessagesPerSecond;
    private readonly int _maxInboundBytesPerSecond;
    private readonly int _maxFanOutMessagesPerSecond;
    private readonly int _maxFanOutDeliveriesPerSecond;

    // Shared by every connection's receive loop, unlike a client's own ClientRateLimiter — see
    // SharedLogThrottle's own remarks for why a rate-limit warning needs a hub-wide gate rather than
    // one gate per connection.
    private readonly SharedLogThrottle _rateLimitLogThrottle = new();

    private readonly bool _notifyOnQueueSaturation;
    private readonly TimeSpan _backpressureAwaitTimeout;

    // Store-and-forward is off unless an integrator supplies a store; null is the whole "disabled"
    // switch, and every path that touches the feature is behind a null check on this field, so a hub
    // without one does exactly what it did before the feature existed.
    private readonly IOfflineStore? _offlineStore;
    private readonly TimeSpan _offlineStoreTimeout;

    // The name that last held each id, retained past disconnect so a sender addressing the id it looked
    // up before the recipient went away can still have its message stored under that recipient's name.
    // Only populated when an offline store is configured. Two maps rather than one so the stale entry
    // can be dropped in constant time when the name comes back — the reverse map is what turns "this
    // name has re-registered" into the one id that needs forgetting.
    private readonly ConcurrentDictionary<Guid, string> _offlineNamesById = new();
    private readonly ConcurrentDictionary<string, Guid> _offlineIdsByName = new();

    // How long a dormant session may be reclaimed for, or null when session resumption is switched off —
    // in which case no token is ever issued, the table below stays empty, and a RegistrationComplete
    // reply is byte-for-byte what it was before the feature existed.
    private readonly TimeSpan? _sessionResumptionWindow;

    // Resumable sessions, keyed by the hex SHA-256 hash of the token that reclaims them rather than by
    // the token itself: the table is then not a bag of live bearer credentials, so a hub's memory — a
    // dump, a debugger, a log of the wrong object — does not hand out identities.
    private readonly ConcurrentDictionary<string, ResumableSession> _sessions = new(StringComparer.Ordinal);

    // How many connections are currently open from each remote address, counting from acceptance
    // until the handler that owns that connection finishes — including the pre-registration window,
    // since that is exactly the window an unauthenticated flood exploits. Only addresses the accept
    // loop can actually observe: a transport that does not report one (see
    // <see cref="IRemoteEndPointTransport"/>) is never counted here and so is never capped by it.
    private readonly ConcurrentDictionary<IPAddress, int> _connectionsByRemoteAddress = new();

    // Caps how many integrator authenticator callbacks may run concurrently. Null when no authenticator
    // is configured, since there is then no pre-authentication work to bound.
    private readonly SemaphoreSlim? _authenticationSlots;

    private readonly ConcurrentDictionary<Guid, ClientConnection> _clients = new();
    private readonly ConcurrentDictionary<string, Guid> _clientNames = new();
    private readonly ConcurrentDictionary<Task, byte> _handlerTasks = new();

    // One Meter per hub instance, disposed with it — rather than a single static Meter shared by every
    // hub in the process — so a hub's instruments stop reporting the moment it is torn down instead of
    // going on publishing zeros (or nothing at all) for a resource that no longer exists. Multiple hubs
    // in one process still publish under the same meter name, which is exactly what an exporter expects:
    // OpenTelemetry aggregates every Meter with a matching name regardless of how many instances created
    // one.
    private readonly Meter _meter;
    private readonly UpDownCounter<int> _connectedClientsCounter;
    private readonly Counter<long> _messagesRoutedCounter;
    private readonly Counter<long> _bytesRoutedCounter;
    private readonly Counter<long> _messagesDroppedCounter;
    private readonly Counter<long> _messagesOfflineQueuedCounter;
    private readonly ObservableGauge<int> _outboundQueueDepthGauge;

    // Single-tag KeyValuePairs, reused across every call site rather than built inline. Counter<T>.Add has
    // an allocation-free overload that takes exactly one KeyValuePair<string, object?>, which every
    // instrument below uses with exactly one tag — allocating a fresh pair (or array) per routed or
    // dropped message would put a heap allocation on the hub's hottest paths for no benefit.
    private static readonly KeyValuePair<string, object?> DirectDirectionTag = new("direction", "direct");
    private static readonly KeyValuePair<string, object?> BroadcastDirectionTag = new("direction", "broadcast");
    private static readonly KeyValuePair<string, object?> GroupDirectionTag = new("direction", "group");
    private static readonly KeyValuePair<string, object?> UnknownRecipientDropTag = new("reason", "unknown-recipient");
    private static readonly KeyValuePair<string, object?> QueueFullDropTag = new("reason", "queue-full");
    private static readonly KeyValuePair<string, object?> ExpiredDropTag = new("reason", "expired");
    private static readonly KeyValuePair<string, object?> OfflineQueueFullDropTag = new("reason", "offline-queue-full");
    private static readonly KeyValuePair<string, object?> FrameTooLargeDropTag = new("reason", "frame-too-large");

    // How many client slots are currently claimed. This, rather than _clients.Count, is what maxClients
    // is enforced against: a slot is claimed by a single atomic operation before the client is put into
    // the registries and given back when its handler ends. Testing the client count and adding
    // afterwards let concurrent registrations all read the same count and all admit, so the cap could be
    // overshot by as many clients as happened to be registering at once. Every claim is owned by exactly
    // one client handler and released in that handler's finally, so a shutdown that clears the registries
    // deliberately does not reset this — the handlers still running own the outstanding claims and give
    // them back themselves. A hub stopped while a handler is still unwinding therefore reports no
    // connected clients while briefly still holding its slot, which is the safe way round.
    private int _reservedClientSlots;

    // Each group is guarded by its own lock, so traffic to distinct groups routes in parallel and
    // only mutation of the same group contends. A group is created on first join and removed once
    // empty. Each connection also tracks the groups it joined so it can be removed from all of them
    // on disconnect; that set is only ever touched by the connection's own receive loop (and its
    // teardown, which runs after the loop ends), so it needs no additional lock.
    private readonly ConcurrentDictionary<string, Group> _groups = new(StringComparer.Ordinal);

    // Guards every lifecycle field below. Starting, stopping and disposing can each be called from a
    // different thread, so each of them takes the state it needs in one critical section and then works
    // only from locals: reading a field twice is what let a concurrent stop null the token source
    // between a check and the dereference that followed it. Nothing that blocks or awaits is done while
    // holding it.
    private readonly Lock _stateLock = new();

    private CancellationTokenSource? _cts;
    private Task? _acceptLoopTask;

    // The call to StopAsync that finds the hub running takes ownership of the shutdown and stores it
    // here; every concurrent call awaits that same task rather than running a second shutdown over state
    // the first has already taken. Cleared once the shutdown finishes, so the hub can be started again.
    // Read and written only under the lock.
    private Task? _stopTask;

    // The first call to DisposeAsync stores its teardown here; every later or concurrent call awaits
    // that same task. Read and written only under the lock.
    private Task? _disposeTask;

    // Set while a start is between claiming the hub and publishing its accept loop. It holds the running
    // slot across the listener start without exposing a token source that a concurrent stop could take
    // ownership of before there is anything to stop.
    private bool _starting;

    // Set the instant disposal begins, before any teardown starts, so a start racing a disposal cannot
    // bring the hub back up on a listener that is being torn down.
    private bool _disposed;

    /// <param name="logger">The logger used to record hub activity.</param>
    /// <param name="listener">The transport listener that accepts incoming client connections.</param>
    /// <param name="registrationTimeout">
    /// The maximum time a newly accepted connection is given to complete registration before it is
    /// dropped. Guards against connections that accept but never register. Defaults to 10 seconds.
    /// </param>
    /// <param name="maxClients">
    /// The maximum number of clients that may be registered at once. Further registration attempts are
    /// refused with <see cref="RegistrationErrorCode.HubAtCapacity"/>. Defaults to 1000. Pass
    /// <see cref="int.MaxValue"/> to admit an unlimited number of clients.
    /// </param>
    /// <param name="heartbeatInterval">
    /// How long a registered client may be idle before the hub probes it with a ping — unless
    /// <paramref name="maxMissedHeartbeats"/> is 1, in which case the first idle interval evicts the
    /// client rather than probing it. A client that fails to send any frame across
    /// <paramref name="maxMissedHeartbeats"/> consecutive intervals is evicted, detecting half-open
    /// connections. Defaults to 30 seconds, so idle eviction is on unless a hub opts out. Pass
    /// <see cref="Timeout.InfiniteTimeSpan"/> to disable idle eviction entirely and let a registered
    /// client sit silent indefinitely — only do this if something else bounds how long a connection
    /// may go unused.
    /// </param>
    /// <param name="maxMissedHeartbeats">
    /// The number of consecutive idle intervals that causes a client to be evicted. A client that sends
    /// no frame across this many consecutive intervals is dropped; any frame it sends resets the count.
    /// The hub pings the client on each idle interval before the last, so it is probed
    /// <paramref name="maxMissedHeartbeats"/> minus one times before eviction — a value of 1 evicts on
    /// the first idle interval without probing at all. Only used when
    /// <paramref name="heartbeatInterval"/> is set. Defaults to 2.
    /// </param>
    /// <param name="authenticator">
    /// An optional callback invoked for each registration to decide whether the client may join, given
    /// its name and the opaque credential it supplied. Returning <see langword="false"/> refuses the
    /// client with <see cref="RegistrationErrorCode.AuthenticationFailed"/>. When <see langword="null"/>
    /// (the default) the hub performs no authentication and admits any peer that completes the
    /// handshake — in that case the hub must only be exposed to a trusted network.
    /// </param>
    /// <param name="maxConcurrentAuthentications">
    /// The maximum number of <paramref name="authenticator"/> callbacks that may run at once. The
    /// authenticator runs on unauthenticated input, so this bounds the work an unauthenticated peer can
    /// cause by connecting. A connection that cannot obtain a slot within
    /// <paramref name="registrationTimeout"/> is refused with
    /// <see cref="RegistrationErrorCode.AuthenticationFailed"/>. Defaults to 64. Ignored when
    /// <paramref name="authenticator"/> is <see langword="null"/>.
    /// </param>
    /// <param name="groupAuthoriser">
    /// An optional callback invoked for each group join to decide whether the client may become a member,
    /// given its registered identity and the group name. Returning <see langword="false"/> refuses the
    /// join and tells the client so. When <see langword="null"/> (the default) the hub authorises no
    /// joins and any client may join any group — groups are then a routing convenience, not an isolation
    /// boundary. Sending to a group always requires membership of it, with or without this callback.
    /// </param>
    /// <param name="groupAuthorisationTimeout">
    /// The maximum time a <paramref name="groupAuthoriser"/> callback is given to decide before the join
    /// is refused. Bounds a hanging integrator callback, which would otherwise stall the calling client's
    /// receive loop — and hold its client slot — indefinitely. Defaults to 10 seconds. Ignored when
    /// <paramref name="groupAuthoriser"/> is <see langword="null"/>.
    /// </param>
    /// <param name="maxConnectionsPerRemoteEndpoint">
    /// The maximum number of connections the accept loop admits from a single remote address at once,
    /// counted from acceptance until the connection's handler finishes — including the pre-registration
    /// window, which <paramref name="maxClients"/> does not cover. Refused connections are closed
    /// immediately, before any handshake. Only enforced for a transport that reports its remote address
    /// via <see cref="IRemoteEndPointTransport"/>; a transport that does not is never capped by this. An
    /// IPv6 address is grouped with every other address in its /64 network prefix before the cap is
    /// applied, since a single host is routinely assigned an entire /64 and could otherwise defeat the
    /// cap by using a different address within it for every connection. Defaults to 100. Pass
    /// <see cref="int.MaxValue"/> to opt out.
    /// </param>
    /// <param name="notifyOnQueueSaturation">
    /// Whether a sender is sent a <see cref="Messages.MessageType.QueueSaturated"/> control frame when a
    /// <b>directly addressed</b> message of its own is dropped because the recipient's outbound queue was
    /// full. Broadcast and group sends never produce this frame however this is set, since their dropped
    /// recipient's identity comes from the hub's registries rather than from the sender — see
    /// <see cref="NotifySenderOfQueueSaturation"/>. The drop itself is always observable in-process via
    /// <see cref="QueueSaturated"/> and the existing <c>meshworx.hub.messages.dropped</c> metric, for
    /// every shape of send; this only controls whether the sender is also told over the wire. Defaults to
    /// <see langword="false"/>, so a hub that does not opt in sends nothing beyond what it already sent
    /// before this existed — the fire-and-forget default is unchanged.
    /// </param>
    /// <param name="backpressureAwaitTimeout">
    /// The maximum time <c>RouteMessageWithHeaders</c> awaits free capacity on a saturated recipient
    /// queue for a sender that opted into <see cref="DeliveryOptions.AwaitCapacity"/>, before giving up
    /// and falling back to the drop-on-full behaviour. Bounds how long a recipient that never drains can
    /// stall the sending client's receive loop — and, with it, how long that sender's messages to every
    /// <em>other</em> recipient wait behind it, since one connection's frames are routed in order.
    /// Defaults to 30 seconds. Pass <see cref="Timeout.InfiniteTimeSpan"/> to wait indefinitely; note
    /// that a parked sender is deliberately exempt from idle eviction, so an infinite timeout removes
    /// the only bound on how long a connection can sit parked.
    /// </param>
    /// <param name="offlineStore">
    /// Where to hold direct messages addressed to a client that is not currently connected, so they can
    /// be delivered when that name next registers. Supplying a store is what switches store-and-forward
    /// on; leaving it <see langword="null"/> — the default — keeps the hub's original behaviour of
    /// dropping a message to an unknown recipient outright. Use <see cref="InMemoryOfflineStore"/> for
    /// the bounded, process-local default, or any <see cref="IOfflineStore"/> for a durable one.
    /// </param>
    /// <param name="offlineStoreTimeout">
    /// How long to wait on a single <paramref name="offlineStore"/> call before giving up on it, so a
    /// slow or hanging store cannot park a sender's receive loop or hold up a registration. Defaults to
    /// 10 seconds. Ignored when no store is configured.
    /// </param>
    /// <param name="sessionResumptionWindow">
    /// How long after a client disconnects it may reconnect and reclaim the same id and group
    /// memberships by presenting the resumption token it was issued. <see langword="null"/> — the
    /// default — switches session resumption off entirely: no token is issued, and a
    /// <see cref="MessageType.RegistrationComplete"/> reply carries none. Requires the connection to
    /// negotiate protocol version <see cref="Protocol.SessionResumptionMinVersion"/> or higher.
    /// </param>
    /// <param name="maxInboundMessagesPerSecond">
    /// The maximum number of frames of any type a single registered client may send per second, with a
    /// burst allowance of one second's own budget. A frame beyond the budget is dropped rather than
    /// processed or queued; the sender is not told, matching how a full outbound queue is already
    /// handled elsewhere in the hub. Defaults to 200. Pass <see cref="int.MaxValue"/> to opt out.
    /// </param>
    /// <param name="maxInboundBytesPerSecond">
    /// The maximum number of bytes across every inbound frame a single registered client may send per
    /// second, with a burst allowance of one second's own budget, charged independently of
    /// <paramref name="maxInboundMessagesPerSecond"/> — a client sending fewer but larger frames is
    /// bound by this even when it never approaches the message-count budget. Defaults to 4 MiB. Pass
    /// <see cref="int.MaxValue"/> to opt out.
    /// </param>
    /// <param name="maxFanOutMessagesPerSecond">
    /// The maximum number of broadcast or group-send frames a single registered client may send per
    /// second, with a burst allowance of one second's own budget, enforced in addition to — not instead
    /// of — <paramref name="maxInboundMessagesPerSecond"/> and <paramref name="maxInboundBytesPerSecond"/>.
    /// Broadcast and group sends fan out to every recipient, so one inbound frame costs the hub far more
    /// than an ordinary send, and this keeps that cost from being driven by a single client. This bounds
    /// how often a client may trigger a fan-out; it does not by itself bound how large each one is — see
    /// <paramref name="maxFanOutDeliveriesPerSecond"/> for that. Defaults to 20. Pass
    /// <see cref="int.MaxValue"/> to opt out.
    /// </param>
    /// <param name="maxFanOutDeliveriesPerSecond">
    /// The maximum number of individual deliveries a single registered client's broadcast and
    /// group-send frames may cause per second, with a burst allowance of one second's own budget,
    /// charged by the actual number of recipients each fan-out reaches rather than by the frame, and
    /// enforced in addition to <paramref name="maxFanOutMessagesPerSecond"/>. A frequency budget alone
    /// does not bound the amplification a fan-out causes — at a given frequency, the number of
    /// deliveries it produces grows with the size of the client population, without limit, unless
    /// something else catches it. This is that something else: it keeps the hub's actual worst-case
    /// fan-out cost bounded by a figure that does not move just because the population, or
    /// <paramref name="maxFanOutMessagesPerSecond"/> itself, does. Defaults to 20,000 — the worst case
    /// the other defaults already implied, so a hub built with every default unchanged sees no new
    /// limit in practice. Pass <see cref="int.MaxValue"/> to opt out.
    /// </param>
    public MeshHub(
        ILogger<MeshHub> logger,
        ITransportListener listener,
        TimeSpan? registrationTimeout = null,
        int? maxClients = null,
        TimeSpan? heartbeatInterval = null,
        int maxMissedHeartbeats = 2,
        ClientAuthenticator? authenticator = null,
        int? maxConcurrentAuthentications = null,
        GroupAuthoriser? groupAuthoriser = null,
        TimeSpan? groupAuthorisationTimeout = null,
        int? maxConnectionsPerRemoteEndpoint = null,
        bool notifyOnQueueSaturation = false,
        TimeSpan? backpressureAwaitTimeout = null,
        IOfflineStore? offlineStore = null,
        TimeSpan? offlineStoreTimeout = null,
        TimeSpan? sessionResumptionWindow = null,
        int? maxInboundMessagesPerSecond = null,
        int? maxInboundBytesPerSecond = null,
        int? maxFanOutMessagesPerSecond = null,
        int? maxFanOutDeliveriesPerSecond = null)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(listener);

        if (registrationTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(registrationTimeout), "The registration timeout must be positive.");
        }

        if (maxClients is { } max && max <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxClients), "The maximum client count must be positive.");
        }

        // Timeout.InfiniteTimeSpan is the one negative value accepted here — it is the deliberate,
        // explicit opt-out from idle eviction, not a misconfiguration.
        if (heartbeatInterval is { } interval
            && interval <= TimeSpan.Zero
            && interval != Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(
                nameof(heartbeatInterval),
                "The heartbeat interval must be positive, or Timeout.InfiniteTimeSpan to disable idle "
                + "eviction.");
        }

        if (maxMissedHeartbeats < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxMissedHeartbeats), "The maximum missed heartbeats must be at least one.");
        }

        if (maxConcurrentAuthentications is { } maxAuthentications && maxAuthentications <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConcurrentAuthentications),
                "The maximum concurrent authentication count must be positive.");
        }

        if (groupAuthorisationTimeout is { } authorisationTimeout && authorisationTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(groupAuthorisationTimeout), "The group authorisation timeout must be positive.");
        }

        if (maxConnectionsPerRemoteEndpoint is { } maxPerEndpoint && maxPerEndpoint <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConnectionsPerRemoteEndpoint),
                "The maximum connections per remote endpoint must be positive.");
        }

        // Timeout.InfiniteTimeSpan is the one negative value accepted here — the deliberate, explicit
        // opt-in to waiting forever, mirroring heartbeatInterval's own sentinel.
        if (backpressureAwaitTimeout is { } awaitTimeout
            && awaitTimeout <= TimeSpan.Zero
            && awaitTimeout != Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(
                nameof(backpressureAwaitTimeout),
                "The backpressure await timeout must be positive, or Timeout.InfiniteTimeSpan to wait "
                + "indefinitely.");
        }

        if (offlineStoreTimeout is { } storeTimeout && storeTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offlineStoreTimeout), "The offline store timeout must be positive.");
        }

        if (sessionResumptionWindow is { } resumptionWindow && resumptionWindow <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(sessionResumptionWindow),
                "The session resumption window must be positive, or null to disable resumption.");
        }

        if (maxInboundMessagesPerSecond is { } maxMessages && maxMessages <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxInboundMessagesPerSecond),
                "The maximum inbound messages per second must be positive.");
        }

        if (maxInboundBytesPerSecond is { } maxBytes && maxBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxInboundBytesPerSecond),
                "The maximum inbound bytes per second must be positive.");
        }

        if (maxFanOutMessagesPerSecond is { } maxFanOut && maxFanOut <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxFanOutMessagesPerSecond),
                "The maximum fan-out messages per second must be positive.");
        }

        if (maxFanOutDeliveriesPerSecond is { } maxFanOutDeliveries && maxFanOutDeliveries <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxFanOutDeliveriesPerSecond),
                "The maximum fan-out deliveries per second must be positive.");
        }

        _logger = logger;
        _listener = listener;
        _registrationTimeout = registrationTimeout ?? DefaultRegistrationTimeout;
        MaxClients = maxClients ?? DefaultMaxClients;

        // Null now means "not configured" rather than "disabled": an unconfigured hub still gets idle
        // eviction, at the default interval, exactly like an unconfigured maxClients still gets a cap.
        // Only the explicit Timeout.InfiniteTimeSpan sentinel switches it off.
        _heartbeatInterval = heartbeatInterval == Timeout.InfiniteTimeSpan
            ? null
            : heartbeatInterval ?? DefaultHeartbeatInterval;
        _maxMissedHeartbeats = maxMissedHeartbeats;
        _authenticator = authenticator;
        _groupAuthoriser = groupAuthoriser;
        _groupAuthorisationTimeout = groupAuthorisationTimeout ?? DefaultGroupAuthorisationTimeout;
        _maxConnectionsPerRemoteEndpoint = maxConnectionsPerRemoteEndpoint ?? DefaultMaxConnectionsPerRemoteEndpoint;
        _notifyOnQueueSaturation = notifyOnQueueSaturation;
        _backpressureAwaitTimeout = backpressureAwaitTimeout ?? DefaultBackpressureAwaitTimeout;
        _offlineStore = offlineStore;
        _offlineStoreTimeout = offlineStoreTimeout ?? DefaultOfflineStoreTimeout;
        _sessionResumptionWindow = sessionResumptionWindow;
        _maxInboundMessagesPerSecond = maxInboundMessagesPerSecond ?? DefaultMaxInboundMessagesPerSecond;
        _maxInboundBytesPerSecond = maxInboundBytesPerSecond ?? DefaultMaxInboundBytesPerSecond;
        _maxFanOutMessagesPerSecond = maxFanOutMessagesPerSecond ?? DefaultMaxFanOutMessagesPerSecond;
        _maxFanOutDeliveriesPerSecond = maxFanOutDeliveriesPerSecond ?? DefaultMaxFanOutDeliveriesPerSecond;

        if (authenticator is not null)
        {
            int slots = maxConcurrentAuthentications ?? DefaultMaxConcurrentAuthentications;
            _authenticationSlots = new SemaphoreSlim(slots, slots);
        }

        // At a maximum of one missed heartbeat there is no interval left in which a ping could be
        // answered, so the hub evicts on the first idle interval without probing. A client that only
        // receives — and so sends nothing of its own — is then dropped every interval. That is a
        // legitimate choice if clients are expected to send continuously, but it is far more often a
        // misconfiguration, and a silent one, so say so once at construction. Checked against the
        // resolved interval rather than the raw parameter, since idle eviction now runs by default even
        // when heartbeatInterval was never set.
        if (_heartbeatInterval is not null && maxMissedHeartbeats == 1)
        {
            _logger.LogWarning(
                "Heartbeats are enabled with maxMissedHeartbeats set to 1, so clients are evicted on "
                + "their first idle interval and are never probed with a ping. Clients that do not send "
                + "frames of their own will be evicted every interval; use 2 or more to probe liveness.");
        }

        // While the group authoriser runs, the calling client's receive loop is parked and reads nothing,
        // so that client looks idle to the heartbeat monitor however healthy it is — it cannot answer a
        // ping the loop is not there to read. If the authoriser is still deciding when the eviction
        // budget runs out, the monitor cancels the connection and the client is dropped rather than
        // refused, which is not what the fail-closed contract promises and, behind a reconnector, becomes
        // a reconnect loop. Warn rather than throw: the combination is legal, the default authorisation
        // timeout would otherwise refuse construction of a hub with a short heartbeat interval, and a slow
        // authoriser may simply never take that long in practice. Checked against the resolved interval
        // for the same reason as above.
        if (groupAuthoriser is not null
            && _heartbeatInterval is { } configuredInterval
            && _groupAuthorisationTimeout >= configuredInterval * maxMissedHeartbeats)
        {
            _logger.LogWarning(
                "The group authorisation timeout ({Timeout}) is not shorter than the heartbeat eviction "
                + "budget ({Interval} × {MaxMissed}). A client's receive loop is parked while its join is "
                + "being authorised, so a decision that takes this long will have the client evicted "
                + "instead of refused. Set groupAuthorisationTimeout below the budget.",
                _groupAuthorisationTimeout,
                configuredInterval,
                maxMissedHeartbeats);
        }

        // A sender parked awaiting capacity is exempt from idle eviction — it reads nothing while parked
        // and cannot answer a ping, so evicting it would drop a healthy client for backpressure the hub
        // itself applied. That exemption is safe precisely because the park is bounded by the await
        // timeout. Setting the timeout to infinite removes that bound, leaving nothing at all to limit
        // how long a connection sits parked against a recipient that never drains. Warn rather than
        // throw: it is a legal, deliberate opt-in, and a hub whose recipients are known to drain
        // eventually may genuinely want to wait rather than lose the message.
        if (_backpressureAwaitTimeout == Timeout.InfiniteTimeSpan)
        {
            _logger.LogWarning(
                "The backpressure await timeout is infinite, so a send that opted into awaiting capacity "
                + "parks its sender's receive loop until the recipient drains, however long that takes. A "
                + "parked sender is exempt from idle eviction, so nothing else bounds this. Set a finite "
                + "timeout unless every recipient is known to drain.");
        }

        _meter = new Meter(MeshworxMeterName.Value);
        _connectedClientsCounter = _meter.CreateUpDownCounter<int>(
            "meshworx.hub.clients.connected",
            unit: "{client}",
            description: "The number of clients currently registered with the hub.");
        _messagesRoutedCounter = _meter.CreateCounter<long>(
            "meshworx.hub.messages.routed",
            unit: "{message}",
            description: "The number of messages the hub has routed, tagged by direction "
                + "(direct, broadcast or group).");
        _bytesRoutedCounter = _meter.CreateCounter<long>(
            "meshworx.hub.bytes.routed",
            unit: "By",
            description: "The number of message payload bytes the hub has routed, tagged by direction "
                + "(direct, broadcast or group).");
        _messagesDroppedCounter = _meter.CreateCounter<long>(
            "meshworx.hub.messages.dropped",
            unit: "{message}",
            description: "The number of messages the hub has dropped, tagged by reason "
                + "(unknown-recipient, queue-full, expired or offline-queue-full).");
        _messagesOfflineQueuedCounter = _meter.CreateCounter<long>(
            "meshworx.hub.messages.offline_queued",
            unit: "{message}",
            description: "The number of messages the hub has held in the offline store for a "
                + "disconnected client rather than dropping them.");
        _outboundQueueDepthGauge = _meter.CreateObservableGauge(
            "meshworx.hub.outbound_queue.depth",
            ObserveOutboundQueueDepth,
            unit: "{message}",
            description: "The total number of messages currently queued for delivery, summed across "
                + "every connected client's outbound queue.");
    }

    /// <summary>
    /// Reports the total number of frames currently sitting in every connected client's outbound queue.
    /// </summary>
    /// <remarks>
    /// A single aggregate value rather than one measurement per client: tagging by client id would give
    /// an observable gauge series whose cardinality grows with every client the hub has ever seen
    /// connect, which is exactly the kind of unbounded tag value OpenTelemetry's own guidance warns
    /// against. The aggregate is enough to see a hub-wide backlog forming; per-client depth is available
    /// on <see cref="ClientConnection.OutboundQueue"/> to anything already holding a reference to one.
    /// </remarks>
    private IEnumerable<Measurement<int>> ObserveOutboundQueueDepth()
    {
        int depth = 0;
        foreach (ClientConnection connection in _clients.Values)
        {
            depth += connection.OutboundQueue.Count;
        }

        yield return new Measurement<int>(depth);
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">The hub has been disposed.</exception>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_stateLock)
        {
            // A disposed hub must stay disposed. Without this a start racing a disposal would begin
            // listening on a transport that is being torn down, and the teardown would then leave a
            // running accept loop behind that nothing owns.
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_cts is not null || _stopTask is not null || _starting)
            {
                throw new InvalidOperationException("The hub is already running.");
            }

            // Claim the running slot with a flag rather than by publishing the token source early. A
            // second concurrent start is refused here, but a concurrent stop cannot take a token source
            // whose accept loop does not exist yet — which would abandon a listener that had just been
            // bound, on a hub that then reported itself stopped.
            _starting = true;
        }

        var cts = new CancellationTokenSource();

        try
        {
            await _listener.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Release the claim, so a hub whose listener failed to start is startable again rather than
            // permanently reporting itself as already running. Nothing else has seen this token source,
            // so disposing it here cannot race a stop.
            lock (_stateLock)
            {
                _starting = false;
            }

            cts.Dispose();
            throw;
        }

        lock (_stateLock)
        {
            _starting = false;

            // A disposal may have run to completion while the listener was starting. Its teardown has
            // already disposed the listener, so publishing an accept loop here would run it against a
            // closed listener for as long as it took to notice.
            if (_disposed)
            {
                cts.Dispose();
                throw new ObjectDisposedException(GetType().FullName);
            }

            // Publish the token source and the accept loop together, so no stop can ever see one without
            // the other. Creating the loop holds the lock across the synchronous head of
            // ITransportListener.AcceptAsync; both listeners in this library reach their first await in a
            // lock acquisition and a field read, and only another lifecycle call could contend.
            _cts = cts;
            _acceptLoopTask = AcceptLoopAsync(cts.Token);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Safe to call more than once, and from more than one thread at a time. The call that finds the hub
    /// running takes ownership of its state and performs the shutdown; every concurrent call awaits that
    /// same shutdown, so each of them returns only once the hub has actually stopped, and none of them
    /// notifies the clients a second time or disposes the token source twice. A call made when the hub is
    /// not running returns immediately.
    /// </remarks>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task shutdown;

        lock (_stateLock)
        {
            if (_stopTask is null)
            {
                CancellationTokenSource? cts = _cts;
                if (cts is null)
                {
                    return Task.CompletedTask;
                }

                // Hand the state to the shutdown and clear the fields in the same critical section, so no
                // other caller can see a half-stopped hub or tear the same state down twice.
                Task? acceptLoopTask = _acceptLoopTask;
                _cts = null;
                _acceptLoopTask = null;

                // Started on the thread pool rather than called directly: an async method runs
                // synchronously up to its first await on the calling thread, and StopCoreAsync's first
                // await has no ConfigureAwait to fall back on — a caller stopping the hub from a UI thread
                // would silently strand the shutdown's continuation on that thread's message pump.
                // Task.Run's own cancellation parameter is deliberately CancellationToken.None rather than
                // the caller's cancellationToken: that token bounds only this caller's own wait on the
                // shutdown, per the remarks above — it must never gate whether the shutdown itself runs.
                _stopTask = Task.Run(
                    () => StopCoreAsync(cts, acceptLoopTask, cancellationToken), CancellationToken.None);
            }

            shutdown = _stopTask;
        }

        // A caller that joined someone else's shutdown still honours its own cancellation token. Giving
        // up on the wait does not cancel the shutdown itself, which belongs to the caller that started it.
        return cancellationToken.CanBeCanceled ? shutdown.WaitAsync(cancellationToken) : shutdown;
    }

    /// <summary>
    /// Performs the one and only shutdown of a running hub, working from the state handed to it so that it
    /// cannot race another caller over the fields.
    /// </summary>
    private async Task StopCoreAsync(
        CancellationTokenSource cts, Task? acceptLoopTask, CancellationToken cancellationToken)
    {
        // Started via Task.Run at the call site, which is what keeps nothing of the shutdown running on
        // the caller's stack while it still holds the state lock — nothing here needs to yield first.
        try
        {
            byte[] disconnectPayload = [(byte)MessageType.Disconnect];
            foreach (ClientConnection client in _clients.Values)
            {
                try
                {
                    await client.Transport.SendAsync(disconnectPayload, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is IOException or ObjectDisposedException or OperationCanceledException)
                {
                    // Best-effort disconnect notification; the client may already be gone.
                }
            }
        }
        finally
        {
            // The notification above is best-effort, but the shutdown proper is not: run it even if a
            // transport failed in a way the filter above does not cover. Skipping it would leave the
            // accept loop running and the token source undisposed on a hub that now reports itself
            // stopped, which no later call could put right.
            await ShutDownAsync(cts, acceptLoopTask, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Cancels the hub's work, waits for the accept loop and every client handler to finish, and clears the
    /// registries.
    /// </summary>
    private async Task ShutDownAsync(
        CancellationTokenSource cts, Task? acceptLoopTask, CancellationToken cancellationToken)
    {
        try
        {
            await cts.CancelAsync().ConfigureAwait(false);

            if (acceptLoopTask is not null)
            {
                try
                {
                    await acceptLoopTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected when the cancellation token is triggered during shutdown.
                }
            }

            // Wait for all handler tasks to complete — each handler disposes its own client
            // connection in its finally block, so no separate disposal loop is needed.
            try
            {
                await Task.WhenAll(_handlerTasks.Keys).WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Handler task exceptions are already logged individually via ContinueWith.
                // This catch prevents WhenAll from propagating during shutdown.
            }

            _handlerTasks.Clear();
            _clientNames.Clear();
            _clients.Clear();
            _groups.Clear();

            // Nothing outlives the hub that held it: a session's whole point is reclaiming an identity on
            // *this* hub, and the offline identity map only means anything alongside the store it feeds.
            // Dropping the session table also means a stopped hub is not still holding material that
            // reclaims identities on it.
            _sessions.Clear();
            _offlineNamesById.Clear();
            _offlineIdsByName.Clear();

            cts.Dispose();
        }
        finally
        {
            // Release the shutdown claim whatever happened, so a hub can be started again once it has
            // stopped — and so a shutdown that failed part way leaves the hub stopped rather than wedged
            // as permanently stopping.
            lock (_stateLock)
            {
                _stopTask = null;
            }
        }
    }

    /// <inheritdoc/>
    public event EventHandler<ClientConnectionEventArgs>? ClientConnected;

    /// <inheritdoc/>
    public event EventHandler<ClientConnectionEventArgs>? ClientDisconnected;

    /// <inheritdoc/>
    public event EventHandler<QueueSaturatedEventArgs>? QueueSaturated;

    /// <inheritdoc/>
    public int ConnectedClientCount => _clients.Count;

    /// <inheritdoc/>
    public bool IsRunning
    {
        get
        {
            lock (_stateLock)
            {
                return _cts is not null;
            }
        }
    }

    /// <inheritdoc/>
    public int MaxClients { get; }

    /// <inheritdoc/>
    public int ClaimedClientSlots => Volatile.Read(ref _reservedClientSlots);

    /// <inheritdoc/>
    public bool IsClientRegistered(Guid clientId)
    {
        return _clients.ContainsKey(clientId);
    }

    /// <summary>
    /// Gets the resolved interval the idle/heartbeat monitor uses, or <see langword="null"/> if idle
    /// eviction is disabled.
    /// </summary>
    /// <remarks>
    /// Internal rather than private so a test can assert how the constructor resolved
    /// <c>heartbeatInterval</c> — including the default it falls back to and the
    /// <see cref="Timeout.InfiniteTimeSpan"/> opt-out — without waiting out a real interval to observe
    /// the behaviour indirectly.
    /// </remarks>
    internal TimeSpan? GetHeartbeatIntervalForTesting()
    {
        return _heartbeatInterval;
    }

    /// <summary>
    /// Gets the <see cref="Meter"/> this hub publishes its instruments to.
    /// </summary>
    /// <remarks>
    /// Internal rather than private so a test can filter a <see cref="System.Diagnostics.Metrics.MeterListener"/>
    /// down to exactly this hub's instruments by reference, rather than by meter name alone — several
    /// hubs across a test run can share the name, but never this object.
    /// </remarks>
    internal Meter GetMeterForTesting()
    {
        return _meter;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Safe to call more than once, and from more than one thread at a time. The first call performs the
    /// teardown; every other call awaits that same teardown, so each of them returns only once the hub has
    /// stopped and the listener is closed. A disposed hub cannot be started again.
    /// </remarks>
    public ValueTask DisposeAsync()
    {
        Task disposal;

        lock (_stateLock)
        {
            // Mark the hub disposed before any teardown begins, so a start racing this call is refused
            // rather than racing the listener's disposal.
            _disposed = true;

            // Started on the thread pool rather than called directly: an async method runs synchronously
            // up to its first await on the calling thread, and DisposeCoreAsync's first await has no
            // ConfigureAwait to fall back on — a caller disposing the hub from a UI thread would silently
            // strand the teardown's continuation on that thread's message pump.
            _disposeTask ??= Task.Run(DisposeCoreAsync);
            disposal = _disposeTask;
        }

        return new ValueTask(disposal);
    }

    /// <summary>
    /// Performs the one and only teardown of the hub.
    /// </summary>
    private async Task DisposeCoreAsync()
    {
        // Started from inside the state lock via Task.Run at the call site, and the shutdown it awaits
        // takes that same lock — Task.Run is what keeps this off the disposing thread while the lock is
        // still held, so nothing here needs to yield first.

        await StopAsync().ConfigureAwait(false);
        await _listener.DisposeAsync().ConfigureAwait(false);
        _authenticationSlots?.Dispose();
        _meter.Dispose();
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            ITransport transport;
            try
            {
                transport = await _listener.AcceptAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (Exception ex)
            {
                // A single connection failing to be accepted — a peer resetting the moment it
                // connects, or a transient socket error — must not tear down the accept loop and
                // stop the hub serving every future client. Log and keep listening. This is the
                // background service's top-level loop, so catching broadly here is intentional.
                _logger.LogWarning(ex, "Failed to accept an incoming connection; continuing to listen");

                // A persistent failure — descriptor exhaustion, notably — must not spin this loop hot
                // logging and retrying instantly. Paced here rather than trusted to every
                // ITransportListener implementation, since some (the cleartext TCP and Unix accept paths,
                // historically) accept failures instantly with no pacing of their own.
                try
                {
                    await Task.Delay(AcceptRetryDelay, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                continue;
            }

            // Cap connections per remote address before spending a handler task on one. This is
            // enforced here rather than left to maxClients because maxClients only bounds registered
            // clients: a flood of connections that never complete registration would otherwise sail
            // straight past it. Only checked for a transport that can report where it came from —
            // an in-process transport, for instance, has no meaningful remote address to cap.
            IPAddress? remoteAddress = ExtractRemoteAddress(transport);
            if (remoteAddress is not null && !TryReserveEndpointSlot(remoteAddress))
            {
                _logger.LogWarning(
                    "Refusing a connection from {RemoteAddress}: already at the limit of {Limit} "
                    + "concurrent connections from that address",
                    remoteAddress,
                    _maxConnectionsPerRemoteEndpoint);
                await DisposeRefusedTransportAsync(transport).ConfigureAwait(false);
                continue;
            }

            var handlerTask = HandleClientAsync(transport, remoteAddress, cancellationToken);
            _handlerTasks.TryAdd(handlerTask, 0);
            _ = handlerTask.ContinueWith(
                t =>
                {
                    _handlerTasks.TryRemove(t, out _);
                    if (t.IsFaulted)
                    {
                        _logger.LogError(t.Exception, "Unhandled exception in client handler");
                    }
                },
                TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Reads the remote address a transport reports, if it reports one at all.
    /// </summary>
    /// <remarks>
    /// Only <see cref="IRemoteEndPointTransport"/> implementers are considered, and only when the
    /// endpoint they report is an <see cref="IPEndPoint"/> — the per-remote-endpoint cap is keyed on
    /// the address alone, not the port, since every connection from the same peer arrives on a fresh
    /// ephemeral port and a port-qualified key would never actually catch a repeat source. The address
    /// itself is normalised through <see cref="NormaliseForEndpointCap"/> before use.
    /// </remarks>
    private static IPAddress? ExtractRemoteAddress(ITransport transport)
    {
        return transport is IRemoteEndPointTransport { RemoteEndPoint: IPEndPoint endpoint }
            ? NormaliseForEndpointCap(endpoint.Address)
            : null;
    }

    // The network-prefix length an IPv6 address is masked to before it keys the per-remote-endpoint
    // cap. /64 is the smallest block a single host is routinely assigned by an ISP or cloud provider,
    // so it is the coarsest grouping that still corresponds to "one host" rather than "one address".
    private const int IPv6CapPrefixLength = 64;

    /// <summary>
    /// Reduces an address to the key the per-remote-endpoint cap treats it as coming from.
    /// </summary>
    /// <remarks>
    /// An IPv6 host is routinely handed an entire /64 — or larger — allocation, so keying the cap on
    /// the full address would let a single attacker defeat it by using a different address within that
    /// allocation for every connection: each one is, as far as a full-address key is concerned, a
    /// distinct and never-before-seen source. Masking to the /64 network prefix and zeroing the
    /// interface identifier closes that gap, treating every address in the same /64 as one source for
    /// capping purposes. IPv4 addresses are not routinely multi-assigned to a single host in the same
    /// way, so they are returned unchanged.
    /// </remarks>
    private static IPAddress NormaliseForEndpointCap(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetworkV6)
        {
            return address;
        }

        Span<byte> addressBytes = stackalloc byte[16];
        address.TryWriteBytes(addressBytes, out _);

        // Zero everything past the /64 network prefix (the low 8 bytes, the interface identifier),
        // so addresses that differ only there key to the same masked address.
        addressBytes[(IPv6CapPrefixLength / 8)..].Clear();

        return new IPAddress(addressBytes);
    }

    /// <summary>
    /// Closes a transport that was refused before any handler was created for it.
    /// </summary>
    private async Task DisposeRefusedTransportAsync(ITransport transport)
    {
        try
        {
            await transport.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or OperationCanceledException)
        {
            // Best-effort close of a connection nothing else owns; the peer may already be gone, or
            // shutdown may already be cancelling everything else at the same moment.
        }
    }

    /// <summary>
    /// Claims one of the connection slots available to a remote address if one is free.
    /// </summary>
    /// <remarks>
    /// Compare-and-swap against the dictionary rather than a plain increment, mirroring
    /// <see cref="TryReserveClientSlot"/>: a claim is only ever made from a count that was still under
    /// the cap at the instant of the claim, so a burst of concurrent accepts from the same address
    /// cannot all read the same count and all be admitted.
    /// </remarks>
    private bool TryReserveEndpointSlot(IPAddress address)
    {
        while (true)
        {
            int current = _connectionsByRemoteAddress.GetOrAdd(address, 0);
            if (current >= _maxConnectionsPerRemoteEndpoint)
            {
                return false;
            }

            if (_connectionsByRemoteAddress.TryUpdate(address, current + 1, current))
            {
                return true;
            }
        }
    }

    /// <summary>
    /// Gives back a connection slot claimed by <see cref="TryReserveEndpointSlot"/>. Must be called
    /// exactly once per successful claim.
    /// </summary>
    /// <remarks>
    /// Removes the address from the dictionary once its count reaches zero rather than leaving a
    /// zero-valued entry behind, so a hub that has seen many distinct remote addresses over its
    /// lifetime does not accumulate one dictionary entry per address forever.
    /// </remarks>
    private void ReleaseEndpointSlot(IPAddress address)
    {
        while (true)
        {
            if (!_connectionsByRemoteAddress.TryGetValue(address, out int current))
            {
                return;
            }

            if (current <= 1)
            {
                if (_connectionsByRemoteAddress.TryRemove(new KeyValuePair<IPAddress, int>(address, current)))
                {
                    return;
                }
            }
            else if (_connectionsByRemoteAddress.TryUpdate(address, current - 1, current))
            {
                return;
            }
        }
    }

    private async Task HandleClientAsync(
        ITransport transport, IPAddress? remoteAddress, CancellationToken cancellationToken)
    {
        var clientId = Guid.NewGuid();
        ClientConnection? connection = null;
        Task? sendLoopTask = null;
        Task? heartbeatMonitorTask = null;
        CancellationTokenSource? clientCts = null;
        bool slotReserved = false;

        try
        {
            byte[]? registrationData;
            using (var registrationCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                registrationCts.CancelAfter(_registrationTimeout);
                try
                {
                    registrationData = await transport.ReceiveAsync(registrationCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                    when (registrationCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    _logger.LogDebug(
                        "Client {ClientId} did not complete registration within {Timeout}; dropping connection",
                        clientId,
                        _registrationTimeout);
                    return;
                }
            }

            // Registration frame: [type][versionMin][versionMax][name length (2, big-endian)][name][credential].
            if (registrationData is null
                || registrationData.Length < 3
                || (MessageType)registrationData[0] != MessageType.RegistrationRequest)
            {
                return;
            }

            if (!TryNegotiateProtocolVersion(registrationData[1], registrationData[2], out byte negotiatedVersion))
            {
                byte[] versionError =
                    [(byte)MessageType.Error, (byte)RegistrationErrorCode.UnsupportedProtocolVersion];
                await transport.SendAsync(versionError, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (registrationData.Length < 5)
            {
                // Too short to carry the 2-byte name length; malformed.
                return;
            }

            int registrationNameLength = BinaryPrimitives.ReadUInt16BigEndian(registrationData.AsSpan(3, 2));
            if (registrationNameLength == 0 || registrationData.Length < 5 + registrationNameLength)
            {
                // Malformed frame: the name is empty, or the declared name runs past the payload. An
                // empty name is refused here rather than admitted, because it would otherwise reserve
                // the empty string in the name registry. No in-box client can produce one.
                return;
            }

            string clientName = Encoding.UTF8.GetString(registrationData.AsSpan(5, registrationNameLength));

            if (clientName.Length > Protocol.MaxClientNameLength)
            {
                byte[] nameTooLongError =
                    [(byte)MessageType.Error, (byte)RegistrationErrorCode.ClientNameTooLong];
                await transport.SendAsync(nameTooLongError, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (_authenticator is not null)
            {
                // Refuse an already-full hub before running the integrator's authenticator, so a
                // connection flood cannot drive authentication work — or hold handler tasks on a slow
                // authenticator — once there is nothing left to admit it to. This is only an early-out:
                // the slot itself is claimed below, after authentication returns, so that a peer which
                // never authenticates cannot hold capacity away from one that would.
                if (Volatile.Read(ref _reservedClientSlots) >= MaxClients)
                {
                    await RefuseAtCapacityAsync(transport, clientId, cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (!await AuthenticateAsync(
                        clientId, clientName, registrationData, registrationNameLength, cancellationToken)
                    .ConfigureAwait(false))
                {
                    byte[] authError = [(byte)MessageType.Error, (byte)RegistrationErrorCode.AuthenticationFailed];
                    await transport.SendAsync(authError, cancellationToken).ConfigureAwait(false);
                    return;
                }
            }

            // Claim a slot rather than testing the client count and adding to the registry afterwards.
            // Any number of registrations can read the same count, all pass a test against it, and all
            // then add — so the count-based check made maxClients a soft cap that a burst could overshoot
            // by the size of the burst. The claim is one atomic operation, so exactly one of any number
            // of concurrent registrations takes the last slot and the rest are refused here.
            if (!TryReserveClientSlot())
            {
                await RefuseAtCapacityAsync(transport, clientId, cancellationToken).ConfigureAwait(false);
                return;
            }

            slotReserved = true;

            if (!_clientNames.TryAdd(clientName, clientId))
            {
                byte[] errorPayload = [(byte)MessageType.Error, (byte)RegistrationErrorCode.DuplicateClientName];
                await transport.SendAsync(errorPayload, cancellationToken).ConfigureAwait(false);
                return;
            }

            connection = new ClientConnection(
                clientId,
                clientName,
                transport,
                negotiatedVersion,
                new ClientRateLimiter(
                    _maxInboundMessagesPerSecond,
                    _maxInboundBytesPerSecond,
                    _maxFanOutMessagesPerSecond,
                    _maxFanOutDeliveriesPerSecond));
            _clients.TryAdd(clientId, connection);
            _connectedClientsCounter.Add(1);

            // This name is reachable again, under a new id. Forget the id it was last reachable by, so a
            // peer still holding the old one is told the recipient is unknown rather than having its
            // messages held for a client that is sitting right here connected.
            ForgetOfflineIdentity(clientName);

            // A resumption token, when the feature is on and this connection negotiated high enough for
            // it, rides along on the registration reply — the only frame on which it is ever sent. A
            // connection that gets none produces the identical 18-byte reply every version before 6
            // produced.
            byte[]? sessionToken = IssueSessionToken(connection);
            byte[] responsePayload = sessionToken is null
                ? new byte[18]
                : new byte[18 + 2 + sessionToken.Length];
            responsePayload[0] = (byte)MessageType.RegistrationComplete;
            clientId.TryWriteBytes(responsePayload.AsSpan(1, 16));
            responsePayload[17] = negotiatedVersion;

            if (sessionToken is not null)
            {
                BinaryPrimitives.WriteUInt16BigEndian(responsePayload.AsSpan(18, 2), (ushort)sessionToken.Length);
                sessionToken.CopyTo(responsePayload.AsSpan(20));
            }

            await transport.SendAsync(responsePayload, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation("Client {ClientId} ({ClientName}) connected", clientId, clientName);
            RaiseClientEvent(ClientConnected, clientId, clientName, nameof(ClientConnected));

            clientCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            sendLoopTask = SendLoopAsync(connection, clientCts);

            // A single monitor per connection probes liveness off a PeriodicTimer, so the receive
            // loop below reads against one long-lived token with no per-frame CancellationTokenSource
            // or timer-queue churn. The monitor is only started when heartbeats are configured.
            if (_heartbeatInterval is { } heartbeatInterval)
            {
                heartbeatMonitorTask = MonitorHeartbeatAsync(connection, clientCts, heartbeatInterval, clientId);
            }

            // Drained after the send loop is running, so the held messages are written out as they are
            // queued rather than sitting until the first live frame, and before the receive loop starts,
            // so anything this client is sent from here on queues behind what it missed.
            await DeliverStoredMessagesAsync(connection, clientCts.Token).ConfigureAwait(false);

            while (!clientCts.Token.IsCancellationRequested)
            {
                byte[]? data = await transport.ReceiveAsync(clientCts.Token).ConfigureAwait(false);

                if (data is null)
                {
                    break;
                }

                // Any received frame proves the client is alive; the heartbeat monitor observes this.
                connection.RecordActivity();

                // Charge every frame against the general per-client budgets before it is looked at any
                // further — including an empty one. A zero-length frame carries no opcode and is cheaper
                // for the sender than any real message, so if the check below ran only after the
                // data.Length == 0 guard, a flood of empty frames would cost the hub processing time
                // while spending no budget at all, bypassing the very limit this exists to enforce.
                // Refused here, the frame is dropped silently — the sender is not told, matching how a
                // full outbound queue is already handled below.
                if (!connection.RateLimiter.TryAdmitFrame(data.Length))
                {
                    if (_rateLimitLogThrottle.ShouldLog())
                    {
                        _logger.LogWarning(
                            "Client {ClientId} exceeded its inbound rate limit; frame dropped", clientId);
                    }

                    continue;
                }

                if (data.Length == 0)
                {
                    // Empty frames carry no opcode; ignore rather than indexing data[0].
                    continue;
                }

                var messageType = (MessageType)data[0];

                // Broadcast and group sends fan out to every recipient, so admitting one costs the hub
                // far more than an ordinary send. This second, stricter budget applies only to those
                // message types, on top of the general one just above. GroupMessageWithHeaders belongs
                // here for exactly the same reason as GroupMessage — the header block changes what each
                // recipient's copy carries, not how many recipients there are — so leaving it out would
                // let a client opt out of the fan-out budget simply by attaching an empty header.
                if (messageType is MessageType.BroadcastMessage
                        or MessageType.GroupMessage
                        or MessageType.GroupMessageWithHeaders
                    && !connection.RateLimiter.TryAdmitFanOut())
                {
                    if (_rateLimitLogThrottle.ShouldLog())
                    {
                        _logger.LogWarning(
                            "Client {ClientId} exceeded its fan-out rate limit; broadcast or group message "
                            + "dropped",
                            clientId);
                    }

                    continue;
                }

                if (data.Length >= 17
                    && (MessageType)data[0] == MessageType.SendMessage)
                {
                    var recipientId = new Guid(data.AsSpan(1, 16));
                    ReadOnlyMemory<byte> messageData = data.AsMemory(17);

                    await RouteMessage(clientId, recipientId, messageData, clientCts.Token)
                        .ConfigureAwait(false);
                }
                else if (data.Length >= 19
                    && (MessageType)data[0] == MessageType.SendMessageWithHeaders)
                {
                    var recipientId = new Guid(data.AsSpan(1, 16));
                    int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(17, 2));

                    if (data.Length >= 19 + headerBlockLength)
                    {
                        ReadOnlyMemory<byte> headerBlock = data.AsMemory(19, headerBlockLength);
                        ReadOnlyMemory<byte> body = data.AsMemory(19 + headerBlockLength);

                        await RouteMessageWithHeaders(clientId, recipientId, headerBlock, body, clientCts.Token)
                            .ConfigureAwait(false);
                    }
                }
                else if ((MessageType)data[0] == MessageType.BroadcastMessage)
                {
                    BroadcastMessage(clientId, data.AsMemory(1));
                }
                else if ((MessageType)data[0] == MessageType.JoinGroup)
                {
                    string groupName = Encoding.UTF8.GetString(data.AsSpan(1));
                    // Pass the original name bytes through as well, so a refusal can echo them
                    // rather than re-encode the string they were just decoded from.
                    await JoinGroupAsync(connection, groupName, data.AsMemory(1), clientCts.Token)
                        .ConfigureAwait(false);
                }
                else if ((MessageType)data[0] == MessageType.LeaveGroup)
                {
                    string groupName = Encoding.UTF8.GetString(data.AsSpan(1));
                    LeaveGroup(connection, groupName);
                }
                else if (data.Length >= 3
                    && (MessageType)data[0] == MessageType.GroupMessage)
                {
                    int nameLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(1, 2));
                    if (data.Length >= 3 + nameLength)
                    {
                        string groupName = Encoding.UTF8.GetString(data.AsSpan(3, nameLength));
                        // Pass the original name bytes straight through so SendToGroup does not
                        // re-encode the string it was just decoded from.
                        SendToGroup(
                            clientId,
                            groupName,
                            data.AsMemory(3, nameLength),
                            data.AsMemory(3 + nameLength));
                    }
                }
                else if (data.Length >= 5
                    && (MessageType)data[0] == MessageType.GroupMessageWithHeaders)
                {
                    int nameLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(1, 2));
                    int headerLengthOffset = 3 + nameLength;

                    if (data.Length >= headerLengthOffset + 2)
                    {
                        int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(
                            data.AsSpan(headerLengthOffset, 2));
                        int bodyOffset = headerLengthOffset + 2 + headerBlockLength;

                        if (data.Length >= bodyOffset)
                        {
                            string groupName = Encoding.UTF8.GetString(data.AsSpan(3, nameLength));
                            ReadOnlyMemory<byte> headerBlock = data.AsMemory(headerLengthOffset + 2, headerBlockLength);
                            ReadOnlyMemory<byte> body = data.AsMemory(bodyOffset);

                            SendToGroupWithHeaders(
                                clientId,
                                groupName,
                                data.AsMemory(3, nameLength),
                                headerBlock,
                                body);
                        }
                    }
                }
                else if (data.Length >= 5
                    && (MessageType)data[0] == MessageType.ClientLookupRequest)
                {
                    int correlationId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(1, 4));
                    string lookupName = Encoding.UTF8.GetString(data.AsSpan(5));

                    byte[] lookupResponse;
                    if (_clientNames.TryGetValue(lookupName, out Guid foundId)
                        && _clients.TryGetValue(foundId, out ClientConnection? found))
                    {
                        lookupResponse = new byte[22];
                        lookupResponse[0] = (byte)MessageType.ClientLookupResponse;
                        BinaryPrimitives.WriteInt32BigEndian(lookupResponse.AsSpan(1, 4), correlationId);
                        lookupResponse[5] = 0x01;
                        found.Id.TryWriteBytes(lookupResponse.AsSpan(6));
                    }
                    else
                    {
                        lookupResponse = new byte[6];
                        lookupResponse[0] = (byte)MessageType.ClientLookupResponse;
                        BinaryPrimitives.WriteInt32BigEndian(lookupResponse.AsSpan(1, 4), correlationId);
                        lookupResponse[5] = 0x00;
                    }

                    await transport.SendAsync(lookupResponse, clientCts.Token).ConfigureAwait(false);
                }
                else if (data.Length > 1
                    && (MessageType)data[0] == MessageType.ResumeSession)
                {
                    // Reassigns the loop's own notion of who this client is, so everything downstream —
                    // the sender id on routed frames, and the registry keys this handler's finally
                    // removes — follows the reclaimed identity rather than the discarded one.
                    clientId = await ResumeSessionAsync(connection, data.AsMemory(1), clientCts.Token)
                        .ConfigureAwait(false);
                }
                else if ((MessageType)data[0] == MessageType.Pong)
                {
                    // Liveness reply to a heartbeat ping; RecordActivity above already noted it.
                }
                else if ((MessageType)data[0] == MessageType.Disconnect)
                {
                    _logger.LogDebug("Client {ClientId} sent disconnect", clientId);
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when the cancellation token is triggered during shutdown.
        }
        catch (IOException ex)
        {
            _logger.LogWarning(ex, "Client {ClientId} transport error", clientId);
        }
        finally
        {
            connection?.OutboundQueue.Complete();

            if (clientCts is not null)
            {
                await clientCts.CancelAsync().ConfigureAwait(false);
            }

            if (sendLoopTask is not null)
            {
                try
                {
                    await sendLoopTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected during shutdown.
                }
            }

            if (heartbeatMonitorTask is not null)
            {
                try
                {
                    await heartbeatMonitorTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected once the client's cancellation token is triggered.
                }
            }

            clientCts?.Dispose();

            if (connection is not null)
            {
                // Before RemoveFromAllGroups, which is what empties the set this needs to capture.
                MakeSessionDormant(connection);

                RemoveFromAllGroups(connection);
                _clientNames.TryRemove(connection.Name, out _);

                // connection.Id, not the local clientId: a resume that rebinds the connection and
                // republishes the registries but then throws before ResumeSessionAsync returns leaves
                // clientId holding the discarded fresh id, which is already gone from _clients. Removing
                // by that stale id would leave the reclaimed entry — pointing at the connection about to
                // be disposed below — behind for ever. connection.Id always names the current registry
                // key; ClientDisconnected is raised with it a few lines below for the same reason.
                _clients.TryRemove(connection.Id, out _);
                _connectedClientsCounter.Add(-1);

                // Retained only after the client is out of both registries, so a sender racing this
                // teardown either finds it still connected and routes normally, or finds it gone and
                // resolves the id to a name — never sees it as both at once.
                if (_offlineStore is not null)
                {
                    RetainOfflineIdentity(connection.Id, connection.Name);
                }
            }

            // Give the slot back on every path that claimed one — a client that was admitted and has now
            // disconnected, and one that claimed a slot but was then refused for a duplicate name.
            // Released the moment the client is out of the registries, and deliberately before the
            // transport is disposed: a transport that blocks on close would otherwise hold a slot for as
            // long as it hangs, and it is the hub's registries rather than the socket that maxClients
            // accounts for.
            if (slotReserved)
            {
                ReleaseClientSlot();
            }

            // The per-remote-endpoint slot was claimed in the accept loop, before this handler even
            // started, and covers the connection's whole lifetime including the pre-registration
            // window — so it is released here regardless of whether registration ever completed.
            if (remoteAddress is not null)
            {
                ReleaseEndpointSlot(remoteAddress);
            }

            if (connection is not null)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                _logger.LogInformation("Client {ClientId} disconnected", clientId);
                RaiseClientEvent(ClientDisconnected, connection.Id, connection.Name, nameof(ClientDisconnected));
            }
            else
            {
                await transport.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Claims one of the hub's client slots if one is free.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if a slot was claimed, in which case the caller owns it and must give it
    /// back with <see cref="ReleaseClientSlot"/>; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Internal rather than private so a test can put the hub into the state a concurrent registration
    /// produces — a slot taken, but the client not yet in the registry — which is precisely the window a
    /// check against the observable client count cannot see.
    /// </remarks>
    internal bool TryReserveClientSlot()
    {
        // Compare-and-swap rather than increment-then-test-and-undo. An increment that overshoots the cap
        // is visible to every other registration until it is undone, so a burst that all overshot and all
        // backed out would refuse clients for slots that were never really taken. Here a claim is only
        // ever made from a count that was still under the cap at the instant the claim was made.
        int claimed = Volatile.Read(ref _reservedClientSlots);

        while (claimed < MaxClients)
        {
            int observed = Interlocked.CompareExchange(ref _reservedClientSlots, claimed + 1, claimed);
            if (observed == claimed)
            {
                return true;
            }

            claimed = observed;
        }

        return false;
    }

    /// <summary>
    /// Gives back a client slot claimed by <see cref="TryReserveClientSlot"/>.
    /// </summary>
    /// <remarks>
    /// Internal for the same reason as <see cref="TryReserveClientSlot"/>. Must be called exactly once
    /// per successful claim.
    /// </remarks>
    internal void ReleaseClientSlot()
    {
        Interlocked.Decrement(ref _reservedClientSlots);
    }

    /// <summary>
    /// Writes a raw frame directly onto a registered client's outbound queue, bypassing routing entirely.
    /// </summary>
    /// <remarks>
    /// Internal so a test can drive a client's outbound queue to capacity deterministically. Filling it
    /// by racing a real producer against the real consumer — sending enough messages through the wire
    /// protocol and hoping the consumer is slower — depends on thread-pool scheduling that is not
    /// reliably reproducible from a test.
    /// </remarks>
    /// <returns>
    /// <see langword="true"/> if the frame was queued; <see langword="false"/> if the client is not
    /// registered or its queue was already full.
    /// </returns>
    internal bool TryQueueRawFrameForTesting(Guid clientId, byte[] frame)
    {
        return TryQueueRawFrameForTesting(clientId, frame, MessagePriority.Normal);
    }

    /// <summary>
    /// As <see cref="TryQueueRawFrameForTesting(Guid, byte[])"/>, but onto a specific priority lane, so a
    /// test can drive the shared capacity gate to full from a mix of lanes, or assert lane-drain order
    /// directly.
    /// </summary>
    internal bool TryQueueRawFrameForTesting(Guid clientId, byte[] frame, MessagePriority priority)
    {
        return _clients.TryGetValue(clientId, out ClientConnection? connection)
            && connection.OutboundQueue.TryEnqueue(priority, frame);
    }

    /// <summary>
    /// The capacity of a client's outbound queue, exposed for a test driving one to that capacity via
    /// <see cref="TryQueueRawFrameForTesting(Guid, byte[])"/>.
    /// </summary>
    internal const int OutboundQueueCapacityForTesting = ClientConnection.OutboundQueueCapacity;

    /// <summary>
    /// Tells a registering client the hub is full and records why it was refused.
    /// </summary>
    private async Task RefuseAtCapacityAsync(
        ITransport transport, Guid clientId, CancellationToken cancellationToken)
    {
        byte[] capacityError = [(byte)MessageType.Error, (byte)RegistrationErrorCode.HubAtCapacity];
        await transport.SendAsync(capacityError, cancellationToken).ConfigureAwait(false);
        _logger.LogWarning(
            "Refusing client {ClientId}: hub at capacity ({MaxClients} clients)", clientId, MaxClients);
    }

    /// <summary>
    /// Selects the highest protocol version supported by both this hub and the connecting client.
    /// </summary>
    /// <param name="clientMinVersion">The lowest protocol version the client is willing to speak.</param>
    /// <param name="clientMaxVersion">The highest protocol version the client is willing to speak.</param>
    /// <param name="negotiatedVersion">
    /// The highest version common to the hub's supported range (<see cref="Protocol.MinSupportedVersion"/>
    /// to <see cref="Protocol.MaxSupportedVersion"/>) and the client's advertised range, when negotiation
    /// succeeds; otherwise <c>0</c>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the client's advertised range is well-formed and overlaps the hub's
    /// supported range; otherwise <see langword="false"/>.
    /// </returns>
    private static bool TryNegotiateProtocolVersion(
        byte clientMinVersion, byte clientMaxVersion, out byte negotiatedVersion)
    {
        if (clientMinVersion > clientMaxVersion)
        {
            negotiatedVersion = 0;
            return false;
        }

        int overlapMin = Math.Max(clientMinVersion, Protocol.MinSupportedVersion);
        int overlapMax = Math.Min(clientMaxVersion, Protocol.MaxSupportedVersion);

        if (overlapMin > overlapMax)
        {
            negotiatedVersion = 0;
            return false;
        }

        // Highest mutually supported version wins, so both peers speak as much of the shared
        // feature set as they can.
        negotiatedVersion = (byte)overlapMax;
        return true;
    }

    private async Task<bool> AuthenticateAsync(
        Guid clientId,
        string clientName,
        byte[] registrationData,
        int nameLength,
        CancellationToken cancellationToken)
    {
        // The authenticator runs on unauthenticated input, once per accepted connection, so an
        // unauthenticated peer can drive it simply by connecting. Bound how many may run at once,
        // otherwise a connection flood turns a deliberately expensive credential check into a
        // denial of service. Waiting is bounded too: a connection that cannot get a slot within the
        // registration timeout is refused as at-capacity rather than held indefinitely.
        if (!await _authenticationSlots!.WaitAsync(_registrationTimeout, cancellationToken).ConfigureAwait(false))
        {
            _logger.LogWarning(
                "Refusing client {ClientId} ({ClientName}): no authentication slot became available within {Timeout}",
                clientId,
                clientName,
                _registrationTimeout);
            return false;
        }

        try
        {
            // Copy the credential out of the registration frame so the context does not alias the larger
            // inbound buffer, which is safer if a caller retains it beyond the call.
            byte[] credential = registrationData.AsSpan(5 + nameLength).ToArray();
            var context = new RegistrationContext { ClientName = clientName, Credential = credential };

            bool authenticated;
            try
            {
                // Bound the authenticator by the registration timeout so a slow or hanging integrator
                // callback cannot hold the handler task (and its connection) open indefinitely. WaitAsync
                // abandons the wait even if the callback ignores the cancellation token.
                authenticated = await _authenticator!(context, cancellationToken)
                    .AsTask()
                    .WaitAsync(_registrationTimeout, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning(
                    "Authenticator did not complete within {Timeout} for client {ClientName}; refusing registration",
                    _registrationTimeout,
                    clientName);
                return false;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // The cancellation came from inside the callback, not from hub shutdown — an HTTP call to
                // an identity provider timing out is the common case. Treat it as a refusal, so the client
                // gets AuthenticationFailed and the reason is logged, rather than letting it unwind to the
                // handler's shutdown catch and drop the connection silently. Callback boundary.
                _logger.LogWarning(
                    "Authenticator was cancelled for client {ClientName}; refusing registration", clientName);
                return false;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A throwing authenticator must refuse the client, not fault the handler. Callback boundary.
                _logger.LogError(
                    ex, "Authenticator threw for client {ClientName}; refusing registration", clientName);
                return false;
            }

            if (!authenticated)
            {
                _logger.LogWarning(
                    "Refusing client {ClientId} ({ClientName}): authentication failed", clientId, clientName);
            }

            return authenticated;
        }
        finally
        {
            _authenticationSlots.Release();
        }
    }

    private void RaiseClientEvent(
        EventHandler<ClientConnectionEventArgs>? handler,
        Guid clientId,
        string clientName,
        string eventName)
    {
        if (handler is null)
        {
            return;
        }

        try
        {
            handler(this, new ClientConnectionEventArgs { ClientId = clientId, ClientName = clientName });
        }
        catch (Exception ex)
        {
            // A throwing subscriber must not fault the client handler task. Callback boundary.
            _logger.LogError(ex, "A {EventName} handler threw an exception", eventName);
        }
    }

    /// <summary>
    /// Raises <see cref="QueueSaturated"/> for a message dropped because the recipient's outbound queue
    /// was full. Always called, from every routing method's queue-full branch, so a hub-side subscriber
    /// sees every drop regardless of which routing shape produced it.
    /// </summary>
    /// <remarks>
    /// In-process only. The hub's own operator is already trusted with the identity of every client
    /// connected to it, so this carries the recipient's id whatever shape of send was dropped — unlike
    /// the wire notification in <see cref="NotifySenderOfQueueSaturation"/>, which does not.
    /// </remarks>
    private void RaiseQueueSaturated(Guid senderId, Guid recipientId)
    {
        EventHandler<QueueSaturatedEventArgs>? handler = QueueSaturated;
        if (handler is null)
        {
            return;
        }

        try
        {
            handler(this, new QueueSaturatedEventArgs { SenderId = senderId, RecipientId = recipientId });
        }
        catch (Exception ex)
        {
            // A throwing subscriber must not fault the client handler task. Callback boundary.
            _logger.LogError(ex, "A QueueSaturated handler threw an exception");
        }
    }

    /// <summary>
    /// Raises <see cref="QueueSaturated"/> and, only when this hub was constructed with
    /// <c>notifyOnQueueSaturation</c>, best-effort sends the sender a
    /// <see cref="MessageType.QueueSaturated"/> control frame naming the saturated recipient.
    /// </summary>
    /// <remarks>
    /// Only ever called from the two <b>direct</b>-send paths (<see cref="RouteMessage"/> and
    /// <see cref="RouteMessageWithHeaders"/>), where the sender supplied <paramref name="recipientId"/>
    /// itself and so learns nothing from being told it. The fan-out paths
    /// (<see cref="BroadcastMessage"/>, <see cref="SendToGroup"/>, <see cref="SendToGroupWithHeaders"/>)
    /// deliberately call <see cref="RaiseQueueSaturated"/> instead and send no frame: there the id comes
    /// from the hub's own client and group registries, not from the sender, so echoing it back would
    /// disclose the identity of a client the sender never named. Since any client may broadcast, and may
    /// address a group it never joined, that would let a sender enumerate the id of every client
    /// connected to the hub simply by broadcasting until somebody's queue filled — an identity census
    /// the name-based lookup deliberately does not offer, since that requires already knowing the name.
    /// The control frame carries only routing metadata — the recipient's id — never the dropped
    /// message's body.
    /// </remarks>
    private void NotifySenderOfQueueSaturation(Guid senderId, Guid recipientId)
    {
        RaiseQueueSaturated(senderId, recipientId);

        if (!_notifyOnQueueSaturation)
        {
            return;
        }

        if (!_clients.TryGetValue(senderId, out ClientConnection? sender))
        {
            return;
        }

        var nackPayload = new byte[17];
        nackPayload[0] = (byte)MessageType.QueueSaturated;
        recipientId.TryWriteBytes(nackPayload.AsSpan(1));

        // Best-effort: if the sender's own queue is also full, the notification is itself dropped rather
        // than retried or escalated — this is routing-level signalling, not guaranteed delivery. Sent
        // High priority: a control frame telling a sender to back off should not itself be stuck behind
        // the very backlog it is warning about.
        sender.OutboundQueue.TryEnqueue(MessagePriority.High, nackPayload);
    }

    /// <summary>
    /// Awaits free capacity on a saturated recipient queue, bounded by <see cref="_backpressureAwaitTimeout"/>,
    /// for a sender that opted into <see cref="DeliveryOptions.AwaitCapacity"/> via
    /// <see cref="BackpressureHeaderKeys.AwaitCapacity"/>.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if the frame was queued before the timeout elapsed; otherwise
    /// <see langword="false"/>, in which case the caller falls back to the ordinary drop-on-full path.
    /// </returns>
    private static async Task<bool> TryAwaitCapacityAsync(
        ClientConnection sender,
        ClientConnection recipient,
        MessagePriority priority,
        byte[] payload,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        // The sender's own receive loop is parked for the duration of this wait, so mark it as such:
        // it is not idle, and must not be evicted for failing to send frames it is not being read for.
        using ClientConnection.CapacityWaitScope parked = sender.BeginAwaitingCapacity();

        // TryEnqueueAsync already bounds the wait by the timeout and by the recipient's own disposal —
        // the recipient disconnecting while capacity is being awaited is handled there, not here.
        return await recipient.OutboundQueue
            .TryEnqueueAsync(priority, payload, timeout, cancellationToken)
            .ConfigureAwait(false);
    }

    // How many bytes of already-queued frames the send loop will coalesce into a single write before
    // flushing. Small enough to bound the rented buffer, large enough to absorb fan-out bursts.
    private const int SendCoalesceByteBudget = 64 * 1024;

    /// <summary>
    /// Determines whether a queued outbound frame has already passed the expiry its sender attached, so
    /// <see cref="SendLoopAsync"/> can drop it instead of delivering it stale, and records the drop on
    /// <see cref="_messagesDroppedCounter"/> when it does.
    /// </summary>
    /// <remarks>
    /// Absent, unparseable or out-of-range expiry data means "does not expire" — identical to a message
    /// with no time-to-live at all — via the shared, deliberately non-throwing
    /// <see cref="MessageExpiryHeaderKeys.TryParseExpiry"/>: this header block's bytes are entirely
    /// sender-controlled, so a hostile or merely malformed value (for example one numerically valid but
    /// outside the range <see cref="DateTimeOffset"/> can represent) must never throw and abort this
    /// connection's send loop. A malformed header block itself is not this check's concern either: the
    /// recipient's own decode already handles that case (logging and dropping just that frame), so
    /// treating it as non-expiring here does not lose anything twice over.
    /// </remarks>
    private bool IsExpiredFrame(byte[] frame, Guid recipientId)
    {
        if (!TryGetHeaderBlock(frame, out ReadOnlySpan<byte> block))
        {
            return false;
        }

        string? expiresAtText;
        try
        {
            if (!HeaderEnvelope.TryReadValue(
                block, block.Length, MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds, out expiresAtText))
            {
                return false;
            }
        }
        catch (FormatException)
        {
            return false;
        }

        if (!MessageExpiryHeaderKeys.TryParseExpiry(expiresAtText, out DateTimeOffset expiry)
            || DateTimeOffset.UtcNow <= expiry)
        {
            return false;
        }

        _logger.LogDebug("Dropping an expired message queued for {RecipientId}", recipientId);
        _messagesDroppedCounter.Add(1, ExpiredDropTag);
        return true;
    }

    /// <summary>
    /// Locates the header block within a queued outbound delivery frame, if it is one of the two
    /// header-bearing delivery opcodes. Used only so <see cref="IsExpiredFrame"/> can check the one
    /// well-known expiry header — the hub still never inspects a frame's body.
    /// </summary>
    private static bool TryGetHeaderBlock(ReadOnlySpan<byte> frame, out ReadOnlySpan<byte> block)
    {
        block = default;

        if (frame.Length < 1)
        {
            return false;
        }

        switch ((MessageType)frame[0])
        {
            case MessageType.DeliverMessageWithHeaders when frame.Length >= 19:
            {
                int headerLength = BinaryPrimitives.ReadUInt16BigEndian(frame.Slice(17, 2));
                if (frame.Length < 19 + headerLength)
                {
                    return false;
                }

                block = frame.Slice(19, headerLength);
                return true;
            }

            case MessageType.DeliverGroupMessageWithHeaders when frame.Length >= 19:
            {
                int nameLength = BinaryPrimitives.ReadUInt16BigEndian(frame.Slice(17, 2));
                int headerLengthOffset = 19 + nameLength;
                if (frame.Length < headerLengthOffset + 2)
                {
                    return false;
                }

                int headerLength = BinaryPrimitives.ReadUInt16BigEndian(frame.Slice(headerLengthOffset, 2));
                if (frame.Length < headerLengthOffset + 2 + headerLength)
                {
                    return false;
                }

                block = frame.Slice(headerLengthOffset + 2, headerLength);
                return true;
            }

            default:
                return false;
        }
    }

    private async Task SendLoopAsync(ClientConnection connection, CancellationTokenSource clientCts)
    {
        // Reused across the connection's lifetime so coalescing adds no per-frame allocation.
        var batch = new List<ReadOnlyMemory<byte>>();

        try
        {
            await foreach (byte[] payload in connection.OutboundQueue
                .ReadAllAsync(clientCts.Token).ConfigureAwait(false))
            {
                // A frame that has already passed the expiry its sender attached (SendAsync's
                // time-to-live overload) is dropped here rather than delivered stale — this is
                // precisely the "expired while queued" case the feature exists for: the frame was
                // still fresh when RouteMessageWithHeaders/SendToGroupWithHeaders queued it, but has
                // since sat behind other traffic. IsExpiredFrame only ever inspects the one well-known
                // expiry header, never the body, so this remains true to "the hub routes opaque bytes".
                if (IsExpiredFrame(payload, connection.Id))
                {
                    continue;
                }

                long batchBytes = payload.Length;
                batch.Add(payload);

                // Drain whatever is already queued so a fan-out burst becomes one write. TryRead never
                // blocks, so a lone frame is sent immediately with no added latency; only frames already
                // waiting are batched, and only up to the byte budget so the write stays bounded.
                while (batchBytes < SendCoalesceByteBudget
                    && connection.OutboundQueue.TryDequeue(out byte[]? next))
                {
                    if (IsExpiredFrame(next, connection.Id))
                    {
                        continue;
                    }

                    batch.Add(next);
                    batchBytes += next.Length;
                }

                if (batch.Count == 0)
                {
                    // Everything drained on this pass had already expired; nothing left to send.
                    continue;
                }

                if (batch.Count == 1)
                {
                    await connection.Transport.SendAsync(batch[0], clientCts.Token).ConfigureAwait(false);
                }
                else if (connection.Transport is IBatchSendTransport batchTransport)
                {
                    await batchTransport.SendAsync(batch, clientCts.Token).ConfigureAwait(false);
                }
                else
                {
                    // A transport without batching support still gets every frame, just one at a time.
                    foreach (ReadOnlyMemory<byte> queuedFrame in batch)
                    {
                        await connection.Transport.SendAsync(queuedFrame, clientCts.Token).ConfigureAwait(false);
                    }
                }

                batch.Clear();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or ArgumentException)
        {
            // ArgumentException here means a transport (TcpTransport, notably) rejected a delivery
            // frame as oversized — the routing methods above can grow a near-maximum-size inbound
            // payload past the transport's cap by adding the sender id, group name and, for a
            // header-bearing frame, the header block. Left uncaught, this would fault the task that
            // HandleClientAsync's own cleanup awaits from inside its finally block, aborting that
            // finally partway through and skipping the slot release, name removal, group removal and
            // disposal that follow it — leaking the client's registration permanently rather than
            // merely losing the one oversized message. Treating it exactly like a transport fault, by
            // cancelling the client here instead of letting it propagate, keeps that cleanup intact.
            _logger.LogWarning(
                ex,
                "Send loop for client {ClientId} terminated due to transport error",
                connection.Id);
            await clientCts.CancelAsync().ConfigureAwait(false);
        }
    }

    private async Task MonitorHeartbeatAsync(
        ClientConnection connection,
        CancellationTokenSource clientCts,
        TimeSpan interval,
        Guid clientId)
    {
        // One timer per connection, reused for the connection's whole lifetime. Between ticks the
        // receive loop bumps ActivitySequence for every frame; an unchanged sequence across a tick
        // means the client sent nothing during that interval, so it is probed and eventually evicted.
        using var timer = new PeriodicTimer(interval);
        long lastSeenActivity = connection.ActivitySequence;
        int missedHeartbeats = 0;

        try
        {
            while (await timer.WaitForNextTickAsync(clientCts.Token).ConfigureAwait(false))
            {
                long currentActivity = connection.ActivitySequence;
                if (currentActivity != lastSeenActivity)
                {
                    // A frame arrived during the interval; the client is alive.
                    lastSeenActivity = currentActivity;
                    missedHeartbeats = 0;
                    continue;
                }

                // The connection read nothing across this interval because the hub deliberately parked
                // its receive loop awaiting capacity for one of its messages — not because the client
                // went silent. It cannot answer a ping the loop is not there to read, so probing it
                // would be pointless and evicting it would drop a healthy client for backpressure the
                // hub itself applied. Treat the park as liveness and reset the counter.
                if (connection.IsAwaitingCapacity)
                {
                    missedHeartbeats = 0;
                    continue;
                }

                // The client sent nothing across this interval. Evicting on the _maxMissedHeartbeats'th
                // consecutive silent interval — not the one after it — is what the documented contract
                // promises, so the comparison must be inclusive.
                missedHeartbeats++;
                if (missedHeartbeats >= _maxMissedHeartbeats)
                {
                    _logger.LogInformation(
                        "Client {ClientId} was idle across {Missed} consecutive heartbeat intervals; evicting",
                        clientId,
                        missedHeartbeats);
                    await clientCts.CancelAsync().ConfigureAwait(false);
                    return;
                }

                // Probe liveness via the outbound queue so the ping serialises with any other queued
                // frames. A live client replies with a Pong (or any frame), resetting the counter. Sent
                // High priority so a liveness probe is never delayed behind a client's own backlog — a
                // ping stuck behind bulk traffic could otherwise starve into a false eviction.
                connection.OutboundQueue.TryEnqueue(MessagePriority.High, [(byte)MessageType.Ping]);
            }
        }
        catch (OperationCanceledException)
        {
            // The connection's cancellation token was triggered; stop monitoring.
        }
    }

    /// <remarks>
    /// <c>async</c> since the offline store (issue #28) was added, and named without the <c>Async</c>
    /// suffix to match its sibling <see cref="RouteMessageWithHeaders"/> rather than the general
    /// convention. Nothing on the delivered path awaits: a recipient that is connected — every message
    /// on a hub with no store configured, and the overwhelming majority on one that has — runs to
    /// completion synchronously and returns the cached completed task.
    /// </remarks>
    private async Task RouteMessage(
        Guid senderId,
        Guid recipientId,
        ReadOnlyMemory<byte> messageData,
        CancellationToken cancellationToken)
    {
        if (!_clients.TryGetValue(recipientId, out ClientConnection? recipient))
        {
            if (await TryStoreForOfflineDeliveryAsync(
                    senderId, recipientId, ReadOnlyMemory<byte>.Empty, messageData, cancellationToken)
                .ConfigureAwait(false))
            {
                return;
            }

            _logger.LogDebug(
                "Message from {SenderId} dropped: recipient {RecipientId} not found",
                senderId,
                recipientId);
            _messagesDroppedCounter.Add(1, UnknownRecipientDropTag);
            return;
        }

        var deliveryPayload = new byte[1 + 16 + messageData.Length];
        deliveryPayload[0] = (byte)MessageType.DeliverMessage;
        senderId.TryWriteBytes(deliveryPayload.AsSpan(1));
        messageData.CopyTo(deliveryPayload.AsMemory(17));

        // The plain (headerless) opcode carries no priority hint, so it always lands on the normal
        // lane — exactly the queueing behaviour that existed before priority lanes did.
        if (!recipient.OutboundQueue.TryEnqueue(MessagePriority.Normal, deliveryPayload))
        {
            _logger.LogWarning(
                "Outbound queue for {RecipientId} is full, message from {SenderId} dropped",
                recipientId,
                senderId);
            _messagesDroppedCounter.Add(1, QueueFullDropTag);
            NotifySenderOfQueueSaturation(senderId, recipientId);
            return;
        }

        // A direct send has exactly one recipient, so "routed" only counts once the frame has actually
        // been queued for delivery — unlike the fan-out sends below, there is no partial-success case to
        // reconcile.
        _messagesRoutedCounter.Add(1, DirectDirectionTag);
        _bytesRoutedCounter.Add(messageData.Length, DirectDirectionTag);
    }

    /// <summary>
    /// Routes a header-bearing direct message to its recipient, choosing the outgoing frame shape from
    /// that recipient's own negotiated protocol version.
    /// </summary>
    /// <remarks>
    /// The header block is never decoded here beyond a single well-known key — the hub reads its length
    /// so it can either forward it unchanged to a recipient that understands
    /// <see cref="MessageType.DeliverMessageWithHeaders"/>, or strip it entirely and fall back to the
    /// plain <see cref="MessageType.DeliverMessage"/> frame for a recipient negotiated below
    /// <see cref="Protocol.HeaderEnvelopeMinVersion"/>, which could not parse a header block it does not
    /// recognise the opcode for. A sender that set <see cref="BackpressureHeaderKeys.AwaitCapacity"/>
    /// (via <see cref="DeliveryOptions.AwaitCapacity"/>) is awaited on a saturated queue instead of being
    /// dropped immediately — see <see cref="TryAwaitCapacityAsync"/>.
    /// </remarks>
    private async Task RouteMessageWithHeaders(
        Guid senderId,
        Guid recipientId,
        ReadOnlyMemory<byte> headerBlock,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        if (!_clients.TryGetValue(recipientId, out ClientConnection? recipient))
        {
            if (await TryStoreForOfflineDeliveryAsync(
                    senderId, recipientId, headerBlock, body, cancellationToken).ConfigureAwait(false))
            {
                return;
            }

            _logger.LogDebug(
                "Message from {SenderId} dropped: recipient {RecipientId} not found",
                senderId,
                recipientId);
            _messagesDroppedCounter.Add(1, UnknownRecipientDropTag);
            return;
        }

        byte[] deliveryPayload = recipient.NegotiatedProtocolVersion >= Protocol.HeaderEnvelopeMinVersion
            ? BuildDeliverMessageWithHeaders(senderId, headerBlock, body)
            : BuildDeliverMessage(senderId, body);

        MessagePriority priority = ReadPriority(headerBlock);
        bool queued = recipient.OutboundQueue.TryEnqueue(priority, deliveryPayload);

        // Only awaited when the sender is still registered: the park is recorded on the sender's own
        // connection, and a sender that has gone away has no receive loop left to park or protect.
        if (!queued
            && WantsAwaitCapacity(headerBlock)
            && _clients.TryGetValue(senderId, out ClientConnection? sender))
        {
            queued = await TryAwaitCapacityAsync(
                    sender, recipient, priority, deliveryPayload, _backpressureAwaitTimeout, cancellationToken)
                .ConfigureAwait(false);
        }

        if (!queued)
        {
            _logger.LogWarning(
                "Outbound queue for {RecipientId} is full, message from {SenderId} dropped",
                recipientId,
                senderId);
            _messagesDroppedCounter.Add(1, QueueFullDropTag);
            NotifySenderOfQueueSaturation(senderId, recipientId);
            return;
        }

        _messagesRoutedCounter.Add(1, DirectDirectionTag);
        _bytesRoutedCounter.Add(body.Length, DirectDirectionTag);
    }

    /// <summary>
    /// Offers a direct message whose recipient is not connected to the configured offline store, so it
    /// can be delivered when that recipient's name next registers.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if the offline path took ownership of the message — either storing it, or
    /// having it refused by a store that was full and counting that refusal itself; <see langword="false"/>
    /// if store-and-forward does not apply, in which case the caller drops the message the way it always
    /// did.
    /// </returns>
    /// <remarks>
    /// <para>
    /// A sender addresses a recipient by the id it looked up, so the first thing this has to do is turn
    /// that id back into the name the offline store is keyed by. Only ids the hub retained at disconnect
    /// resolve; an id that never existed, or one whose name has since come back under a <em>different</em>
    /// id, does not — the latter is forgotten on the spot, so a stale id stops resolving the moment its
    /// owner is reachable again rather than quietly accruing messages nobody will ever drain.
    /// </para>
    /// <para>
    /// Both byte ranges are copied out of the inbound frame before they are handed over: the receive
    /// buffer is reused, and a store may hold what it is given for minutes.
    /// </para>
    /// </remarks>
    private async Task<bool> TryStoreForOfflineDeliveryAsync(
        Guid senderId,
        Guid recipientId,
        ReadOnlyMemory<byte> headerBlock,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        if (_offlineStore is null)
        {
            return false;
        }

        if (!_offlineNamesById.TryGetValue(recipientId, out string? recipientName))
        {
            return false;
        }

        if (_clientNames.ContainsKey(recipientName))
        {
            // The name is registered again under an id this sender does not know. Storing here would put
            // the message somewhere only the *next* reconnect would drain, while the recipient sits
            // connected — a worse outcome than the ordinary unknown-recipient drop, which at least tells
            // the truth about the id being stale.
            ForgetOfflineIdentity(recipientName);
            return false;
        }

        var message = new OfflineMessage(
            senderId, headerBlock.ToArray(), body.ToArray(), DateTimeOffset.UtcNow);

        bool stored;
        try
        {
            stored = await _offlineStore
                .TryEnqueueAsync(recipientName, message, cancellationToken)
                .AsTask()
                .WaitAsync(_offlineStoreTimeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // The store took longer than the bound, or cancelled itself. Either way this message is not
            // stored; fall back to the ordinary drop rather than holding the sender's receive loop.
            _logger.LogWarning(
                "Offline store did not accept a message for {RecipientName} within {Timeout}",
                recipientName,
                _offlineStoreTimeout);
            return false;
        }
        catch (TimeoutException)
        {
            _logger.LogWarning(
                "Offline store did not accept a message for {RecipientName} within {Timeout}",
                recipientName,
                _offlineStoreTimeout);
            return false;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Callback boundary: an integrator's store must not fault the sender's receive loop.
            _logger.LogError(ex, "Offline store threw while storing a message for {RecipientName}", recipientName);
            return false;
        }

        if (!stored)
        {
            _logger.LogWarning(
                "Offline store refused a message from {SenderId} for {RecipientName}; dropped",
                senderId,
                recipientName);
            _messagesDroppedCounter.Add(1, OfflineQueueFullDropTag);
            return true;
        }

        _messagesOfflineQueuedCounter.Add(1);
        _logger.LogDebug(
            "Message from {SenderId} for disconnected {RecipientName} held for later delivery",
            senderId,
            recipientName);
        return true;
    }

    /// <summary>
    /// Hands a newly registered client everything the offline store was holding for its name, oldest
    /// first, before its receive loop starts.
    /// </summary>
    /// <remarks>
    /// Queued straight onto the connection's outbound queue rather than routed, because routing would
    /// look the sender up and these messages outlive their senders routinely. The frame shape is chosen
    /// from <em>this</em> connection's negotiated version, exactly as live routing does — which is the
    /// whole reason the store holds message parts rather than built frames. The stored priority header is
    /// honoured too, so a high-priority message that arrived while the client was away still overtakes
    /// the backlog it was stored behind.
    /// </remarks>
    private async Task DeliverStoredMessagesAsync(ClientConnection connection, CancellationToken cancellationToken)
    {
        if (_offlineStore is null)
        {
            return;
        }

        IReadOnlyList<OfflineMessage> pending;
        try
        {
            pending = await _offlineStore
                .TakeAllAsync(connection.Name, cancellationToken)
                .AsTask()
                .WaitAsync(_offlineStoreTimeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Offline store did not return {ClientName}'s held messages within {Timeout}",
                connection.Name,
                _offlineStoreTimeout);
            return;
        }
        catch (TimeoutException)
        {
            _logger.LogWarning(
                "Offline store did not return {ClientName}'s held messages within {Timeout}",
                connection.Name,
                _offlineStoreTimeout);
            return;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Callback boundary: a throwing store must not stop the client from connecting.
            _logger.LogError(ex, "Offline store threw while draining {ClientName}", connection.Name);
            return;
        }

        if (pending.Count == 0)
        {
            return;
        }

        foreach (OfflineMessage message in pending)
        {
            byte[] deliveryPayload =
                !message.HeaderBlock.IsEmpty
                && connection.NegotiatedProtocolVersion >= Protocol.HeaderEnvelopeMinVersion
                    ? BuildDeliverMessageWithHeaders(message.SenderId, message.HeaderBlock, message.Body)
                    : BuildDeliverMessage(message.SenderId, message.Body);

            if (!connection.OutboundQueue.TryEnqueue(ReadPriority(message.HeaderBlock), deliveryPayload))
            {
                // More was held than the outbound queue can take at once. The rest of the drain is
                // attempted anyway rather than abandoned: the queue is drained concurrently by the send
                // loop that is already running, so a later message may well still fit.
                _logger.LogWarning(
                    "Outbound queue for {ClientId} is full, held message from {SenderId} dropped",
                    connection.Id,
                    message.SenderId);
                _messagesDroppedCounter.Add(1, QueueFullDropTag);
                RaiseQueueSaturated(message.SenderId, connection.Id);
                continue;
            }

            _messagesRoutedCounter.Add(1, DirectDirectionTag);
            _bytesRoutedCounter.Add(message.Body.Length, DirectDirectionTag);
        }

        _logger.LogInformation(
            "Delivered {MessageCount} held message(s) to {ClientName} on registration",
            pending.Count,
            connection.Name);
    }

    /// <summary>
    /// Mints a resumption token for a newly registered connection and records the session it reclaims,
    /// or returns <see langword="null"/> when resumption is switched off, the connection negotiated too
    /// low a version for it, or the session table is full.
    /// </summary>
    /// <remarks>
    /// The token is returned to the caller — to be put on the wire exactly once, in the
    /// <see cref="MessageType.RegistrationComplete"/> reply — while only its hash is retained here.
    /// </remarks>
    private byte[]? IssueSessionToken(ClientConnection connection)
    {
        if (_sessionResumptionWindow is null
            || connection.NegotiatedProtocolVersion < Protocol.SessionResumptionMinVersion)
        {
            return null;
        }

        if (_sessions.Count >= MaxClients)
        {
            PurgeExpiredSessions();

            if (_sessions.Count >= MaxClients)
            {
                // Refusing a token rather than evicting somebody else's session: a client that is not
                // issued one simply cannot resume, which is the pre-feature behaviour, whereas evicting
                // would silently break a resumption another client is entitled to.
                _logger.LogDebug(
                    "Session table is full; {ClientName} was not issued a resumption token", connection.Name);
                return null;
            }
        }

        byte[] token = RandomNumberGenerator.GetBytes(Protocol.SessionTokenLength);
        string tokenHash = HashSessionToken(token);

        _sessions[tokenHash] = new ResumableSession(connection.Name, connection.Id);
        connection.SessionTokenHash = tokenHash;

        return token;
    }

    /// <summary>
    /// Marks a departing connection's session dormant — capturing the groups it was in and the instant
    /// its resumption window closes — so a reconnect within that window can reclaim it.
    /// </summary>
    private void MakeSessionDormant(ClientConnection connection)
    {
        if (_sessionResumptionWindow is not { } window
            || connection.SessionTokenHash is not { } tokenHash)
        {
            return;
        }

        if (_sessions.TryGetValue(tokenHash, out ResumableSession? session))
        {
            // Captured here rather than read live at resume time, because by then the connection has
            // been removed from every group it was in — this teardown is what removes it.
            session.Groups = [.. connection.Groups];
            session.DormantUntil = DateTimeOffset.UtcNow + window;
        }
    }

    /// <summary>
    /// Handles a <see cref="MessageType.ResumeSession"/> frame: reclaims the id, group memberships and
    /// resumption token of a previous session, or refuses and leaves the client on the fresh identity it
    /// registered with.
    /// </summary>
    /// <returns>
    /// The id this connection is known by after the attempt — the reclaimed one on success, the one that
    /// was passed in otherwise.
    /// </returns>
    /// <remarks>
    /// <para>
    /// Runs after an ordinary registration has already completed, which is what makes the whole feature
    /// degrade cleanly: a hub that does not know this opcode ignores it, and the client simply keeps the
    /// identity it was just assigned. That is also why the token could not be carried in the registration
    /// frame itself — the client has to send that before it knows what version was negotiated, so an
    /// older hub would have misparsed a token field as credential bytes.
    /// </para>
    /// <para>
    /// The token is single-use: the winning <c>TryRemove</c> is what makes two connections racing the
    /// same token resolve to exactly one resumption, and a fresh token is issued in the reply.
    /// </para>
    /// </remarks>
    private async Task<Guid> ResumeSessionAsync(
        ClientConnection connection, ReadOnlyMemory<byte> token, CancellationToken cancellationToken)
    {
        if (_sessionResumptionWindow is null
            || connection.NegotiatedProtocolVersion < Protocol.SessionResumptionMinVersion
            || token.Length != Protocol.SessionTokenLength)
        {
            await RefuseSessionResumeAsync(connection, cancellationToken).ConfigureAwait(false);
            return connection.Id;
        }

        string tokenHash = HashSessionToken(token.Span);

        // Looked up before it is claimed, so a token that fails validation — one whose session is still
        // held by a live connection, notably — is left in place for its rightful owner rather than being
        // burnt by anyone who can present it once.
        if (!_sessions.TryGetValue(tokenHash, out ResumableSession? session)
            || session.DormantUntil is not { } dormantUntil
            || dormantUntil <= DateTimeOffset.UtcNow
            || !string.Equals(session.Name, connection.Name, StringComparison.Ordinal)
            || _clients.ContainsKey(session.ClientId))
        {
            _logger.LogDebug(
                "Session resumption refused for {ClientName}: no live session matched the token",
                connection.Name);
            await RefuseSessionResumeAsync(connection, cancellationToken).ConfigureAwait(false);
            return connection.Id;
        }

        // Claiming the entry is what enforces single use: of two connections presenting the same token at
        // once, exactly one gets true here and the other is refused.
        if (!_sessions.TryRemove(new KeyValuePair<string, ResumableSession>(tokenHash, session)))
        {
            await RefuseSessionResumeAsync(connection, cancellationToken).ConfigureAwait(false);
            return connection.Id;
        }

        // This connection's own registration already issued it a token and recorded an entry for it,
        // which becomes unreachable the moment the identity swap below moves it onto the resumed id.
        // Left in place, that entry would never be revisited by anything — it is not the fresh id's
        // registration entry any more, nor the spent token above — and its DormantUntil would stay null
        // for ever, making it permanently unreclaimable by PurgeExpiredSessions. Removed alongside the
        // spent token so a successful resume never grows the table.
        if (connection.SessionTokenHash is { } freshTokenHash)
        {
            _sessions.TryRemove(freshTokenHash, out _);
        }

        Guid freshId = connection.Id;
        Guid resumedId = session.ClientId;

        // Published under the reclaimed id before the fresh one is withdrawn, so a peer addressing either
        // reaches the connection throughout the swap rather than falling into a gap where neither
        // resolves. Both mapping to one connection for an instant is harmless; neither doing so is not.
        _clients[resumedId] = connection;
        connection.Rebind(resumedId);
        _clientNames[connection.Name] = resumedId;
        _clients.TryRemove(freshId, out _);

        // The registration that admitted this connection raised ClientConnected for freshId, and nothing
        // has raised ClientDisconnected for it since — the connection never actually dropped, it just
        // changed which id it answers to. A subscriber tracking connected ids would otherwise leak
        // freshId for ever and later receive an unmatched ClientDisconnected for resumedId at teardown.
        // Raising this pair keeps every id balanced without inventing a new event type.
        RaiseClientEvent(ClientDisconnected, freshId, connection.Name, nameof(ClientDisconnected));
        RaiseClientEvent(ClientConnected, resumedId, connection.Name, nameof(ClientConnected));

        IReadOnlyList<string> restoredGroups = await RestoreGroupMembershipAsync(
            connection, session.Groups, cancellationToken).ConfigureAwait(false);

        byte[] renewedToken = RandomNumberGenerator.GetBytes(Protocol.SessionTokenLength);
        string renewedHash = HashSessionToken(renewedToken);
        _sessions[renewedHash] = new ResumableSession(connection.Name, resumedId);
        connection.SessionTokenHash = renewedHash;

        byte[] reply = BuildSessionResumedReply(connection, resumedId, renewedToken, restoredGroups);
        await connection.Transport.SendAsync(reply, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Client {ClientName} resumed session {ResumedId}, replacing {FreshId}",
            connection.Name,
            resumedId,
            freshId);

        return resumedId;
    }

    /// <summary>
    /// Re-establishes the group memberships a resumed session held, running each one back through the
    /// group authoriser rather than reinstating it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A restore that bypassed the authoriser would make the first <see langword="true"/> permanent: an
    /// authoriser that has since changed its mind would find the client still receiving that group's
    /// traffic and still entitled to send to it. The hub already refuses to let
    /// <see cref="MeshClientReconnector"/>'s own restore do that (it re-joins over the wire, and every
    /// join is authorised on its own merits), and resumption must not become the back door.
    /// </para>
    /// <para>
    /// A refused group is simply not restored, with no <see cref="MessageType.GroupJoinRefused"/> frame:
    /// the client did not ask to join anything on this connection, so there is no join to refuse, and
    /// re-encoding a stored name to echo it could not preserve the size guarantee that frame relies on.
    /// </para>
    /// </remarks>
    /// <returns>
    /// The subset of <paramref name="groups"/> that was actually restored, in the order the authoriser
    /// considered them — every entry the caller can trust the resumed connection is now a genuine member
    /// of, for a version-7-or-later peer to be told about in the <see cref="MessageType.SessionResumed"/>
    /// reply.
    /// </returns>
    private async Task<IReadOnlyList<string>> RestoreGroupMembershipAsync(
        ClientConnection connection, IReadOnlyList<string> groups, CancellationToken cancellationToken)
    {
        List<string>? restored = null;

        foreach (string groupName in groups)
        {
            if (_groupAuthoriser is not null
                && !await AuthoriseGroupJoinAsync(connection, groupName, cancellationToken).ConfigureAwait(false))
            {
                _logger.LogWarning(
                    "Group {GroupName} was not restored for resumed client {ClientId}: the authoriser refused it",
                    ForLog(groupName),
                    connection.Id);
                continue;
            }

            AddToGroup(connection, groupName);
            (restored ??= []).Add(groupName);
        }

        return restored ?? [];
    }

    /// <summary>
    /// Builds the <see cref="MessageType.SessionResumed"/> reply frame: the reclaimed id and a fresh
    /// resumption token, and — for a peer that negotiated <see cref="Protocol.SessionResumedGroupsMinVersion"/>
    /// or later — the group memberships <see cref="RestoreGroupMembershipAsync"/> actually restored, so the
    /// client can repopulate its own membership record without re-joining anything.
    /// </summary>
    /// <remarks>
    /// A peer below that version gets exactly the frame version 6 always produced: id, token length, token,
    /// nothing more. The group block is appended, never inserted, so an older client's fixed reads of the
    /// leading fields are unaffected by its presence and it costs a version-6 connection nothing.
    /// </remarks>
    private byte[] BuildSessionResumedReply(
        ClientConnection connection, Guid resumedId, byte[] renewedToken, IReadOnlyList<string> restoredGroups)
    {
        if (connection.NegotiatedProtocolVersion < Protocol.SessionResumedGroupsMinVersion)
        {
            var replyWithoutGroups = new byte[1 + 16 + 2 + renewedToken.Length];
            replyWithoutGroups[0] = (byte)MessageType.SessionResumed;
            resumedId.TryWriteBytes(replyWithoutGroups.AsSpan(1, 16));
            BinaryPrimitives.WriteUInt16BigEndian(replyWithoutGroups.AsSpan(17, 2), (ushort)renewedToken.Length);
            renewedToken.CopyTo(replyWithoutGroups.AsSpan(19));
            return replyWithoutGroups;
        }

        byte[][] groupNameBytes = BuildReportableGroupNameBytes(connection, restoredGroups);
        int groupsBlockLength = 2 + groupNameBytes.Sum(nameBytes => 2 + nameBytes.Length);

        var reply = new byte[1 + 16 + 2 + renewedToken.Length + groupsBlockLength];
        reply[0] = (byte)MessageType.SessionResumed;
        resumedId.TryWriteBytes(reply.AsSpan(1, 16));
        BinaryPrimitives.WriteUInt16BigEndian(reply.AsSpan(17, 2), (ushort)renewedToken.Length);
        renewedToken.CopyTo(reply.AsSpan(19));

        int offset = 19 + renewedToken.Length;
        BinaryPrimitives.WriteUInt16BigEndian(reply.AsSpan(offset, 2), (ushort)groupNameBytes.Length);
        offset += 2;

        foreach (byte[] nameBytes in groupNameBytes)
        {
            BinaryPrimitives.WriteUInt16BigEndian(reply.AsSpan(offset, 2), (ushort)nameBytes.Length);
            offset += 2;
            nameBytes.CopyTo(reply, offset);
            offset += nameBytes.Length;
        }

        return reply;
    }

    /// <summary>
    /// Encodes the group names a <see cref="MessageType.SessionResumed"/> reply can actually name, dropping
    /// any that cannot be represented in this block's <see cref="ushort"/>-prefixed wire format rather than
    /// letting a bare cast silently truncate the length prefix out of step with the bytes that follow it.
    /// </summary>
    /// <remarks>
    /// Nothing today caps a group name's length or a connection's membership count, so both bounds are
    /// reachable in principle even though ordinary use never gets near them. A dropped name is not an
    /// unrestored membership — the join already succeeded in <see cref="RestoreGroupMembershipAsync"/> — it
    /// is only left out of what this particular reply can report, exactly like the version-6 case above.
    /// </remarks>
    private byte[][] BuildReportableGroupNameBytes(ClientConnection connection, IReadOnlyList<string> restoredGroups)
    {
        var groupNameBytes = new List<byte[]>();

        foreach (string groupName in restoredGroups)
        {
            byte[] nameBytes = Encoding.UTF8.GetBytes(groupName);

            if (nameBytes.Length > ushort.MaxValue)
            {
                _logger.LogWarning(
                    "Group {GroupName} restored for resumed client {ClientId} is too long to report in the " +
                    "SessionResumed reply; the membership is real, but this reply cannot name it",
                    ForLog(groupName),
                    connection.Id);
                continue;
            }

            if (groupNameBytes.Count == ushort.MaxValue)
            {
                _logger.LogWarning(
                    "Resumed client {ClientId} restored more group memberships than the SessionResumed " +
                    "reply's {MaxReportableGroups} group cap; the remainder are not reported",
                    connection.Id,
                    ushort.MaxValue);
                break;
            }

            groupNameBytes.Add(nameBytes);
        }

        return [.. groupNameBytes];
    }

    private async Task RefuseSessionResumeAsync(ClientConnection connection, CancellationToken cancellationToken)
    {
        byte[] refusal = [(byte)MessageType.SessionResumeRefused];
        await connection.Transport.SendAsync(refusal, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops every session whose resumption window has closed. Only called when the table is at its
    /// bound, since a hub below it has nothing to gain from the sweep.
    /// </summary>
    /// <remarks>
    /// A <c>null</c> <see cref="ResumableSession.DormantUntil"/> ordinarily means the session's
    /// connection is still live and must not be reclaimed. An entry can only end up with a
    /// <c>null</c> <see cref="ResumableSession.DormantUntil"/> and no live connection behind it if a
    /// bug orphaned it — <see cref="ResumeSessionAsync"/> is careful not to — so this is a backstop:
    /// such an entry is stale rather than protected, and is swept alongside the genuinely expired ones
    /// rather than pinning the table shut for ever.
    /// </remarks>
    private void PurgeExpiredSessions()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;

        foreach (KeyValuePair<string, ResumableSession> entry in _sessions)
        {
            bool expired = entry.Value.DormantUntil is { } dormantUntil && dormantUntil <= now;
            bool orphaned = entry.Value.DormantUntil is null && !_clients.ContainsKey(entry.Value.ClientId);

            if (expired || orphaned)
            {
                _sessions.TryRemove(entry);
            }
        }
    }

    private static string HashSessionToken(ReadOnlySpan<byte> token)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(token, hash);
        return Convert.ToHexString(hash);
    }

    /// <summary>
    /// A registered identity that outlives the connection that held it, so a reconnect within the
    /// resumption window can reclaim the same id and group memberships rather than starting afresh.
    /// </summary>
    /// <remarks>
    /// <see cref="DormantUntil"/> being <see langword="null"/> means the session's connection is still
    /// live, and that is exactly what makes it unresumable — a token is a way to reclaim an identity
    /// nobody is currently using, never a way to take one off somebody who is.
    /// </remarks>
    private sealed class ResumableSession(string name, Guid clientId)
    {
        public string Name { get; } = name;

        public Guid ClientId { get; } = clientId;

        public IReadOnlyList<string> Groups { get; set; } = [];

        public DateTimeOffset? DormantUntil { get; set; }
    }

    /// <summary>
    /// Remembers which name held <paramref name="clientId"/>, so a message addressed to that id after the
    /// client has gone can still be stored under its name.
    /// </summary>
    /// <remarks>
    /// Bounded by <see cref="MaxClients"/>: a hub that sees a long tail of one-shot names must not
    /// accumulate an entry for every name it has ever admitted. Once the retention map is full, further
    /// disconnects are simply not retained — their messages take the ordinary unknown-recipient drop —
    /// rather than evicting an identity that may be about to be reclaimed.
    /// </remarks>
    private void RetainOfflineIdentity(Guid clientId, string clientName)
    {
        if (_offlineNamesById.Count >= MaxClients)
        {
            _logger.LogDebug(
                "Offline identity retention is full; {ClientName} will not be reachable by its previous id",
                clientName);
            return;
        }

        // A name that reconnected and left again leaves its earlier id behind; drop it as the new one is
        // recorded, so the reverse map holds exactly one id per name.
        if (_offlineIdsByName.TryGetValue(clientName, out Guid previousId) && previousId != clientId)
        {
            _offlineNamesById.TryRemove(previousId, out _);
        }

        _offlineIdsByName[clientName] = clientId;
        _offlineNamesById[clientId] = clientName;
    }

    /// <summary>
    /// Forgets the id a name was last reachable by, so it stops resolving to that name.
    /// </summary>
    private void ForgetOfflineIdentity(string clientName)
    {
        if (_offlineIdsByName.TryRemove(clientName, out Guid previousId))
        {
            _offlineNamesById.TryRemove(previousId, out _);
        }
    }

    /// <summary>
    /// Checks the one well-known <see cref="BackpressureHeaderKeys.AwaitCapacity"/> header, without
    /// decoding the rest of the block. A malformed block is tolerated as "not requested", mirroring
    /// <see cref="IsExpiredFrame"/> — the header block is sender-supplied and must never be able to fault
    /// the routing path that reads it.
    /// </summary>
    private static bool WantsAwaitCapacity(ReadOnlyMemory<byte> headerBlock)
    {
        try
        {
            return HeaderEnvelope.TryReadValue(
                    headerBlock.Span, headerBlock.Length, BackpressureHeaderKeys.AwaitCapacity, out string? value)
                && value == "1";
        }
        catch (FormatException)
        {
            return false;
        }
    }

    /// <summary>
    /// Reads the one well-known <see cref="MessagePriorityHeaderKeys.Priority"/> header, without decoding
    /// the rest of the block, to decide which outbound lane a frame is queued on.
    /// </summary>
    /// <remarks>
    /// A malformed block, or a value that is not one of the recognised priority strings, resolves to
    /// <see cref="MessagePriority.Normal"/> — the header block is sender-supplied and must never be able
    /// to fault the routing path that reads it, mirroring <see cref="WantsAwaitCapacity"/> and
    /// <see cref="IsExpiredFrame"/>.
    /// </remarks>
    private static MessagePriority ReadPriority(ReadOnlyMemory<byte> headerBlock)
    {
        try
        {
            return HeaderEnvelope.TryReadValue(
                    headerBlock.Span, headerBlock.Length, MessagePriorityHeaderKeys.Priority, out string? value)
                ? MessagePriorityHeaderKeys.Parse(value)
                : MessagePriority.Normal;
        }
        catch (FormatException)
        {
            return MessagePriority.Normal;
        }
    }

    /// <summary>
    /// Builds a plain <see cref="MessageType.DeliverMessage"/> frame: <c>[type][senderId(16)][body]</c>.
    /// </summary>
    private static byte[] BuildDeliverMessage(Guid senderId, ReadOnlyMemory<byte> body)
    {
        var frame = new byte[1 + 16 + body.Length];
        frame[0] = (byte)MessageType.DeliverMessage;
        senderId.TryWriteBytes(frame.AsSpan(1));
        body.CopyTo(frame.AsMemory(17));
        return frame;
    }

    /// <summary>
    /// Builds a <see cref="MessageType.DeliverMessageWithHeaders"/> frame:
    /// <c>[type][senderId(16)][headerBlockLength(2)][headerBlock][body]</c>.
    /// </summary>
    private static byte[] BuildDeliverMessageWithHeaders(
        Guid senderId, ReadOnlyMemory<byte> headerBlock, ReadOnlyMemory<byte> body)
    {
        var frame = new byte[1 + 16 + 2 + headerBlock.Length + body.Length];
        frame[0] = (byte)MessageType.DeliverMessageWithHeaders;
        senderId.TryWriteBytes(frame.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(17, 2), (ushort)headerBlock.Length);
        headerBlock.CopyTo(frame.AsMemory(19));
        body.CopyTo(frame.AsMemory(19 + headerBlock.Length));
        return frame;
    }

    private void BroadcastMessage(Guid senderId, ReadOnlyMemory<byte> messageData)
    {
        // Charged by the actual number of recipients this reaches — every other registered client —
        // on top of the per-frame fan-out frequency budget the receive loop already applied. See
        // ClientRateLimiter.TryAdmitFanOutDelivery for why a frequency budget alone does not bound the
        // amplification a broadcast causes. The lookup fails only if the sender's own connection has
        // already been torn down by the time this runs, in which case there is no budget left to
        // charge against and the broadcast is let through rather than refused for a reason that has
        // nothing to do with its own behaviour.
        if (_clients.TryGetValue(senderId, out ClientConnection? senderConnection))
        {
            int recipientCount = Math.Max(0, _clients.Count - 1);

            if (!senderConnection.RateLimiter.TryAdmitFanOutDelivery(recipientCount))
            {
                if (_rateLimitLogThrottle.ShouldLog())
                {
                    _logger.LogWarning(
                        "Client {SenderId} exceeded its fan-out delivery-volume rate limit; broadcast "
                        + "dropped",
                        senderId);
                }

                return;
            }
        }

        // Build the delivery frame once and share it across every recipient's queue. The send
        // loops only read the array, so concurrent reads of this never-mutated buffer are safe.
        int frameLength = 1 + 16 + messageData.Length;

        if (ExceedsFrameCap(frameLength))
        {
            DropOversizeFanOut(senderId, frameLength, "broadcast");
            return;
        }

        var deliveryPayload = new byte[frameLength];
        deliveryPayload[0] = (byte)MessageType.DeliverMessage;
        senderId.TryWriteBytes(deliveryPayload.AsSpan(1));
        messageData.CopyTo(deliveryPayload.AsMemory(17));

        // Recorded once per broadcast that actually reaches somebody, rather than once per recipient —
        // this counts the message the hub routed, not the number of deliveries it fanned out to — and
        // not at all when the sender is the only client connected, mirroring SendToGroup's equivalent
        // "sender is the group's only member" case. hasRecipient is discovered in the same pass as
        // delivery, rather than pre-checked via _clients.Count, so this stays a single lock-free
        // traversal of the registry. Each recipient whose queue is full is still counted separately
        // below as its own dropped message.
        bool hasRecipient = false;

        foreach (KeyValuePair<Guid, ClientConnection> entry in _clients)
        {
            if (entry.Key == senderId)
            {
                // A broadcast is not echoed back to its sender.
                continue;
            }

            hasRecipient = true;

            // BroadcastMessage has no headers-bearing counterpart, so this always lands on the normal
            // lane — unchanged from the queueing behaviour before priority lanes existed.
            if (!entry.Value.OutboundQueue.TryEnqueue(MessagePriority.Normal, deliveryPayload))
            {
                _logger.LogWarning(
                    "Outbound queue for {RecipientId} is full, broadcast from {SenderId} dropped",
                    entry.Key,
                    senderId);
                _messagesDroppedCounter.Add(1, QueueFullDropTag);

                // Event only, never a wire frame — see NotifySenderOfQueueSaturation's remarks. The
                // sender never named this recipient, so telling it which one was saturated would let
                // any client enumerate the hub's connected clients by broadcasting.
                RaiseQueueSaturated(senderId, entry.Key);
            }
        }

        if (hasRecipient)
        {
            _messagesRoutedCounter.Add(1, BroadcastDirectionTag);
            _bytesRoutedCounter.Add(messageData.Length, BroadcastDirectionTag);
        }
    }

    /// <summary>
    /// Admits a client to a group once the configured <see cref="GroupAuthoriser"/> has allowed it.
    /// </summary>
    /// <remarks>
    /// Every join goes through here, including the re-joins a client sends after reconnecting, so an
    /// authorisation decision cannot be carried across a connection or bypassed by a restore: a
    /// reconnected client is a new client id that must be authorised again on its own merits.
    /// </remarks>
    private async Task JoinGroupAsync(
        ClientConnection connection,
        string groupName,
        ReadOnlyMemory<byte> groupNameBytes,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(groupName))
        {
            return;
        }

        if (_groupAuthoriser is not null)
        {
            // Copy the name out of the inbound frame before awaiting the authoriser, because the refusal
            // below echoes these bytes and is built after that await. ITransport promises nothing about
            // how long the array it returned stays valid; both shipped transports hand back a freshly
            // allocated one per frame, so nothing reuses it today, but a pooled-buffer transport would
            // leave the refusal aliasing a buffer that had already been recycled. One array on a
            // control-plane path settles that rather than resting on an implementation detail of the
            // transports that happen to be in the box.
            //
            // Note this is stricter than AuthenticateAsync, which reads registrationData to copy the
            // credential only *after* awaiting an authentication slot. That is equally safe today and for
            // the same reason, but it is not a precedent for copying early — if a pooled-buffer transport
            // is ever added, that read needs this treatment too.
            byte[] retainedNameBytes = groupNameBytes.ToArray();

            if (!await AuthoriseGroupJoinAsync(connection, groupName, cancellationToken).ConfigureAwait(false))
            {
                // A refusal has to revoke, not merely decline to add. The client may already be a member
                // from an earlier join that was allowed — the same connection re-joining a group it is in,
                // which is legal and idempotent — and if the authoriser has since changed its mind, leaving
                // that membership in place would mean a deliberate "no" left the client still receiving the
                // group's traffic and still entitled to send to it. Removing here also keeps the two sides
                // in step: the client drops the group when it sees the refusal.
                LeaveGroup(connection, groupName);
                RefuseGroupJoin(connection, groupName, retainedNameBytes);
                return;
            }
        }

        AddToGroup(connection, groupName);
    }

    // How much of a group name is written to a log line. Group names are client-supplied and carry no
    // length cap, so a name may be most of a 1 MiB frame. The refusal paths below log at Warning and
    // Error and are reachable at will by any admitted client, so logging a name whole would let one
    // client turn a rejected join into an arbitrary volume of log output. Long enough to identify a
    // real group, short enough that the log line's size is the hub's decision rather than the client's.
    private const int MaxLoggedGroupNameLength = 64;

    /// <summary>
    /// Renders a client-supplied group name for a log line, clipping it to a bounded length so that a
    /// client cannot choose how much the hub writes.
    /// </summary>
    private static string ForLog(string groupName)
    {
        return groupName.Length <= MaxLoggedGroupNameLength
            ? groupName
            : string.Concat(groupName.AsSpan(0, MaxLoggedGroupNameLength), "… (truncated)");
    }

    /// <summary>
    /// Asks the configured group authoriser whether a client may join a group, failing closed on every
    /// outcome that is not an explicit approval.
    /// </summary>
    private async Task<bool> AuthoriseGroupJoinAsync(
        ClientConnection connection, string groupName, CancellationToken cancellationToken)
    {
        var context = new GroupJoinContext
        {
            ClientId = connection.Id,
            ClientName = connection.Name,
            GroupName = groupName,
        };

        try
        {
            // Unlike the registration authenticator, this callback runs on input from an already-admitted
            // client and is driven from that client's own receive loop, which reads nothing further from
            // it until this returns. So there is no semaphore here: the registration authenticator has one
            // because it runs on unauthenticated input, where any peer that reaches the port can drive it,
            // and that is not the position this callback is in.
            //
            // What the wait below bounds is this hub's willingness to wait, not the callback's execution.
            // A callback that outruns the timeout is abandoned and goes on running, so a client that keeps
            // asking after each refusal can leave invocations piling up behind it. Across clients the
            // ceiling is the connected client count, which is maxClients only if one was configured. An
            // authoriser that holds a resource per call must therefore bound its own concurrency; this is
            // documented on the delegate and in the README rather than guessed at with a limit here.
            ValueTask<bool> pending = _groupAuthoriser!(context, cancellationToken);

            // A decision taken synchronously — a lookup against a policy table is the common case — needs
            // no task to bound, and joins recur across a connection's life rather than happening once at
            // registration, so the fast path is worth keeping allocation-free. Anything else, including an
            // already-faulted result, goes through the bounded wait below and is handled by the filters.
            bool authorised = pending.IsCompletedSuccessfully
                ? pending.Result
                : await pending
                    .AsTask()
                    .WaitAsync(_groupAuthorisationTimeout, cancellationToken)
                    .ConfigureAwait(false);

            if (!authorised)
            {
                _logger.LogWarning(
                    "Refusing client {ClientId} ({ClientName}) membership of group {GroupName}: not authorised",
                    connection.Id,
                    connection.Name,
                    ForLog(groupName));
            }

            return authorised;
        }
        catch (TimeoutException)
        {
            _logger.LogWarning(
                "The group authoriser did not complete within {Timeout} for client {ClientName} joining "
                + "group {GroupName}; refusing the join",
                _groupAuthorisationTimeout,
                connection.Name,
                ForLog(groupName));
            return false;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // The cancellation came from inside the callback rather than from the client disconnecting or
            // the hub shutting down — a lookup against an external policy store timing out is the common
            // case. Refuse the join and say why, rather than letting it unwind into the receive loop's
            // shutdown catch and drop a live connection. Callback boundary.
            _logger.LogWarning(
                "The group authoriser was cancelled for client {ClientName} joining group {GroupName}; "
                + "refusing the join",
                connection.Name,
                ForLog(groupName));
            return false;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A throwing authoriser must refuse the join, not fault the client handler. Callback boundary.
            _logger.LogError(
                ex,
                "The group authoriser threw for client {ClientName} joining group {GroupName}; refusing the join",
                connection.Name,
                ForLog(groupName));
            return false;
        }
    }

    /// <summary>
    /// Tells a client its group join was refused, so it does not go on believing it is a member of a
    /// group it will receive nothing from and may not send to.
    /// </summary>
    private void RefuseGroupJoin(
        ClientConnection connection, string groupName, ReadOnlyMemory<byte> groupNameBytes)
    {
        // Echo the name bytes the client sent rather than re-encoding the string they were decoded from.
        // Re-encoding is not size-preserving: every byte that is not valid UTF-8 decodes to U+FFFD and
        // encodes back as three bytes, so a name of invalid bytes would triple. Group names are not
        // length-capped, so the refusal could then exceed the transport's maximum payload and throw on
        // send — which faults the send loop, and a faulted send loop is awaited during this connection's
        // teardown, abandoning the rest of it including the release of the client's slot. Echoing keeps
        // the refusal no larger than the frame that provoked it, which the transport already bounded.
        var refusal = new byte[1 + groupNameBytes.Length];
        refusal[0] = (byte)MessageType.GroupJoinRefused;
        groupNameBytes.Span.CopyTo(refusal.AsSpan(1));

        // Control frame: sent High priority so a join refusal is not stuck behind the client's own
        // application backlog.
        if (!connection.OutboundQueue.TryEnqueue(MessagePriority.High, refusal))
        {
            _logger.LogWarning(
                "Outbound queue for {ClientId} is full, group join refusal for {GroupName} dropped",
                connection.Id,
                ForLog(groupName));
        }
    }

    private void AddToGroup(ClientConnection connection, string groupName)
    {
        while (true)
        {
            Group group = _groups.GetOrAdd(groupName, static _ => new Group());
            lock (group.Lock)
            {
                if (group.Removed)
                {
                    // The group was emptied and removed between GetOrAdd and acquiring its lock;
                    // retry so a live instance is used rather than resurrecting a dead one.
                    continue;
                }

                group.Members.Add(connection.Id);
                connection.Groups.Add(groupName);
            }

            _logger.LogDebug("Client {ClientId} joined group {GroupName}", connection.Id, groupName);
            return;
        }
    }

    private void LeaveGroup(ClientConnection connection, string groupName)
    {
        if (string.IsNullOrEmpty(groupName))
        {
            return;
        }

        RemoveMemberFromGroup(connection.Id, groupName);
        connection.Groups.Remove(groupName);

        _logger.LogDebug("Client {ClientId} left group {GroupName}", connection.Id, groupName);
    }

    private void RemoveFromAllGroups(ClientConnection connection)
    {
        foreach (string groupName in connection.Groups)
        {
            RemoveMemberFromGroup(connection.Id, groupName);
        }

        connection.Groups.Clear();
    }

    private void RemoveMemberFromGroup(Guid clientId, string groupName)
    {
        if (!_groups.TryGetValue(groupName, out Group? group))
        {
            return;
        }

        lock (group.Lock)
        {
            if (group.Members.Remove(clientId) && group.Members.Count == 0)
            {
                // Last member left: mark the group removed under its lock and take it out of the
                // dictionary only if this exact instance is still mapped, so a group another thread
                // created under the same name is never dropped.
                group.Removed = true;
                _groups.TryRemove(new KeyValuePair<string, Group>(groupName, group));
            }
        }
    }

    private void SendToGroup(
        Guid senderId,
        string groupName,
        ReadOnlyMemory<byte> groupNameBytes,
        ReadOnlyMemory<byte> messageData)
    {
        if (!_groups.TryGetValue(groupName, out Group? group))
        {
            return;
        }

        // Sending to a group is a member's privilege. Membership is what a join grants — and what the
        // group authoriser decides — so a client that never joined, or was refused, must not be able to
        // inject a frame that reaches every member carrying its id. The test is a set lookup against the
        // live membership inside the lock, not a scan of the snapshot afterwards, so a sender removed
        // from the group cannot slip a message through the gap. Null means not a member: taking that
        // branch outside the lock keeps the logging call out of the critical section.
        Guid[]? recipients = null;
        lock (group.Lock)
        {
            if (group.Members.Contains(senderId))
            {
                // Snapshot membership with a plain CopyTo — no LINQ closure or enumerator in the
                // critical section — so the queues can be written without holding the lock. The sender
                // is filtered out during delivery below rather than inside the lock.
                recipients = new Guid[group.Members.Count];
                group.Members.CopyTo(recipients);
            }
        }

        if (recipients is null)
        {
            _logger.LogDebug(
                "Group message from {SenderId} to group {GroupName} dropped: the sender is not a member",
                senderId,
                ForLog(groupName));
            return;
        }

        if (recipients.Length == 1 && recipients[0] == senderId)
        {
            // The sender is the only member; nothing to deliver and no frame to build.
            return;
        }

        // Charged by the actual number of recipients this reaches — every other member — on top of the
        // per-frame fan-out frequency budget the receive loop already applied. See BroadcastMessage and
        // ClientRateLimiter.TryAdmitFanOutDelivery for the reasoning; the sender is confirmed a member
        // above, so recipients.Length always includes it and is at least 2 by this point.
        if (_clients.TryGetValue(senderId, out ClientConnection? senderConnection))
        {
            int recipientCount = recipients.Length - 1;

            if (!senderConnection.RateLimiter.TryAdmitFanOutDelivery(recipientCount))
            {
                if (_rateLimitLogThrottle.ShouldLog())
                {
                    _logger.LogWarning(
                        "Client {SenderId} exceeded its fan-out delivery-volume rate limit; group "
                        + "message dropped",
                        senderId);
                }

                return;
            }
        }

        // One shared, never-mutated delivery frame across every recipient's queue (see
        // BroadcastMessage). The frame carries the group name so recipients know its origin. The
        // name bytes are copied straight from the inbound frame rather than re-encoding the string.
        int nameLength = groupNameBytes.Length;
        int frameLength = 1 + 16 + 2 + nameLength + messageData.Length;

        if (ExceedsFrameCap(frameLength))
        {
            DropOversizeFanOut(senderId, frameLength, "group message");
            return;
        }

        var deliveryPayload = new byte[frameLength];
        deliveryPayload[0] = (byte)MessageType.DeliverGroupMessage;
        senderId.TryWriteBytes(deliveryPayload.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(deliveryPayload.AsSpan(17, 2), (ushort)nameLength);
        groupNameBytes.Span.CopyTo(deliveryPayload.AsSpan(19));
        messageData.CopyTo(deliveryPayload.AsMemory(19 + nameLength));

        // Recorded once per group send rather than once per member, mirroring BroadcastMessage: this
        // counts the message the hub routed, not the number of deliveries it fanned out to. Each member
        // whose queue is full is still counted separately below as its own dropped message.
        _messagesRoutedCounter.Add(1, GroupDirectionTag);
        _bytesRoutedCounter.Add(messageData.Length, GroupDirectionTag);

        foreach (Guid recipientId in recipients)
        {
            if (recipientId == senderId)
            {
                // A group message is not echoed back to its sender.
                continue;
            }

            // SendToGroup has no headers-bearing counterpart, so this always lands on the normal lane —
            // unchanged from the queueing behaviour before priority lanes existed.
            if (_clients.TryGetValue(recipientId, out ClientConnection? recipient)
                && !recipient.OutboundQueue.TryEnqueue(MessagePriority.Normal, deliveryPayload))
            {
                _logger.LogWarning(
                    "Outbound queue for {RecipientId} is full, group message from {SenderId} dropped",
                    recipientId,
                    senderId);
                _messagesDroppedCounter.Add(1, QueueFullDropTag);

                // Event only, never a wire frame — see NotifySenderOfQueueSaturation's remarks.
                RaiseQueueSaturated(senderId, recipientId);
            }
        }
    }

    /// <summary>
    /// Fans a header-bearing group message out to every other member, mirroring <see cref="SendToGroup"/>.
    /// </summary>
    /// <remarks>
    /// Two shared frames are built at most — one with the header block, one without — regardless of how
    /// many members the group has, and each recipient's outbound queue is given whichever shape its own
    /// negotiated protocol version can understand. A recipient negotiated below
    /// <see cref="Protocol.HeaderEnvelopeMinVersion"/> gets the header-stripped frame rather than one
    /// carrying an opcode it does not recognise. Neither frame is built at all if no member can use it.
    /// </remarks>
    private void SendToGroupWithHeaders(
        Guid senderId,
        string groupName,
        ReadOnlyMemory<byte> groupNameBytes,
        ReadOnlyMemory<byte> headerBlock,
        ReadOnlyMemory<byte> body)
    {
        if (!_groups.TryGetValue(groupName, out Group? group))
        {
            return;
        }

        Guid[]? recipients = null;
        lock (group.Lock)
        {
            if (group.Members.Contains(senderId))
            {
                recipients = new Guid[group.Members.Count];
                group.Members.CopyTo(recipients);
            }
        }

        if (recipients is null)
        {
            _logger.LogDebug(
                "Group message from {SenderId} to group {GroupName} dropped: the sender is not a member",
                senderId,
                ForLog(groupName));
            return;
        }

        if (recipients.Length == 1 && recipients[0] == senderId)
        {
            // The sender is the only member; nothing to deliver and no frame to build.
            return;
        }

        // Charged by the actual number of recipients this reaches, exactly as SendToGroup charges its
        // own fan-out — a header-carrying group send reaches the same members and costs the hub the
        // same deliveries, so it must spend the same budget. See ClientRateLimiter.TryAdmitFanOutDelivery
        // for why the frequency budget alone does not bound the amplification. The sender is confirmed a
        // member above, so recipients.Length always includes it and is at least 2 by this point.
        if (_clients.TryGetValue(senderId, out ClientConnection? senderConnection))
        {
            int recipientCount = recipients.Length - 1;

            if (!senderConnection.RateLimiter.TryAdmitFanOutDelivery(recipientCount))
            {
                if (_rateLimitLogThrottle.ShouldLog())
                {
                    _logger.LogWarning(
                        "Client {SenderId} exceeded its fan-out delivery-volume rate limit; group "
                        + "message dropped",
                        senderId);
                }

                return;
            }
        }

        // Judged against the largest frame this fan-out can build — the header-carrying one — so that
        // whether a group send is accepted does not depend on which members happen to be connected on
        // which protocol version. A rule the sender cannot predict is worse than a stricter one.
        int frameLength = 1 + 16 + 2 + groupNameBytes.Length + 2 + headerBlock.Length + body.Length;

        if (ExceedsFrameCap(frameLength))
        {
            DropOversizeFanOut(senderId, frameLength, "group message");
            return;
        }

        _messagesRoutedCounter.Add(1, GroupDirectionTag);
        _bytesRoutedCounter.Add(body.Length, GroupDirectionTag);

        // One priority for the whole fan-out: the header block describes the sender's single group
        // send, not any one recipient, so every member's copy of it is queued at the same priority.
        MessagePriority priority = ReadPriority(headerBlock);

        byte[]? withHeadersFrame = null;
        byte[]? strippedFrame = null;

        foreach (Guid recipientId in recipients)
        {
            if (recipientId == senderId)
            {
                // A group message is not echoed back to its sender.
                continue;
            }

            if (!_clients.TryGetValue(recipientId, out ClientConnection? recipient))
            {
                continue;
            }

            byte[] deliveryPayload = recipient.NegotiatedProtocolVersion >= Protocol.HeaderEnvelopeMinVersion
                ? withHeadersFrame ??= BuildDeliverGroupMessageWithHeaders(senderId, groupNameBytes, headerBlock, body)
                : strippedFrame ??= BuildDeliverGroupMessage(senderId, groupNameBytes, body);

            if (!recipient.OutboundQueue.TryEnqueue(priority, deliveryPayload))
            {
                _logger.LogWarning(
                    "Outbound queue for {RecipientId} is full, group message from {SenderId} dropped",
                    recipientId,
                    senderId);
                _messagesDroppedCounter.Add(1, QueueFullDropTag);

                // Event only, never a wire frame — see NotifySenderOfQueueSaturation's remarks.
                RaiseQueueSaturated(senderId, recipientId);
            }
        }
    }

    /// <summary>
    /// Whether a delivery frame the hub is about to build is larger than the transport will write.
    /// </summary>
    /// <remarks>
    /// A fan-out frame is bigger than the inbound frame that produced it. A direct send is size-neutral
    /// — the recipient id is replaced by the sender id — but a broadcast or group delivery
    /// <i>prepends</i> the 16-byte sender id with no field to give back, so a body that the sending
    /// client's own validation accepted, and that the receive side accepted right up to
    /// <see cref="StreamFramer.MaxPayloadSize"/>, can still produce a delivery frame over the cap.
    /// <para>
    /// Building it anyway costs the recipients, not the sender: the oversize write fails inside
    /// <c>SendLoopAsync</c>, which treats a write failure as a transport fault and cancels that
    /// client's own token — so one sender's message disconnects every client it was being delivered to.
    /// Refusing here drops the single message instead, and leaves the transport-fault catch as the
    /// belt-and-braces guard it was meant to be rather than the live mechanism.
    /// </para>
    /// </remarks>
    private static bool ExceedsFrameCap(int frameLength)
    {
        return frameLength > StreamFramer.MaxPayloadSize;
    }

    /// <summary>
    /// Records a fan-out refused because its delivery frame would not fit in a transport frame.
    /// </summary>
    private void DropOversizeFanOut(Guid senderId, int frameLength, string shape)
    {
        _logger.LogWarning(
            "Fan-out {Shape} from {SenderId} dropped: its delivery frame would be {FrameLength} bytes, "
            + "over the {MaxFrameLength}-byte maximum. A fan-out frame is larger than the frame the "
            + "sender sent, so a body just inside the cap can still exceed it on delivery",
            shape,
            senderId,
            frameLength,
            StreamFramer.MaxPayloadSize);

        _messagesDroppedCounter.Add(1, FrameTooLargeDropTag);
    }

    /// <summary>
    /// Builds a plain <see cref="MessageType.DeliverGroupMessage"/> frame:
    /// <c>[type][senderId(16)][groupNameLength(2)][groupName][body]</c>.
    /// </summary>
    private static byte[] BuildDeliverGroupMessage(
        Guid senderId, ReadOnlyMemory<byte> groupNameBytes, ReadOnlyMemory<byte> body)
    {
        int nameLength = groupNameBytes.Length;
        var frame = new byte[1 + 16 + 2 + nameLength + body.Length];
        frame[0] = (byte)MessageType.DeliverGroupMessage;
        senderId.TryWriteBytes(frame.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(17, 2), (ushort)nameLength);
        groupNameBytes.Span.CopyTo(frame.AsSpan(19));
        body.CopyTo(frame.AsMemory(19 + nameLength));
        return frame;
    }

    /// <summary>
    /// Builds a <see cref="MessageType.DeliverGroupMessageWithHeaders"/> frame:
    /// <c>[type][senderId(16)][groupNameLength(2)][groupName][headerBlockLength(2)][headerBlock][body]</c>.
    /// </summary>
    private static byte[] BuildDeliverGroupMessageWithHeaders(
        Guid senderId,
        ReadOnlyMemory<byte> groupNameBytes,
        ReadOnlyMemory<byte> headerBlock,
        ReadOnlyMemory<byte> body)
    {
        int nameLength = groupNameBytes.Length;
        var frame = new byte[1 + 16 + 2 + nameLength + 2 + headerBlock.Length + body.Length];
        frame[0] = (byte)MessageType.DeliverGroupMessageWithHeaders;
        senderId.TryWriteBytes(frame.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(17, 2), (ushort)nameLength);
        groupNameBytes.Span.CopyTo(frame.AsSpan(19));

        int headerLengthOffset = 19 + nameLength;
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(headerLengthOffset, 2), (ushort)headerBlock.Length);
        headerBlock.CopyTo(frame.AsMemory(headerLengthOffset + 2));
        body.CopyTo(frame.AsMemory(headerLengthOffset + 2 + headerBlock.Length));
        return frame;
    }

    // A single group's membership, guarded by its own lock. Removed is set true under Lock when the
    // group is taken out of _groups so a concurrent join that already fetched this instance retries
    // against a fresh one rather than resurrecting a dead group.
    private sealed class Group
    {
        public Lock Lock { get; } = new();
        public HashSet<Guid> Members { get; } = new();
        public bool Removed { get; set; }
    }

    private sealed class ClientConnection(
        Guid id,
        string name,
        ITransport transport,
        byte negotiatedProtocolVersion,
        ClientRateLimiter rateLimiter)
        : IAsyncDisposable
    {
        // Internal rather than private so MeshHub.OutboundQueueCapacityForTesting can expose it to a
        // test — a private member of a nested type is not accessible from its enclosing type in C#.
        internal const int OutboundQueueCapacity = 1024;
        private int _disposed;
        private long _activitySequence;
        private int _awaitingCapacityDepth;

        /// <summary>
        /// The id this connection is registered under. Assigned fresh at registration and, for a
        /// connection that goes on to resume a previous session, replaced once by the reclaimed id —
        /// see <see cref="Rebind"/>.
        /// </summary>
        public Guid Id { get; private set; } = id;

        public string Name { get; } = name;
        public ITransport Transport { get; } = transport;

        /// <summary>
        /// The hash of the resumption token currently outstanding for this connection, or
        /// <see langword="null"/> when session resumption is off or this connection negotiated below
        /// <see cref="Protocol.SessionResumptionMinVersion"/>. Replaced when a resume issues a fresh
        /// token, and read at teardown to find the session to make dormant.
        /// </summary>
        public string? SessionTokenHash { get; set; }

        /// <summary>
        /// Takes over a reclaimed id when this connection resumes a previous session.
        /// </summary>
        /// <remarks>
        /// Mutable for this one purpose only, and only ever from the connection's own receive loop while
        /// it is dispatching the resume — nothing else writes it, and nothing reads it concurrently
        /// except routing, which is looking the connection up by whichever id it holds and reaches the
        /// same object either way.
        /// </remarks>
        public void Rebind(Guid resumedId)
        {
            Id = resumedId;
        }

        /// <summary>
        /// The protocol version negotiated with this client during registration. Used to decide whether
        /// a header-bearing delivery frame can be forwarded to it unchanged, or must have its header
        /// block stripped because this connection predates <see cref="Protocol.HeaderEnvelopeMinVersion"/>.
        /// </summary>
        public byte NegotiatedProtocolVersion { get; } = negotiatedProtocolVersion;

        /// <summary>
        /// Bounds this client's inbound frame rate and volume. Only ever consulted from this
        /// connection's own receive loop, so — like <see cref="Groups"/> — it needs no lock.
        /// </summary>
        public ClientRateLimiter RateLimiter { get; } = rateLimiter;

        /// <summary>
        /// A monotonically increasing counter bumped once for every frame received from the client.
        /// The heartbeat monitor compares it between ticks to detect an idle connection without
        /// arming a timer per received frame.
        /// </summary>
        public long ActivitySequence => Volatile.Read(ref _activitySequence);

        public void RecordActivity()
        {
            Interlocked.Increment(ref _activitySequence);
        }

        /// <summary>
        /// Whether this connection's receive loop is currently parked awaiting capacity on some other
        /// client's saturated outbound queue, rather than waiting on the client itself to send something.
        /// </summary>
        /// <remarks>
        /// The heartbeat monitor treats a parked connection as alive. While the loop is parked it reads
        /// nothing, so the client looks idle however healthy it is — and it cannot answer a ping the loop
        /// is not there to read. Evicting it would punish a client for backpressure the hub itself chose
        /// to apply, and behind a reconnector that becomes a reconnect loop. This is the same hazard the
        /// constructor already warns about for a slow <see cref="GroupAuthoriser"/>, but here the hub
        /// knows precisely when the loop is parked and for how long, so it can be exact rather than
        /// merely warn. Incremented and decremented rather than set to a flag, so nested or repeated
        /// parks on the same connection cannot have one unpark clear another's.
        /// </remarks>
        public bool IsAwaitingCapacity => Volatile.Read(ref _awaitingCapacityDepth) > 0;

        /// <summary>
        /// Marks the receive loop parked for the lifetime of the returned scope.
        /// </summary>
        public CapacityWaitScope BeginAwaitingCapacity()
        {
            Interlocked.Increment(ref _awaitingCapacityDepth);
            return new CapacityWaitScope(this);
        }

        private void EndAwaitingCapacity()
        {
            Interlocked.Decrement(ref _awaitingCapacityDepth);
        }

        /// <summary>
        /// Unparks the connection when disposed, so the park is released on every path out of the wait —
        /// including cancellation and the timeout fallback.
        /// </summary>
        internal readonly struct CapacityWaitScope(ClientConnection connection) : IDisposable
        {
            public void Dispose()
            {
                connection.EndAwaitingCapacity();
            }
        }

        /// <summary>
        /// The set of groups this client has joined. Only ever touched by this connection's own
        /// receive loop and its teardown (which runs after the loop ends), so it needs no lock.
        /// </summary>
        public HashSet<string> Groups { get; } = new(StringComparer.Ordinal);

        public PriorityOutboundQueue OutboundQueue { get; } = new(OutboundQueueCapacity);

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                OutboundQueue.Complete();
                await Transport.DisposeAsync().ConfigureAwait(false);
                OutboundQueue.Dispose();
            }
        }
    }
}
