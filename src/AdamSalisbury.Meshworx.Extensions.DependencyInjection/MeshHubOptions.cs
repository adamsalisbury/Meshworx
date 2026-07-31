using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection;

/// <summary>
/// Configures the <see cref="MeshHub"/> registered by
/// <see cref="Microsoft.Extensions.DependencyInjection.MeshHubServiceCollectionExtensions"/>'s
/// <c>AddMeshHub</c> extension methods.
/// </summary>
/// <remarks>
/// Every property mirrors a <see cref="MeshHub"/> constructor parameter and carries the same default:
/// leave a property unset and the hub is built exactly as it would be from a bare
/// <c>new MeshHub(logger, listener)</c> call. See the <c>MeshHub</c> configuration section of the
/// project README for what each default means in practice.
/// </remarks>
public sealed class MeshHubOptions
{
    /// <summary>
    /// Gets or sets the loopback port the hub's default <see cref="TcpTransportListener"/> binds to.
    /// </summary>
    /// <remarks>
    /// Ignored when <see cref="Listener"/> is set — that listener is used as-is instead. Must be
    /// between 1 and 65535.
    /// </remarks>
    public int Port { get; set; } = 22001;

    /// <summary>
    /// Gets or sets the transport listener the hub accepts connections on.
    /// </summary>
    /// <remarks>
    /// Leave unset to have a <see cref="TcpTransportListener"/> built from <see cref="Port"/>. Set this
    /// to supply a listener with TLS configured, a non-loopback bind address, or an entirely different
    /// <see cref="ITransportListener"/> implementation such as the in-memory one.
    /// </remarks>
    public ITransportListener? Listener { get; set; }

    /// <summary>
    /// Gets or sets how long a connection may stay unregistered before the hub drops it.
    /// </summary>
    public TimeSpan? RegistrationTimeout { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of registered clients the hub admits.
    /// </summary>
    public int? MaxClients { get; set; }

    /// <summary>
    /// Gets or sets how often the hub pings an idle client to detect a half-open connection.
    /// </summary>
    public TimeSpan? HeartbeatInterval { get; set; }

    /// <summary>
    /// Gets or sets how many consecutive silent heartbeat intervals a client may go through before eviction.
    /// </summary>
    public int MaxMissedHeartbeats { get; set; } = 2;

    /// <summary>
    /// Gets or sets the callback that decides whether a client may register with the hub.
    /// </summary>
    public ClientAuthenticator? Authenticator { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of concurrent <see cref="Authenticator"/> invocations the hub allows.
    /// </summary>
    public int? MaxConcurrentAuthentications { get; set; }

    /// <summary>
    /// Gets or sets the callback that decides whether a registered client may join a group.
    /// </summary>
    public GroupAuthoriser? GroupAuthoriser { get; set; }

    /// <summary>
    /// Gets or sets how long the hub waits for <see cref="GroupAuthoriser"/> before refusing the join.
    /// </summary>
    public TimeSpan? GroupAuthorisationTimeout { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of concurrent connections the hub accepts from a single remote address.
    /// </summary>
    public int? MaxConnectionsPerRemoteEndpoint { get; set; }

    /// <summary>
    /// Gets or sets whether a sender is sent a control frame when one of its messages is dropped because
    /// the recipient's outbound queue was full.
    /// </summary>
    public bool NotifyOnQueueSaturation { get; set; }

    /// <summary>
    /// Gets or sets how long the hub awaits free capacity on a saturated recipient queue for a sender
    /// that opted into <see cref="AdamSalisbury.Meshworx.DeliveryOptions.AwaitCapacity"/>.
    /// </summary>
    public TimeSpan? BackpressureAwaitTimeout { get; set; }

    /// <summary>
    /// Gets or sets the store used to hold messages addressed to a disconnected client until it
    /// reconnects.
    /// </summary>
    /// <remarks>
    /// Leave unset to have the hub resolve an <see cref="IOfflineStore"/> registered elsewhere in the
    /// container, if any — the natural shape for a store that is itself a service with its own
    /// dependencies. Set this to supply one directly instead, which takes priority over anything
    /// registered in the container. With neither, the hub holds nothing for a disconnected client.
    /// </remarks>
    public IOfflineStore? OfflineStore { get; set; }

    /// <summary>
    /// Gets or sets how long the hub waits on <see cref="OfflineStore"/> before giving up on a single call.
    /// </summary>
    public TimeSpan? OfflineStoreTimeout { get; set; }

    /// <summary>
    /// Gets or sets how long a disconnected client's session may be resumed within.
    /// </summary>
    /// <remarks>
    /// Leave unset to switch session resumption off entirely, in which case a reconnecting client is
    /// always issued a fresh identity.
    /// </remarks>
    public TimeSpan? SessionResumptionWindow { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of inbound messages the hub accepts from a single client per second.
    /// </summary>
    public int? MaxInboundMessagesPerSecond { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of inbound bytes the hub accepts from a single client per second.
    /// </summary>
    public int? MaxInboundBytesPerSecond { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of fan-out sends — broadcasts and group sends — a single client
    /// may issue per second.
    /// </summary>
    public int? MaxFanOutMessagesPerSecond { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of individual deliveries a single client's fan-out sends may
    /// produce per second, across every recipient combined.
    /// </summary>
    public int? MaxFanOutDeliveriesPerSecond { get; set; }

    /// <summary>
    /// Gets or sets whether a client may subscribe to presence. Defaults to <see langword="false"/>.
    /// </summary>
    public bool EnablePresence { get; set; }

    /// <summary>
    /// Gets or sets this hub's own identifier on a peer link. Defaults to a fresh
    /// <see cref="Guid.NewGuid"/> when unset.
    /// </summary>
    public Guid? HubId { get; set; }

    /// <summary>
    /// Gets or sets whether an incoming connection may become a peer link. Defaults to
    /// <see langword="false"/>.
    /// </summary>
    public bool AllowIncomingPeerLinks { get; set; }

    /// <summary>
    /// Gets or sets the callback that decides whether to admit an incoming peer link, once
    /// <see cref="AllowIncomingPeerLinks"/> is set.
    /// </summary>
    public PeerAuthenticator? PeerAuthenticator { get; set; }

    /// <summary>
    /// Gets or sets the shared backplane that lets several hub instances behave as one logical hub.
    /// Left unset, the single-instance path is unchanged.
    /// </summary>
    public Backplane.IHubBackplane? Backplane { get; set; }

    /// <summary>
    /// Gets or sets how long a single backplane operation is given before this hub gives up on it.
    /// Defaults to 10 seconds when unset. Ignored when <see cref="Backplane"/> is unset.
    /// </summary>
    public TimeSpan? BackplaneTimeout { get; set; }
}
