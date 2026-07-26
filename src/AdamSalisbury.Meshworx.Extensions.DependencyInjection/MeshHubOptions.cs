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
}
