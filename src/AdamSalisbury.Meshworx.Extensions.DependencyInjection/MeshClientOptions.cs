using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection;

/// <summary>
/// Configures the <see cref="MeshClient"/> registered by
/// <see cref="Microsoft.Extensions.DependencyInjection.MeshClientServiceCollectionExtensions"/>'s
/// <c>AddMeshClient</c> extension methods.
/// </summary>
/// <remarks>
/// Every property mirrors a <see cref="MeshClient"/> or <see cref="MeshClientReconnector"/> constructor
/// parameter and carries the same default: leave a property unset and the client is built exactly as it
/// would be from a bare <c>new MeshClient(logger)</c> call. See the <c>MeshClient</c> configuration
/// section of the project README for what each default means in practice.
/// </remarks>
public sealed class MeshClientOptions
{
    /// <summary>
    /// Gets or sets the name the client registers with the hub under.
    /// </summary>
    /// <remarks>
    /// Set by <c>AddMeshClient</c> to the name it was called with; a value bound from configuration or
    /// set in a configure delegate is overridden so the registered name always matches the service key.
    /// </remarks>
    public string ClientName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the host the client's default transport connects to.
    /// </summary>
    /// <remarks>Ignored when <see cref="TransportFactory"/> is set.</remarks>
    public string Host { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the port the client's default transport connects to.
    /// </summary>
    /// <remarks>Ignored when <see cref="TransportFactory"/> is set.</remarks>
    public int Port { get; set; } = 22001;

    /// <summary>
    /// Gets or sets the factory used to create the transport for each connection attempt.
    /// </summary>
    /// <remarks>
    /// Leave unset to connect over TCP to <see cref="Host"/>/<see cref="Port"/> via
    /// <see cref="TcpTransport.ConnectAsync(string, int, System.Threading.CancellationToken)"/>. Set
    /// this to use TLS, a non-default transport, or any other connection logic.
    /// </remarks>
    public Func<CancellationToken, Task<ITransport>>? TransportFactory { get; set; }

    /// <summary>
    /// Gets or sets the opaque credential presented to the hub's authenticator on every connection attempt.
    /// </summary>
    /// <remarks>
    /// Declared as <see cref="byte"/>[] rather than <see cref="ReadOnlyMemory{T}"/> deliberately: the
    /// configuration binder has no converter for <see cref="ReadOnlyMemory{T}"/> and would silently leave
    /// it empty rather than fail, so a credential bound from a base64 configuration value (e.g. from a
    /// mounted secret) would be dropped without error. <c>byte[]</c> is the type the binder actually
    /// supports for that case. Left <see langword="null"/>, the client presents no credential.
    /// </remarks>
    public byte[]? Credential { get; set; }

    /// <summary>
    /// Gets or sets how long the client tolerates silence from the hub before treating the connection as lost.
    /// </summary>
    public TimeSpan? IdleTimeout { get; set; }

    /// <summary>
    /// Gets or sets how long a send may take before it is cancelled and surfaced as a <see cref="TimeoutException"/>.
    /// </summary>
    public TimeSpan? SendTimeout { get; set; }

    /// <summary>
    /// Gets or sets how many attempts a send makes when it fails with a transient transport I/O error.
    /// </summary>
    public int MaxSendAttempts { get; set; } = 1;

    /// <summary>
    /// Gets or sets the base delay between send retries, scaled linearly per attempt.
    /// </summary>
    public TimeSpan? SendRetryDelay { get; set; }

    /// <summary>
    /// Gets or sets whether the client is wrapped in a <see cref="MeshClientReconnector"/> that
    /// transparently re-establishes the connection when it is lost.
    /// </summary>
    public bool UseReconnector { get; set; }

    /// <summary>
    /// Gets or sets how long the reconnector waits between failed reconnect attempts.
    /// </summary>
    /// <remarks>Only used when <see cref="UseReconnector"/> is <see langword="true"/>.</remarks>
    public TimeSpan? ReconnectRetryDelay { get; set; }

    /// <summary>
    /// Gets or sets the maximum time a single reconnect attempt may take.
    /// </summary>
    /// <remarks>Only used when <see cref="UseReconnector"/> is <see langword="true"/>.</remarks>
    public TimeSpan? ReconnectConnectTimeout { get; set; }

    /// <summary>
    /// Gets or sets whether the reconnector automatically re-joins the client's previous groups after a reconnect.
    /// </summary>
    /// <remarks>Only used when <see cref="UseReconnector"/> is <see langword="true"/>. Defaults to <see langword="true"/>.</remarks>
    public bool RestoreGroupMembership { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum total size, in bytes, the client holds while reassembling a chunked
    /// message from an unauthenticated or partially-trusted peer.
    /// </summary>
    public int? MaxReassemblyBytes { get; set; }

    /// <summary>
    /// Gets or sets how long the client waits for the remaining chunks of a message before abandoning
    /// the reassembly and freeing what it was holding.
    /// </summary>
    public TimeSpan? ChunkTransferTimeout { get; set; }

    /// <summary>
    /// Gets or sets how long a single attempt at the client's initial connection, made when the host
    /// starts, may take before it is abandoned and retried.
    /// </summary>
    /// <remarks>
    /// Applies to the plain connection path only — a reconnector-backed client (<see cref="UseReconnector"/>)
    /// bounds its own first attempt with <see cref="ReconnectConnectTimeout"/> instead, since that value
    /// already exists for exactly this purpose on that path. Defaults to 10 seconds.
    /// </remarks>
    public TimeSpan? ConnectTimeout { get; set; }

    /// <summary>
    /// Gets or sets how long the hosted service that connects this client at host startup waits between
    /// failed attempts at the initial connection, before the client has connected even once.
    /// </summary>
    /// <remarks>
    /// Used on both the plain and reconnector-backed paths alike: a hub that has not finished starting
    /// yet — very often true in a real deployment, where the hub is usually a separate process — should
    /// not cause the client's host to fail to start, so the initial connection is retried under the
    /// host's own start token rather than given up on after one attempt. This is distinct from
    /// <see cref="ReconnectRetryDelay"/>, which governs the reconnector's retries after a connection that
    /// was already established is later lost. Defaults to 1 second.
    /// </remarks>
    public TimeSpan? ConnectRetryDelay { get; set; }
}
