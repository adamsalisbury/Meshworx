using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Extensions.DependencyInjection;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for registering a <see cref="MeshHub"/> with an <see cref="IServiceCollection"/>.
/// </summary>
public static class MeshHubServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="MeshHub"/> as a singleton <see cref="IMeshHub"/> and hosts it alongside the
    /// application.
    /// </summary>
    /// <param name="services">The service collection to add the hub to.</param>
    /// <param name="configureOptions">Configures the hub.</param>
    /// <returns>The service collection, for chaining.</returns>
    /// <remarks>
    /// The registered <see cref="Microsoft.Extensions.Hosting.IHostedService"/> calls
    /// <see cref="IMeshHub.StartAsync"/> when the host starts and <see cref="IMeshHub.StopAsync"/> when it
    /// begins a graceful shutdown, draining connected clients before the process exits. Requires an
    /// <see cref="ILogger{TCategoryName}"/> to be resolvable — call <c>AddLogging</c> first if the host does
    /// not already provide one. Only the first call on a given collection registers the hub and its hosted
    /// service; a later call still layers its <paramref name="configureOptions"/> onto the same options
    /// pipeline.
    /// <para>
    /// Call this <em>before</em> <c>AddMeshClient</c> when the same host also runs one of this hub's own
    /// clients: <see cref="Microsoft.Extensions.Hosting.IHostedService"/> instances start one at a time, in
    /// registration order, so a client registered first would otherwise start connecting before this
    /// hub's listener has started accepting.
    /// </para>
    /// </remarks>
    public static IServiceCollection AddMeshHub(
        this IServiceCollection services,
        Action<MeshHubOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        return services.AddMeshHubCore(configuration: null, configureOptions);
    }

    /// <summary>
    /// Registers a <see cref="MeshHub"/> as a singleton <see cref="IMeshHub"/>, binding its options from
    /// configuration, and hosts it alongside the application as described on the other
    /// <see cref="AddMeshHub(IServiceCollection, Action{MeshHubOptions})"/> overload.
    /// </summary>
    /// <param name="services">The service collection to add the hub to.</param>
    /// <param name="configuration">
    /// The configuration section to bind <see cref="MeshHubOptions"/> from, e.g.
    /// <c>builder.Configuration.GetSection("MeshHub")</c>.
    /// </param>
    /// <param name="configureOptions">
    /// Configures the hub. Applied after binding, so a value set here overrides the bound one.
    /// </param>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddMeshHub(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<MeshHubOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        return services.AddMeshHubCore(configuration, configureOptions);
    }

    private static IServiceCollection AddMeshHubCore(
        this IServiceCollection services,
        IConfiguration? configuration,
        Action<MeshHubOptions>? configureOptions)
    {
        OptionsBuilder<MeshHubOptions> optionsBuilder = services.AddOptions<MeshHubOptions>();

        if (configuration is not null)
        {
            optionsBuilder.Bind(configuration);
        }

        if (configureOptions is not null)
        {
            optionsBuilder.Configure(configureOptions);
        }

        optionsBuilder
            .Validate(options => options.Port is > 0 and <= 65535, "Port must be between 1 and 65535.")
            .ValidateOnStart();

        services.TryAddSingleton<IMeshHub>(CreateHub);
        services.AddHostedService<MeshHubHostedService>();

        return services;
    }

    private static MeshHub CreateHub(IServiceProvider serviceProvider)
    {
        MeshHubOptions options = serviceProvider.GetRequiredService<IOptions<MeshHubOptions>>().Value;
        var logger = serviceProvider.GetRequiredService<ILogger<MeshHub>>();
        ITransportListener listener = options.Listener ?? new TcpTransportListener(options.Port);

        // An explicitly configured store takes priority over one resolved from the container, matching
        // how Listener above overrides the built-in TcpTransportListener rather than being layered with
        // it. Falls back to whatever IOfflineStore the container has, if any — the natural shape for a
        // store that is itself a service with its own dependencies, rather than a value the caller sets
        // directly on the options.
        IOfflineStore? offlineStore = options.OfflineStore ?? serviceProvider.GetService<IOfflineStore>();

        return new MeshHub(
            logger,
            listener,
            options.RegistrationTimeout,
            options.MaxClients,
            options.HeartbeatInterval,
            options.MaxMissedHeartbeats,
            options.Authenticator,
            options.MaxConcurrentAuthentications,
            options.GroupAuthoriser,
            options.GroupAuthorisationTimeout,
            options.MaxConnectionsPerRemoteEndpoint,
            options.NotifyOnQueueSaturation,
            options.BackpressureAwaitTimeout,
            offlineStore,
            options.OfflineStoreTimeout,
            options.SessionResumptionWindow,
            options.MaxInboundMessagesPerSecond,
            options.MaxInboundBytesPerSecond,
            options.MaxFanOutMessagesPerSecond,
            options.MaxFanOutDeliveriesPerSecond,
            options.EnablePresence,
            options.HubId,
            options.AllowIncomingPeerLinks,
            options.PeerAuthenticator,
            options.Backplane,
            options.BackplaneTimeout);
    }
}
