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
/// Extension methods for registering a <see cref="MeshClient"/> with an <see cref="IServiceCollection"/>.
/// </summary>
public static class MeshClientServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="MeshClient"/> — optionally wrapped in a <see cref="MeshClientReconnector"/> —
    /// as a keyed <see cref="IMeshClient"/> under <paramref name="clientName"/>, and connects it alongside
    /// the application.
    /// </summary>
    /// <param name="services">The service collection to add the client to.</param>
    /// <param name="clientName">
    /// Both the name the client registers with the hub under and the key the resulting
    /// <see cref="IMeshClient"/> is resolved by, e.g. <c>serviceProvider.GetRequiredKeyedService&lt;IMeshClient&gt;(clientName)</c>.
    /// </param>
    /// <param name="configureOptions">Configures the client.</param>
    /// <returns>The service collection, for chaining.</returns>
    /// <remarks>
    /// The registered <see cref="Microsoft.Extensions.Hosting.IHostedService"/> connects the client when
    /// the host starts and disconnects it when the host begins a graceful shutdown. Set
    /// <see cref="MeshClientOptions.UseReconnector"/> to have the connection transparently re-established
    /// on an unexpected drop; the keyed <see cref="IMeshClient"/> is then the reconnector's managed client,
    /// so callers use the same API surface either way. Requires an
    /// <see cref="ILogger{TCategoryName}"/> to be resolvable — call <c>AddLogging</c> first if the host
    /// does not already provide one. Calling this again with the same <paramref name="clientName"/> only
    /// registers the client and its hosted service once; a later call still layers its
    /// <paramref name="configureOptions"/> onto the same named options pipeline.
    /// </remarks>
    public static IServiceCollection AddMeshClient(
        this IServiceCollection services,
        string clientName,
        Action<MeshClientOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrEmpty(clientName);

        return services.AddMeshClientCore(clientName, configuration: null, configureOptions);
    }

    /// <summary>
    /// Registers a <see cref="MeshClient"/> — optionally wrapped in a <see cref="MeshClientReconnector"/> —
    /// as a keyed <see cref="IMeshClient"/> under <paramref name="clientName"/>, binding its options from
    /// configuration, and connects it alongside the application as described on the other
    /// <see cref="AddMeshClient(IServiceCollection, string, Action{MeshClientOptions})"/> overload.
    /// </summary>
    /// <param name="services">The service collection to add the client to.</param>
    /// <param name="clientName">
    /// Both the name the client registers with the hub under and the key the resulting
    /// <see cref="IMeshClient"/> is resolved by.
    /// </param>
    /// <param name="configuration">
    /// The configuration section to bind <see cref="MeshClientOptions"/> from, e.g.
    /// <c>builder.Configuration.GetSection("MeshClients:Alice")</c>.
    /// </param>
    /// <param name="configureOptions">
    /// Configures the client. Applied after binding, so a value set here overrides the bound one.
    /// </param>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddMeshClient(
        this IServiceCollection services,
        string clientName,
        IConfiguration configuration,
        Action<MeshClientOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrEmpty(clientName);
        ArgumentNullException.ThrowIfNull(configuration);

        return services.AddMeshClientCore(clientName, configuration, configureOptions);
    }

    private static IServiceCollection AddMeshClientCore(
        this IServiceCollection services,
        string clientName,
        IConfiguration? configuration,
        Action<MeshClientOptions>? configureOptions)
    {
        OptionsBuilder<MeshClientOptions> optionsBuilder = services.AddOptions<MeshClientOptions>(clientName);

        if (configuration is not null)
        {
            optionsBuilder.Bind(configuration);
        }

        if (configureOptions is not null)
        {
            optionsBuilder.Configure(configureOptions);
        }

        optionsBuilder
            .PostConfigure(options => options.ClientName = clientName)
            .Validate(options => options.Port is > 0 and <= 65535, "Port must be between 1 and 65535.")
            .Validate(options => options.MaxSendAttempts >= 1, "MaxSendAttempts must be at least one.")
            .ValidateOnStart();

        services.TryAddKeyedSingleton<MeshClientReconnector>(
            clientName, (serviceProvider, key) => CreateReconnector(serviceProvider, (string)key!));

        services.TryAddKeyedSingleton<IMeshClient>(clientName, (serviceProvider, key) =>
        {
            var name = (string)key!;
            MeshClientOptions options = serviceProvider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>().Get(name);

            return options.UseReconnector
                ? serviceProvider.GetRequiredKeyedService<MeshClientReconnector>(name).Client
                : CreateClient(serviceProvider, options);
        });

        services.AddHostedService(serviceProvider => new MeshClientHostedService(
            clientName,
            serviceProvider,
            serviceProvider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>()));

        return services;
    }

    private static MeshClient CreateClient(IServiceProvider serviceProvider, MeshClientOptions options)
    {
        var logger = serviceProvider.GetRequiredService<ILogger<MeshClient>>();

        return new MeshClient(
            logger,
            options.IdleTimeout,
            options.SendTimeout,
            options.MaxSendAttempts,
            options.SendRetryDelay);
    }

    private static MeshClientReconnector CreateReconnector(IServiceProvider serviceProvider, string clientName)
    {
        MeshClientOptions options =
            serviceProvider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>().Get(clientName);
        MeshClient client = CreateClient(serviceProvider, options);
        var reconnectorLogger = serviceProvider.GetService<ILogger<MeshClientReconnector>>();
        Func<CancellationToken, Task<ITransport>> transportFactory =
            options.TransportFactory ?? (ct => ConnectDefaultTransportAsync(options, ct));

        return new MeshClientReconnector(
            client,
            options.ClientName,
            transportFactory,
            options.ReconnectRetryDelay,
            options.ReconnectConnectTimeout,
            options.RestoreGroupMembership,
            reconnectorLogger,
            options.Credential);
    }

    private static async Task<ITransport> ConnectDefaultTransportAsync(
        MeshClientOptions options, CancellationToken cancellationToken)
    {
        return await TcpTransport.ConnectAsync(options.Host, options.Port, cancellationToken).ConfigureAwait(false);
    }
}
