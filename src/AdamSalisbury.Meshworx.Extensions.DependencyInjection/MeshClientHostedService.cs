using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection;

/// <summary>
/// Connects the keyed <see cref="IMeshClient"/> registered for <paramref name="clientName"/> when the host
/// starts, and disconnects it when the host begins a graceful shutdown.
/// </summary>
/// <remarks>
/// Registered by <c>AddMeshClient</c>, never constructed directly by an integrator. Whether the connection
/// is a plain <see cref="MeshClient"/> or one managed by a <see cref="MeshClientReconnector"/> is decided
/// from <see cref="MeshClientOptions.UseReconnector"/> at start time, since the option may be bound from
/// configuration and is not known until the options pipeline resolves it.
/// <para>
/// A reconnector-backed client is started via <see cref="MeshClientReconnector.StartAsync"/> so its
/// disconnect monitoring is wired up correctly, and is left to the dependency-injection container's own
/// disposal of the reconnector's singleton — which disconnects gracefully — rather than being disconnected
/// again here on stop.
/// </para>
/// </remarks>
internal sealed class MeshClientHostedService(
    string clientName,
    IServiceProvider serviceProvider,
    IOptionsMonitor<MeshClientOptions> optionsMonitor) : IHostedService
{
    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        MeshClientOptions options = optionsMonitor.Get(clientName);

        return options.UseReconnector
            ? serviceProvider.GetRequiredKeyedService<MeshClientReconnector>(clientName).StartAsync(cancellationToken)
            : ConnectAsync(options, cancellationToken);
    }

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        MeshClientOptions options = optionsMonitor.Get(clientName);

        if (options.UseReconnector)
        {
            return Task.CompletedTask;
        }

        IMeshClient client = serviceProvider.GetRequiredKeyedService<IMeshClient>(clientName);
        return client.DisconnectAsync(cancellationToken);
    }

    private async Task ConnectAsync(MeshClientOptions options, CancellationToken cancellationToken)
    {
        IMeshClient client = serviceProvider.GetRequiredKeyedService<IMeshClient>(clientName);
        Func<CancellationToken, Task<ITransport>> transportFactory =
            options.TransportFactory ?? (ct => ConnectDefaultTransportAsync(options, ct));

        ITransport transport = await transportFactory(cancellationToken).ConfigureAwait(false);
        await client.ConnectAsync(transport, options.ClientName, options.Credential, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<ITransport> ConnectDefaultTransportAsync(
        MeshClientOptions options, CancellationToken cancellationToken)
    {
        return await TcpTransport.ConnectAsync(options.Host, options.Port, cancellationToken).ConfigureAwait(false);
    }
}
