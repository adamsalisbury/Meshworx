using Microsoft.Extensions.Hosting;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection;

/// <summary>
/// Starts and stops the registered <see cref="IMeshHub"/> alongside the host.
/// </summary>
/// <remarks>
/// Registered by <c>AddMeshHub</c>, never constructed directly by an integrator. The hub itself is a
/// singleton, so its <see cref="IAsyncDisposable.DisposeAsync"/> is invoked by the dependency-injection
/// container when the host's root service provider is disposed; this service is only responsible for the
/// start/stop half of the lifecycle.
/// </remarks>
internal sealed class MeshHubHostedService(IMeshHub hub) : IHostedService
{
    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        return hub.StartAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return hub.StopAsync(cancellationToken);
    }
}
