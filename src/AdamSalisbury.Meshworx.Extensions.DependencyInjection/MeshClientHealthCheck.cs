using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection;

/// <summary>
/// Reports the health of a registered, named <see cref="IMeshClient"/>.
/// </summary>
/// <remarks>
/// Registered by <c>AddMeshClient</c> on <see cref="IHealthChecksBuilder"/>, never constructed directly by
/// an integrator. Healthy while <see cref="IMeshClient.IsConnected"/> is <see langword="true"/>, unhealthy
/// otherwise — including while a <see cref="MeshClientReconnector"/> is mid-retry, since the client has no
/// hub connection to route through until it succeeds.
/// <para>
/// Resolves the keyed <see cref="IMeshClient"/> itself, rather than taking one as a constructor dependency,
/// so that a client name with no registration surfaces as a caught resolution failure inside
/// <see cref="CheckHealthAsync"/> — which the health check service reports as the registration's configured
/// failure status — instead of an unhandled exception thrown from the registration's factory delegate.
/// </para>
/// </remarks>
internal sealed class MeshClientHealthCheck(IServiceProvider serviceProvider, string clientName) : IHealthCheck
{
    /// <inheritdoc/>
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        IMeshClient client = serviceProvider.GetRequiredKeyedService<IMeshClient>(clientName);

        return Task.FromResult(client.IsConnected
            ? HealthCheckResult.Healthy("The client is connected.")
            : HealthCheckResult.Unhealthy("The client is not connected."));
    }
}
