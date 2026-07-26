using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection;

/// <summary>
/// Reports the health of a registered <see cref="IMeshHub"/>.
/// </summary>
/// <remarks>
/// Registered by <c>AddMeshHub</c> on <see cref="IHealthChecksBuilder"/>, never constructed directly by an
/// integrator. Unhealthy while the hub is not running; degraded once it has reached
/// <see cref="IMeshHub.MaxClients"/>, since it is still serving existing clients but refusing new ones;
/// healthy otherwise.
/// </remarks>
internal sealed class MeshHubHealthCheck(IMeshHub hub) : IHealthCheck
{
    /// <inheritdoc/>
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!hub.IsRunning)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("The hub is not running."));
        }

        int connectedClientCount = hub.ConnectedClientCount;
        int maxClients = hub.MaxClients;
        var data = new Dictionary<string, object>
        {
            ["connectedClientCount"] = connectedClientCount,
            ["maxClients"] = maxClients,
        };

        return Task.FromResult(connectedClientCount >= maxClients
            ? HealthCheckResult.Degraded(
                $"The hub is running but at capacity ({connectedClientCount}/{maxClients} clients).", data: data)
            : HealthCheckResult.Healthy(
                $"The hub is running with {connectedClientCount} connected client(s).", data));
    }
}
