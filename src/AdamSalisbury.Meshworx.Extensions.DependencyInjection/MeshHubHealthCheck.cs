using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection;

/// <summary>
/// Reports the health of a registered <see cref="IMeshHub"/>.
/// </summary>
/// <remarks>
/// Registered by <c>AddMeshHub</c> on <see cref="IHealthChecksBuilder"/>, never constructed directly by an
/// integrator. Unhealthy while the hub is not running; degraded once <see cref="IMeshHub.ClaimedClientSlots"/>
/// has reached <see cref="IMeshHub.MaxClients"/> — the hub is still serving existing clients but refusing
/// new ones; healthy otherwise.
/// <para>
/// Resolves the <see cref="IMeshHub"/> itself, rather than taking one as a constructor dependency, so that
/// a hub that has not been registered surfaces as a caught resolution failure inside
/// <see cref="CheckHealthAsync"/> — which the health check service reports as the registration's configured
/// failure status — instead of an unhandled exception thrown from the registration's factory delegate.
/// </para>
/// </remarks>
internal sealed class MeshHubHealthCheck(IServiceProvider serviceProvider) : IHealthCheck
{
    /// <inheritdoc/>
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        IMeshHub hub = serviceProvider.GetRequiredService<IMeshHub>();

        if (!hub.IsRunning)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("The hub is not running."));
        }

        int connectedClientCount = hub.ConnectedClientCount;
        int claimedClientSlots = hub.ClaimedClientSlots;
        int maxClients = hub.MaxClients;
        var data = new Dictionary<string, object>
        {
            ["connectedClientCount"] = connectedClientCount,
            ["claimedClientSlots"] = claimedClientSlots,
            ["maxClients"] = maxClients,
        };

        // Capacity is judged against ClaimedClientSlots, not ConnectedClientCount: a slot is claimed as
        // soon as a connection is accepted and given back only once its handler has fully finished, so it
        // is what the hub's own admission check enforces maxClients against. ConnectedClientCount can lag
        // behind it while a client is mid-handshake or mid-teardown, and comparing that instead would let
        // this check report Healthy while the hub is already refusing new connections.
        return Task.FromResult(claimedClientSlots >= maxClients
            ? HealthCheckResult.Degraded(
                $"The hub is running but at capacity ({claimedClientSlots}/{maxClients} client slots claimed).",
                data: data)
            : HealthCheckResult.Healthy(
                $"The hub is running with {connectedClientCount} connected client(s).", data));
    }
}
