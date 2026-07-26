using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for registering a health check for a named <see cref="IMeshClient"/> on
/// <see cref="IHealthChecksBuilder"/>.
/// </summary>
public static class MeshClientHealthCheckBuilderExtensions
{
    /// <summary>
    /// Adds a health check for the keyed <see cref="IMeshClient"/> registered by <c>AddMeshClient</c> under
    /// <paramref name="clientName"/>.
    /// </summary>
    /// <param name="builder">The health checks builder to add the check to.</param>
    /// <param name="clientName">The client name the check reports on, matching the one passed to <c>AddMeshClient</c>.</param>
    /// <param name="name">The health check name. Defaults to <c>"meshclient:{clientName}"</c>.</param>
    /// <param name="failureStatus">
    /// The <see cref="HealthStatus"/> reported when the check fails. Defaults to
    /// <see cref="HealthStatus.Unhealthy"/>.
    /// </param>
    /// <param name="tags">A list of tags used to filter health checks.</param>
    /// <returns>The health checks builder, for chaining.</returns>
    /// <remarks>
    /// Requires an <see cref="IMeshClient"/> to already be registered under <paramref name="clientName"/> —
    /// call <c>AddMeshClient</c> with the same name first.
    /// </remarks>
    public static IHealthChecksBuilder AddMeshClient(
        this IHealthChecksBuilder builder,
        string clientName,
        string? name = null,
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(clientName);

        return builder.Add(new HealthCheckRegistration(
            name ?? $"meshclient:{clientName}",
            serviceProvider => new MeshClientHealthCheck(serviceProvider, clientName),
            failureStatus,
            tags));
    }
}
