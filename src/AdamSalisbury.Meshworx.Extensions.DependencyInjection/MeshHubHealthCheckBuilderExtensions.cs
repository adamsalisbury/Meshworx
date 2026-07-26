using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for registering a health check for a <see cref="IMeshHub"/> on
/// <see cref="IHealthChecksBuilder"/>.
/// </summary>
public static class MeshHubHealthCheckBuilderExtensions
{
    private const string DefaultName = "meshhub";

    /// <summary>
    /// Adds a health check for the <see cref="IMeshHub"/> registered by <c>AddMeshHub</c>.
    /// </summary>
    /// <param name="builder">The health checks builder to add the check to.</param>
    /// <param name="name">The health check name. Defaults to <c>"meshhub"</c>.</param>
    /// <param name="failureStatus">
    /// The <see cref="HealthStatus"/> reported when the check fails. Defaults to
    /// <see cref="HealthStatus.Unhealthy"/>.
    /// </param>
    /// <param name="tags">A list of tags used to filter health checks.</param>
    /// <returns>The health checks builder, for chaining.</returns>
    /// <remarks>
    /// Requires an <see cref="IMeshHub"/> to already be registered — call <c>AddMeshHub</c> first.
    /// </remarks>
    public static IHealthChecksBuilder AddMeshHub(
        this IHealthChecksBuilder builder,
        string name = DefaultName,
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(name);

        return builder.Add(new HealthCheckRegistration(
            name,
            serviceProvider => new MeshHubHealthCheck(serviceProvider.GetRequiredService<IMeshHub>()),
            failureStatus,
            tags));
    }
}
