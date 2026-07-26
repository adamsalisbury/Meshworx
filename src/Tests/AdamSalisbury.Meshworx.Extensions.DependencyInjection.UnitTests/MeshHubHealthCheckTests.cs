using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshHubHealthCheckTests
{
    private static HealthCheckContext CreateContext(MeshHubHealthCheck sut)
    {
        return new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("meshhub", sut, failureStatus: null, tags: null),
        };
    }

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_HubNotRunning_ReturnsUnhealthy()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(false);
        var sut = new MeshHubHealthCheck(hub.Object);

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_HubRunningBelowCapacity_ReturnsHealthy()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(true);
        hub.Setup(h => h.ConnectedClientCount).Returns(3);
        hub.Setup(h => h.MaxClients).Returns(1000);
        var sut = new MeshHubHealthCheck(hub.Object);

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_HubRunningAtCapacity_ReturnsDegraded()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(true);
        hub.Setup(h => h.ConnectedClientCount).Returns(10);
        hub.Setup(h => h.MaxClients).Returns(10);
        var sut = new MeshHubHealthCheck(hub.Object);

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Degraded, result.Status);
    }
}
