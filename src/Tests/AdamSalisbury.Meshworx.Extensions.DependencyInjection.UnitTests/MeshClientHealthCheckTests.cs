using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshClientHealthCheckTests
{
    private static HealthCheckContext CreateContext(MeshClientHealthCheck sut)
    {
        return new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("meshclient", sut, failureStatus: null, tags: null),
        };
    }

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_ClientConnected_ReturnsHealthy()
    {
        var client = new Mock<IMeshClient>();
        client.Setup(c => c.IsConnected).Returns(true);
        var sut = new MeshClientHealthCheck(client.Object);

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_ClientNotConnected_ReturnsUnhealthy()
    {
        var client = new Mock<IMeshClient>();
        client.Setup(c => c.IsConnected).Returns(false);
        var sut = new MeshClientHealthCheck(client.Object);

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }
}
