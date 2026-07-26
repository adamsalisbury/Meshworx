using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshClientHealthCheckTests
{
    private const string ClientName = "Alice";

    private static ServiceProvider CreateServiceProvider(IMeshClient client)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(ClientName, client);
        return services.BuildServiceProvider();
    }

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
        var sut = new MeshClientHealthCheck(CreateServiceProvider(client.Object), ClientName);

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_ClientNotConnected_ReturnsUnhealthy()
    {
        var client = new Mock<IMeshClient>();
        client.Setup(c => c.IsConnected).Returns(false);
        var sut = new MeshClientHealthCheck(CreateServiceProvider(client.Object), ClientName);

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_ClientNotRegistered_ThrowsInvalidOperationException()
    {
        var services = new ServiceCollection();
        var sut = new MeshClientHealthCheck(services.BuildServiceProvider(), ClientName);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.CheckHealthAsync(CreateContext(sut)));
    }
}
