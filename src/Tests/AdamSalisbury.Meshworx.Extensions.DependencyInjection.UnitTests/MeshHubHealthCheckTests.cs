using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshHubHealthCheckTests
{
    private static ServiceProvider CreateServiceProvider(IMeshHub hub)
    {
        var services = new ServiceCollection();
        services.AddSingleton(hub);
        return services.BuildServiceProvider();
    }

    private static HealthCheckContext CreateContext(MeshHubHealthCheck sut)
    {
        return new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("meshhub", sut, failureStatus: null, tags: null),
        };
    }

    // CheckHealthAsync — running state

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_HubNotRunning_ReturnsUnhealthy()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(false);
        var sut = new MeshHubHealthCheck(CreateServiceProvider(hub.Object));

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_HubNotRegistered_ThrowsInvalidOperationException()
    {
        var services = new ServiceCollection();
        var sut = new MeshHubHealthCheck(services.BuildServiceProvider());

        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.CheckHealthAsync(CreateContext(sut)));
    }

    // CheckHealthAsync — capacity, judged against ClaimedClientSlots rather than ConnectedClientCount

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_HubRunningBelowCapacity_ReturnsHealthy()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(true);
        hub.Setup(h => h.ConnectedClientCount).Returns(3);
        hub.Setup(h => h.ClaimedClientSlots).Returns(3);
        hub.Setup(h => h.MaxClients).Returns(1000);
        var sut = new MeshHubHealthCheck(CreateServiceProvider(hub.Object));

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_HubRunningAtCapacity_ReturnsDegraded()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(true);
        hub.Setup(h => h.ConnectedClientCount).Returns(10);
        hub.Setup(h => h.ClaimedClientSlots).Returns(10);
        hub.Setup(h => h.MaxClients).Returns(10);
        var sut = new MeshHubHealthCheck(CreateServiceProvider(hub.Object));

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Degraded, result.Status);
    }

    /// <summary>
    /// Capacity is judged against ClaimedClientSlots, not ConnectedClientCount: several clients can be
    /// mid-handshake with every slot claimed — so the hub is already refusing new connections — while
    /// ConnectedClientCount, which only counts fully registered clients, still reads below MaxClients.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task CheckHealthAsync_SlotsClaimedButNotYetRegistered_ReturnsDegraded()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(true);
        hub.Setup(h => h.ConnectedClientCount).Returns(2);
        hub.Setup(h => h.ClaimedClientSlots).Returns(10);
        hub.Setup(h => h.MaxClients).Returns(10);
        var sut = new MeshHubHealthCheck(CreateServiceProvider(hub.Object));

        HealthCheckResult result = await sut.CheckHealthAsync(CreateContext(sut));

        Assert.Equal(HealthStatus.Degraded, result.Status);
    }
}
