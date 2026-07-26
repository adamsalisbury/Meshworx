using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshHubHealthCheckBuilderExtensionsTests
{
    [Fact]
    public void AddMeshHub_NullBuilder_ThrowsArgumentNullException()
    {
        IHealthChecksBuilder builder = null!;

        Assert.Throws<ArgumentNullException>(() => builder.AddMeshHub());
    }

    [Fact]
    public void AddMeshHub_NullName_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        IHealthChecksBuilder builder = services.AddHealthChecks();

        Assert.Throws<ArgumentNullException>(() => builder.AddMeshHub(name: null!));
    }

    [Fact]
    public void AddMeshHub_EmptyName_ThrowsArgumentException()
    {
        var services = new ServiceCollection();
        IHealthChecksBuilder builder = services.AddHealthChecks();

        Assert.Throws<ArgumentException>(() => builder.AddMeshHub(name: string.Empty));
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_HubRunning_ReportsHealthy()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(true);
        hub.Setup(h => h.ConnectedClientCount).Returns(0);
        hub.Setup(h => h.MaxClients).Returns(1000);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(hub.Object);
        services.AddHealthChecks().AddMeshHub();

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport report = await healthCheckService.CheckHealthAsync();

        Assert.Equal(HealthStatus.Healthy, report.Status);
        Assert.True(report.Entries.ContainsKey("meshhub"));
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_HubNotRunning_ReportsUnhealthy()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(false);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(hub.Object);
        services.AddHealthChecks().AddMeshHub();

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport report = await healthCheckService.CheckHealthAsync();

        Assert.Equal(HealthStatus.Unhealthy, report.Status);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_CustomName_RegistersUnderThatName()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(true);
        hub.Setup(h => h.ConnectedClientCount).Returns(0);
        hub.Setup(h => h.MaxClients).Returns(1000);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(hub.Object);
        services.AddHealthChecks().AddMeshHub(name: "hub-liveness");

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport report = await healthCheckService.CheckHealthAsync();

        Assert.True(report.Entries.ContainsKey("hub-liveness"));
    }
}
