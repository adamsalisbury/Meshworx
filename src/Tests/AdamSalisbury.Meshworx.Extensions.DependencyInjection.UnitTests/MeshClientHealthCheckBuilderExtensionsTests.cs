using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshClientHealthCheckBuilderExtensionsTests
{
    [Fact]
    public void AddMeshClient_NullBuilder_ThrowsArgumentNullException()
    {
        IHealthChecksBuilder builder = null!;

        Assert.Throws<ArgumentNullException>(() => builder.AddMeshClient("Alice"));
    }

    [Fact]
    public void AddMeshClient_NullClientName_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        IHealthChecksBuilder builder = services.AddHealthChecks();

        Assert.Throws<ArgumentNullException>(() => builder.AddMeshClient(null!));
    }

    [Fact]
    public void AddMeshClient_EmptyClientName_ThrowsArgumentException()
    {
        var services = new ServiceCollection();
        IHealthChecksBuilder builder = services.AddHealthChecks();

        Assert.Throws<ArgumentException>(() => builder.AddMeshClient(string.Empty));
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_ClientConnected_ReportsHealthy()
    {
        var client = new Mock<IMeshClient>();
        client.Setup(c => c.IsConnected).Returns(true);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton("Alice", client.Object);
        services.AddHealthChecks().AddMeshClient("Alice");

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport report = await healthCheckService.CheckHealthAsync();

        Assert.Equal(HealthStatus.Healthy, report.Status);
        Assert.True(report.Entries.ContainsKey("meshclient:Alice"));
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_ClientNotConnected_ReportsUnhealthy()
    {
        var client = new Mock<IMeshClient>();
        client.Setup(c => c.IsConnected).Returns(false);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton("Bob", client.Object);
        services.AddHealthChecks().AddMeshClient("Bob");

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport report = await healthCheckService.CheckHealthAsync();

        Assert.Equal(HealthStatus.Unhealthy, report.Status);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_CustomName_RegistersUnderThatName()
    {
        var client = new Mock<IMeshClient>();
        client.Setup(c => c.IsConnected).Returns(true);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton("Carol", client.Object);
        services.AddHealthChecks().AddMeshClient("Carol", name: "carol-connectivity");

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport report = await healthCheckService.CheckHealthAsync();

        Assert.True(report.Entries.ContainsKey("carol-connectivity"));
    }
}
