using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshHubHealthCheckBuilderExtensionsTests
{
    private static Mock<ITransportListener> CreateListenerMock()
    {
        var listener = new Mock<ITransportListener>();
        listener.Setup(l => l.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        listener.Setup(l => l.DisposeAsync()).Returns(ValueTask.CompletedTask);
        listener.Setup(l => l.AcceptAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct).ConfigureAwait(false);
                return null!;
            });

        return listener;
    }

    // Argument guards

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

    // Registration and status mapping, against a mocked IMeshHub

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_HubRunning_ReportsHealthy()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.IsRunning).Returns(true);
        hub.Setup(h => h.ConnectedClientCount).Returns(0);
        hub.Setup(h => h.ClaimedClientSlots).Returns(0);
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
        hub.Setup(h => h.ClaimedClientSlots).Returns(0);
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

    /// <summary>
    /// A hub that was never registered — a typo'd setup, or a health check added without a matching
    /// AddMeshHub call — must map to the registration's failure status rather than surface as an
    /// unhandled exception from the probe.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_HubNotRegistered_ReportsUnhealthyRatherThanThrowing()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks().AddMeshHub();

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport report = await healthCheckService.CheckHealthAsync();

        Assert.Equal(HealthStatus.Unhealthy, report.Status);
        Assert.Equal(HealthStatus.Unhealthy, report.Entries["meshhub"].Status);
        Assert.NotNull(report.Entries["meshhub"].Exception);
    }

    // End-to-end flip, against a real MeshHub rather than a mock

    /// <summary>
    /// The health check flips from Unhealthy to Healthy and back to Unhealthy across a real hub's
    /// StartAsync/StopAsync lifecycle, matching the acceptance criteria in issue #23.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_RealHubStartedThenStopped_FlipsFromUnhealthyToHealthyToUnhealthy()
    {
        Mock<ITransportListener> listener = CreateListenerMock();
        var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener.Object);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMeshHub>(hub);
        services.AddHealthChecks().AddMeshHub();

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport beforeStart = await healthCheckService.CheckHealthAsync();
        Assert.Equal(HealthStatus.Unhealthy, beforeStart.Status);

        await hub.StartAsync();
        HealthReport afterStart = await healthCheckService.CheckHealthAsync();
        Assert.Equal(HealthStatus.Healthy, afterStart.Status);

        await hub.StopAsync();
        HealthReport afterStop = await healthCheckService.CheckHealthAsync();
        Assert.Equal(HealthStatus.Unhealthy, afterStop.Status);
    }
}
