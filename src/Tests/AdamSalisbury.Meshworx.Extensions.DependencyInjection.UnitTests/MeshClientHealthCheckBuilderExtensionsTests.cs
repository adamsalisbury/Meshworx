using System.Threading.Channels;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshClientHealthCheckBuilderExtensionsTests
{
    private static Mock<ITransport> CreateTransportMock(Guid assignedId)
    {
        var transport = new Mock<ITransport>();
        transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var registrationResponse = new byte[18];
        registrationResponse[0] = 0x01; // RegistrationComplete
        assignedId.TryWriteBytes(registrationResponse.AsSpan(1, 16));
        registrationResponse[17] = 0x04; // negotiated protocol version

        // Yield the registration response then block, exactly as a live connection would, so the
        // client's receive loop stays alive until DisconnectAsync cancels it.
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(registrationResponse);
        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));

        return transport;
    }

    // Argument guards

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

    // Registration and status mapping, against a mocked IMeshClient

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

    /// <summary>
    /// A client name with no matching registration — a typo, or a health check added without a matching
    /// AddMeshClient call — must map to the registration's failure status rather than surface as an
    /// unhandled exception from the probe.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_ClientNotRegistered_ReportsUnhealthyRatherThanThrowing()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks().AddMeshClient("Eve");

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport report = await healthCheckService.CheckHealthAsync();

        Assert.Equal(HealthStatus.Unhealthy, report.Status);
        Assert.Equal(HealthStatus.Unhealthy, report.Entries["meshclient:Eve"].Status);
        Assert.NotNull(report.Entries["meshclient:Eve"].Exception);
    }

    // End-to-end flip, against a real MeshClient rather than a mock

    /// <summary>
    /// The health check flips from Unhealthy to Healthy and back to Unhealthy across a real client's
    /// connect/disconnect lifecycle, matching the acceptance criteria in issue #23.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_RealClientConnectedThenDisconnected_FlipsFromUnhealthyToHealthyToUnhealthy()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        Mock<ITransport> transport = CreateTransportMock(Guid.NewGuid());

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IMeshClient>("Dave", client);
        services.AddHealthChecks().AddMeshClient("Dave");

        await using ServiceProvider provider = services.BuildServiceProvider();
        HealthCheckService healthCheckService = provider.GetRequiredService<HealthCheckService>();

        HealthReport beforeConnect = await healthCheckService.CheckHealthAsync();
        Assert.Equal(HealthStatus.Unhealthy, beforeConnect.Status);

        await client.ConnectAsync(transport.Object, "Dave");
        HealthReport afterConnect = await healthCheckService.CheckHealthAsync();
        Assert.Equal(HealthStatus.Healthy, afterConnect.Status);

        await client.DisconnectAsync();
        HealthReport afterDisconnect = await healthCheckService.CheckHealthAsync();
        Assert.Equal(HealthStatus.Unhealthy, afterDisconnect.Status);
    }
}
