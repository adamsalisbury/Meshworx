using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshHubServiceCollectionExtensionsTests
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
    public void AddMeshHub_NullServices_ThrowsArgumentNullException()
    {
        IServiceCollection services = null!;

        Assert.Throws<ArgumentNullException>(() => services.AddMeshHub());
    }

    [Fact]
    public void AddMeshHub_NullConfiguration_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() => services.AddMeshHub((IConfiguration)null!));
    }

    // Registration and defaults

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_NoConfiguration_OptionsCarryMeshHubConstructorDefaults()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshHub();

        await using ServiceProvider provider = services.BuildServiceProvider();
        MeshHubOptions options = provider.GetRequiredService<IOptions<MeshHubOptions>>().Value;

        Assert.Equal(22001, options.Port);
        Assert.Null(options.Listener);
        Assert.Null(options.RegistrationTimeout);
        Assert.Null(options.MaxClients);
        Assert.Null(options.HeartbeatInterval);
        Assert.Equal(2, options.MaxMissedHeartbeats);
        Assert.Null(options.Authenticator);
        Assert.Null(options.MaxConcurrentAuthentications);
        Assert.Null(options.GroupAuthoriser);
        Assert.Null(options.GroupAuthorisationTimeout);
        Assert.Null(options.MaxConnectionsPerRemoteEndpoint);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_ResolvedTwice_ReturnsSameSingletonInstance()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshHub(options => options.Listener = CreateListenerMock().Object);

        await using ServiceProvider provider = services.BuildServiceProvider();

        IMeshHub first = provider.GetRequiredService<IMeshHub>();
        IMeshHub second = provider.GetRequiredService<IMeshHub>();

        Assert.Same(first, second);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_ListenerSet_UsesTheSuppliedListenerRatherThanBuildingATcpOne()
    {
        Mock<ITransportListener> listener = CreateListenerMock();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshHub(options => options.Listener = listener.Object);

        await using ServiceProvider provider = services.BuildServiceProvider();
        IMeshHub hub = provider.GetRequiredService<IMeshHub>();

        // Starting the hub only ever touches the listener we supplied if it really is the one in use.
        await hub.StartAsync();
        listener.Verify(l => l.StartAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_ConfigurationBinding_AppliesBoundValues()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Port"] = "23456",
                ["MaxClients"] = "50",
                ["MaxMissedHeartbeats"] = "3",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshHub(configuration);

        await using ServiceProvider provider = services.BuildServiceProvider();
        MeshHubOptions options = provider.GetRequiredService<IOptions<MeshHubOptions>>().Value;

        Assert.Equal(23456, options.Port);
        Assert.Equal(50, options.MaxClients);
        Assert.Equal(3, options.MaxMissedHeartbeats);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_ConfigureOptionsAfterBinding_OverridesTheBoundValue()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["Port"] = "1000" })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshHub(configuration, options => options.Port = 2000);

        await using ServiceProvider provider = services.BuildServiceProvider();
        MeshHubOptions options = provider.GetRequiredService<IOptions<MeshHubOptions>>().Value;

        Assert.Equal(2000, options.Port);
    }

    // Validation

    [Fact(Timeout = 1000)]
    public async Task AddMeshHub_PortOutOfRange_ThrowsOptionsValidationExceptionOnResolve()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshHub(options => options.Port = 0);

        await using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IMeshHub>());
    }

    [Fact(Timeout = 5000)]
    public async Task AddMeshHub_PortOutOfRange_HostStartAsyncFailsFastBeforeStartingTheHub()
    {
        var listener = CreateListenerMock();
        var builder = new HostBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddLogging();
            services.AddMeshHub(options =>
            {
                options.Port = -1;
                options.Listener = listener.Object;
            });
        });

        using IHost host = builder.Build();

        await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
        listener.Verify(l => l.StartAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    // Hosted service lifecycle

    [Fact(Timeout = 5000)]
    public async Task AddMeshHub_HostStartAndStop_StartsAndStopsTheRegisteredHub()
    {
        Mock<ITransportListener> listener = CreateListenerMock();

        var builder = new HostBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddLogging();
            services.AddMeshHub(options => options.Listener = listener.Object);
        });

        using IHost host = builder.Build();

        await host.StartAsync();
        listener.Verify(l => l.StartAsync(It.IsAny<CancellationToken>()), Times.Once);

        IMeshHub hub = host.Services.GetRequiredService<IMeshHub>();
        Assert.Equal(0, hub.ConnectedClientCount);

        await host.StopAsync();
    }
}
