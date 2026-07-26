using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.InMemory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshClientServiceCollectionExtensionsTests
{
    // Argument guards

    [Fact]
    public void AddMeshClient_NullServices_ThrowsArgumentNullException()
    {
        IServiceCollection services = null!;

        Assert.Throws<ArgumentNullException>(() => services.AddMeshClient("Alice"));
    }

    [Fact]
    public void AddMeshClient_EmptyClientName_ThrowsArgumentException()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddMeshClient(string.Empty));
    }

    [Fact]
    public void AddMeshClient_NullConfiguration_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() => services.AddMeshClient("Alice", (IConfiguration)null!));
    }

    // Registration and defaults

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_NoConfiguration_OptionsCarryMeshClientConstructorDefaults()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Alice");

        await using ServiceProvider provider = services.BuildServiceProvider();
        MeshClientOptions options = provider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>().Get("Alice");

        Assert.Equal("Alice", options.ClientName);
        Assert.Equal("localhost", options.Host);
        Assert.Equal(22001, options.Port);
        Assert.Null(options.TransportFactory);
        Assert.Null(options.Credential);
        Assert.Null(options.IdleTimeout);
        Assert.Null(options.SendTimeout);
        Assert.Equal(1, options.MaxSendAttempts);
        Assert.False(options.UseReconnector);
        Assert.True(options.RestoreGroupMembership);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_ConfigureOptionsSetsADifferentClientName_NameIsForcedBackToTheServiceKey()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Eve", options => options.ClientName = "SomethingElse");

        await using ServiceProvider provider = services.BuildServiceProvider();
        MeshClientOptions options = provider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>().Get("Eve");

        Assert.Equal("Eve", options.ClientName);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_ConfigurationBinding_AppliesBoundValues()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Host"] = "hub.internal",
                ["Port"] = "23456",
                ["UseReconnector"] = "true",
                ["Credential"] = Convert.ToBase64String("secret"u8.ToArray()),
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Alice", configuration);

        await using ServiceProvider provider = services.BuildServiceProvider();
        MeshClientOptions options = provider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>().Get("Alice");

        Assert.Equal("hub.internal", options.Host);
        Assert.Equal(23456, options.Port);
        Assert.True(options.UseReconnector);

        // Credential is deliberately typed byte[] rather than ReadOnlyMemory<byte> — the configuration
        // binder has no converter for ReadOnlyMemory<byte> and would silently leave it empty rather than
        // fail, so this proves a config-bound credential actually reaches the options.
        Assert.Equal("secret"u8.ToArray(), options.Credential);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_ConfigureOptionsAfterBinding_OverridesTheBoundValue()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["Port"] = "1000" })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Alice", configuration, options => options.Port = 2000);

        await using ServiceProvider provider = services.BuildServiceProvider();
        MeshClientOptions options = provider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>().Get("Alice");

        Assert.Equal(2000, options.Port);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_DefaultOptions_KeyedClientIsAPlainMeshClient()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Carol");

        await using ServiceProvider provider = services.BuildServiceProvider();
        IMeshClient client = provider.GetRequiredKeyedService<IMeshClient>("Carol");

        Assert.IsType<MeshClient>(client);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_ResolvedTwice_ReturnsSameSingletonInstance()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Carol");

        await using ServiceProvider provider = services.BuildServiceProvider();

        IMeshClient first = provider.GetRequiredKeyedService<IMeshClient>("Carol");
        IMeshClient second = provider.GetRequiredKeyedService<IMeshClient>("Carol");

        Assert.Same(first, second);
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_UseReconnector_KeyedClientIsTheReconnectorsManagedClient()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Bob", options =>
        {
            options.UseReconnector = true;
            options.TransportFactory = _ => Task.FromResult<ITransport>(Mock.Of<ITransport>());
        });

        await using ServiceProvider provider = services.BuildServiceProvider();

        IMeshClient client = provider.GetRequiredKeyedService<IMeshClient>("Bob");
        MeshClientReconnector reconnector = provider.GetRequiredKeyedService<MeshClientReconnector>("Bob");

        Assert.Same(reconnector.Client, client);
    }

    // Validation

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_PortOutOfRange_ThrowsOptionsValidationExceptionOnResolve()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Alice", options => options.Port = 0);

        await using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>().Get("Alice"));
    }

    [Fact(Timeout = 1000)]
    public async Task AddMeshClient_MaxSendAttemptsLessThanOne_ThrowsOptionsValidationExceptionOnResolve()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Alice", options => options.MaxSendAttempts = 0);

        await using ServiceProvider provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>().Get("Alice"));
    }

    [Fact(Timeout = 5000)]
    public async Task AddMeshClient_PortOutOfRange_HostStartAsyncFailsFast()
    {
        var builder = new HostBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddLogging();
            services.AddMeshClient("Alice", options => options.Port = -1);
        });

        using IHost host = builder.Build();

        await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
    }

    // Hosted service lifecycle — exercised against a real hub over the in-memory transport, since the
    // client's ConnectAsync performs a real registration handshake that a bare transport mock cannot
    // answer.

    [Fact(Timeout = 10000)]
    public async Task AddMeshClient_HostStart_ConnectsThePlainClientToTheHub()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        var builder = new HostBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddLogging();
            services.AddMeshClient("Alice", options =>
                options.TransportFactory = _ => Task.FromResult<ITransport>(listener.Connect()));
        });

        using IHost host = builder.Build();
        await host.StartAsync();

        IMeshClient client = host.Services.GetRequiredKeyedService<IMeshClient>("Alice");
        Assert.True(client.IsConnected);

        await host.StopAsync();
        Assert.False(client.IsConnected);

        await hub.StopAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task AddMeshClient_UseReconnectorHostStart_ConnectsViaTheReconnector()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        var builder = new HostBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddLogging();
            services.AddMeshClient("Bob", options =>
            {
                options.UseReconnector = true;
                options.TransportFactory = _ => Task.FromResult<ITransport>(listener.Connect());
            });
        });

        using IHost host = builder.Build();
        await host.StartAsync();

        IMeshClient client = host.Services.GetRequiredKeyedService<IMeshClient>("Bob");
        Assert.True(client.IsConnected);

        // host.StopAsync() alone must disconnect a reconnector-backed client too — it must not rely on the
        // service provider being disposed afterwards, since a caller may stop a host without disposing it.
        await host.StopAsync();
        Assert.False(client.IsConnected);

        await hub.StopAsync();
    }
}
