using System.Diagnostics;
using System.Reflection;
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
        Assert.Null(options.SendRetryDelay);
        Assert.Null(options.ReconnectRetryDelay);
        Assert.Null(options.ReconnectConnectTimeout);
        Assert.Null(options.MaxReassemblyBytes);
        Assert.Null(options.ChunkTransferTimeout);
    }

    /// <summary>
    /// Every settable <see cref="MeshClientOptions"/> property that is not itself DI plumbing
    /// (<see cref="MeshClientOptions.ClientName"/>, <see cref="MeshClientOptions.Host"/>,
    /// <see cref="MeshClientOptions.Port"/>, <see cref="MeshClientOptions.TransportFactory"/> and
    /// <see cref="MeshClientOptions.UseReconnector"/> together stand in for the constructor parameters
    /// those five replace) must name a real <see cref="MeshClient"/> or
    /// <see cref="MeshClientReconnector"/> constructor parameter, and every constructor parameter beyond
    /// that plumbing must have a matching options property — so that a future constructor addition to
    /// either type fails this test rather than silently becoming unreachable through AddMeshClient
    /// (issue #99).
    /// </summary>
    [Fact]
    public void MeshClientOptions_EveryProperty_MirrorsAMeshClientOrMeshClientReconnectorConstructorParameter()
    {
        ParameterInfo[] clientParameters = typeof(MeshClient).GetConstructors().Single().GetParameters();
        ParameterInfo[] reconnectorParameters =
            typeof(MeshClientReconnector).GetConstructors().Single().GetParameters();

        // logger is supplied from the container on both constructors, not carried on the options.
        // timeProvider is a testing seam with no options surface anywhere in this package.
        var excludedClientParameters = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "logger", "timeProvider",
        };

        // client/clientName/transportFactory are supplied by AddMeshClient itself — the client it just
        // built, the clientName argument, and the TransportFactory/default-transport closure — not read
        // from the options a second time.
        var excludedReconnectorParameters = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "client", "clientName", "transportFactory", "logger",
        };

        // MeshClientReconnector's retryDelay/connectTimeout would collide in meaning with a hypothetical
        // MeshClient-side retry/connect setting, so the options properties disambiguate with a
        // Reconnect-prefixed name; every other parameter name matches its property name exactly.
        var reconnectorParameterRenames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["retryDelay"] = "ReconnectRetryDelay",
            ["connectTimeout"] = "ReconnectConnectTimeout",
        };

        var expectedPropertyNames = clientParameters
            .Select(p => p.Name!)
            .Where(name => !excludedClientParameters.Contains(name))
            .Concat(reconnectorParameters
                .Select(p => p.Name!)
                .Where(name => !excludedReconnectorParameters.Contains(name))
                .Select(name => reconnectorParameterRenames.GetValueOrDefault(name, name)))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var diPlumbingPropertyNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "ClientName", "Host", "Port", "TransportFactory", "UseReconnector",

            // Govern MeshClientHostedService's own initial-connect retry loop (issue #100/#112) rather
            // than mirroring a MeshClient/MeshClientReconnector constructor parameter — neither type's
            // constructor knows anything about retrying its own construction.
            "ConnectTimeout", "ConnectRetryDelay",
        };

        var actualPropertyNames = typeof(MeshClientOptions)
            .GetProperties()
            .Select(p => p.Name)
            .Where(name => !diPlumbingPropertyNames.Contains(name))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Equal(expectedPropertyNames, actualPropertyNames);
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

    [Fact]
    public void AddMeshClient_CalledTwiceForTheSameName_RegistersOnlyOneHostedService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMeshClient("Carol");
        services.AddMeshClient("Carol", options => options.Port = 23456);

        int hostedServiceRegistrations = services.Count(descriptor => descriptor.ServiceType == typeof(IHostedService));

        Assert.Equal(1, hostedServiceRegistrations);
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

    // Initial connect retry (issue #100)

    /// <summary>
    /// A transport factory that never succeeds must not block host startup indefinitely — each attempt
    /// is bounded by <see cref="MeshClientOptions.ConnectTimeout"/>, so cancelling the host's own start
    /// token is observed within roughly one attempt-plus-retry-delay, rather than only once some far
    /// longer, unbounded operation eventually gives up on its own.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task AddMeshClient_HostStart_TransportFactoryNeverSucceeds_RespectsCancellationRatherThanHangingIndefinitely()
    {
        var builder = new HostBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddLogging();
            services.AddMeshClient("Alice", options =>
            {
                options.ConnectTimeout = TimeSpan.FromMilliseconds(50);
                options.ConnectRetryDelay = TimeSpan.FromMilliseconds(50);
                options.TransportFactory = _ => Task.FromException<ITransport>(new IOException("unreachable"));
            });
        });

        using IHost host = builder.Build();

        using var startCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var stopwatch = Stopwatch.StartNew();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => host.StartAsync(startCts.Token));

        stopwatch.Stop();

        // Generous relative to the 500ms cancellation deadline, but tight relative to what an unbounded
        // single attempt would take (TCP's own connect timeout is on the order of two minutes) — proving
        // this returned because cancellation was observed promptly, not because some much longer
        // operation eventually unwound on its own.
        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(5),
            $"Expected host.StartAsync to observe cancellation promptly, took {stopwatch.Elapsed.TotalMilliseconds}ms");
    }

    /// <summary>
    /// A transport factory that fails a few times before succeeding — standing in for a hub that has not
    /// finished starting yet, very often true of a separate process in a real deployment — must still let
    /// the client connect once it becomes reachable, rather than the first failure killing host startup
    /// (issue #100).
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task AddMeshClient_HostStart_TransportFactoryFailsThenSucceeds_ConnectsOnceReachable()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        var attempt = 0;

        var builder = new HostBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddLogging();
            services.AddMeshClient("Alice", options =>
            {
                options.ConnectRetryDelay = TimeSpan.FromMilliseconds(20);
                options.TransportFactory = _ =>
                {
                    Interlocked.Increment(ref attempt);
                    return attempt <= 2
                        ? Task.FromException<ITransport>(new IOException("hub not up yet"))
                        : Task.FromResult<ITransport>(listener.Connect());
                };
            });
        });

        using IHost host = builder.Build();
        await host.StartAsync();

        IMeshClient client = host.Services.GetRequiredKeyedService<IMeshClient>("Alice");
        Assert.True(client.IsConnected);
        Assert.True(attempt >= 3);

        await host.StopAsync();
        await hub.StopAsync();
    }
}
