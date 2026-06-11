using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.InMemory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

public sealed class MeshClientReconnectorTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    private static MeshHub CreateHub(ITransportListener listener)
    {
        return new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
    }

    // Constructor

    /// <summary>
    /// When the reconnector is constructed with a null client, an ArgumentNullException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_NullClient_Throws()
    {
        await Task.CompletedTask;
        Assert.Throws<ArgumentNullException>(
            () => new MeshClientReconnector(null!, "Alice", _ => Task.FromResult<ITransport>(null!)));
    }

    /// <summary>
    /// When the reconnector is constructed with an empty client name, an ArgumentException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_EmptyName_Throws()
    {
        await using var client = CreateClient();
        Assert.Throws<ArgumentException>(
            () => new MeshClientReconnector(client, string.Empty, _ => Task.FromResult<ITransport>(null!)));
    }

    // StartAsync

    /// <summary>
    /// When started against a running hub, the reconnector connects the client.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_ConnectsClient()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = CreateHub(listener);
        await hub.StartAsync();

        await using var client = CreateClient();
        await using var reconnector = new MeshClientReconnector(
            client, "Alice", _ => Task.FromResult<ITransport>(listener.Connect()));

        await reconnector.StartAsync();

        Assert.True(client.IsConnected);
        Assert.True(hub.IsClientRegistered(client.Id));

        await hub.StopAsync();
    }

    /// <summary>
    /// When the initial connection cannot be completed within the connect timeout, StartAsync throws
    /// rather than retrying forever.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_InitialConnectTimesOut_Throws()
    {
        // A started listener with no hub behind it accepts the connection but never answers registration.
        var listener = new InMemoryTransportListener();
        await listener.StartAsync();

        await using var client = CreateClient();
        await using var reconnector = new MeshClientReconnector(
            client,
            "Alice",
            _ => Task.FromResult<ITransport>(listener.Connect()),
            connectTimeout: TimeSpan.FromMilliseconds(200));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reconnector.StartAsync());
        Assert.False(client.IsConnected);

        await listener.DisposeAsync();
    }

    // Reconnection

    /// <summary>
    /// When the connection is lost, the reconnector re-establishes it against a new hub and raises
    /// Reconnected.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task Reconnects_AfterConnectionLost()
    {
        var firstListener = new InMemoryTransportListener();
        var firstHub = CreateHub(firstListener);
        await firstHub.StartAsync();

        // The factory targets whichever listener the single-element holder points at.
        var listenerHolder = new[] { firstListener };

        await using var client = CreateClient();
        await using var reconnector = new MeshClientReconnector(
            client,
            "Alice",
            _ => Task.FromResult<ITransport>(Volatile.Read(ref listenerHolder[0]).Connect()),
            retryDelay: TimeSpan.FromMilliseconds(50),
            connectTimeout: TimeSpan.FromSeconds(2));

        var reconnectedTcs = new TaskCompletionSource();
        reconnector.Reconnected += (_, _) => reconnectedTcs.TrySetResult();

        await reconnector.StartAsync();
        Assert.True(client.IsConnected);

        // Stand up a replacement hub and point the factory at it before dropping the first connection.
        var secondListener = new InMemoryTransportListener();
        await using var secondHub = CreateHub(secondListener);
        await secondHub.StartAsync();
        Volatile.Write(ref listenerHolder[0], secondListener);

        // Dropping the first hub disconnects the client, triggering reconnection to the second hub.
        await firstHub.StopAsync();
        await firstHub.DisposeAsync();

        await reconnectedTcs.Task.WaitAsync(WaitTimeout);

        Assert.True(client.IsConnected);
        Assert.True(secondHub.IsClientRegistered(client.Id));

        await secondHub.StopAsync();
    }

    // DisposeAsync

    /// <summary>
    /// When the reconnector is disposed, the managed client is disconnected.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_DisconnectsClient()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = CreateHub(listener);
        await hub.StartAsync();

        var client = CreateClient();
        var reconnector = new MeshClientReconnector(
            client,
            "Alice",
            _ => Task.FromResult<ITransport>(listener.Connect()),
            logger: NullLogger<MeshClientReconnector>.Instance);
        await reconnector.StartAsync();

        await reconnector.DisposeAsync();

        Assert.False(client.IsConnected);

        await hub.StopAsync();
    }
}
