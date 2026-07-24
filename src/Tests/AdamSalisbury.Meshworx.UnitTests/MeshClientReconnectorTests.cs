using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.InMemory;
using AdamSalisbury.Meshworx.Transport.Tcp;
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

    /// <summary>
    /// The TCP transport-factory idiom shown in the README compiles and is accepted by the constructor.
    /// The factory is never invoked here (StartAsync is not called), so no real connection is attempted.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_AcceptsTcpTransportFactoryFromDocumentation()
    {
        await using var client = CreateClient();

        await using var reconnector = new MeshClientReconnector(
            client,
            "Alice",
            async ct => (ITransport)await TcpTransport.ConnectAsync("localhost", 22001, ct));

        Assert.False(client.IsConnected);
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

    /// <summary>
    /// When the initial connection fails, StartAsync can be called again and succeed once a hub is
    /// available, rather than being locked into a started-but-unconnected state.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_CanBeRetriedAfterInitialFailure()
    {
        // First target: a started listener with no hub, so the initial attempt times out.
        var deadListener = new InMemoryTransportListener();
        await deadListener.StartAsync();
        var listenerHolder = new[] { deadListener };

        await using var client = CreateClient();
        await using var reconnector = new MeshClientReconnector(
            client,
            "Alice",
            _ => Task.FromResult<ITransport>(Volatile.Read(ref listenerHolder[0]).Connect()),
            connectTimeout: TimeSpan.FromMilliseconds(200));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reconnector.StartAsync());
        Assert.False(client.IsConnected);

        // Point the factory at a live hub and retry.
        var liveListener = new InMemoryTransportListener();
        await using var hub = CreateHub(liveListener);
        await hub.StartAsync();
        Volatile.Write(ref listenerHolder[0], liveListener);

        await reconnector.StartAsync();

        Assert.True(client.IsConnected);
        Assert.True(hub.IsClientRegistered(client.Id));

        await hub.StopAsync();
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

    /// <summary>
    /// After a reconnect, the managed client still delivers messages: a handler subscribed once keeps
    /// firing, proving the receive loop restarts and subscriptions persist across reconnection.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task DeliversMessages_AfterReconnect()
    {
        var firstListener = new InMemoryTransportListener();
        var firstHub = CreateHub(firstListener);
        await firstHub.StartAsync();
        var listenerHolder = new[] { firstListener };

        await using var aliceClient = CreateClient();

        // Subscribe once, before any reconnect.
        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        aliceClient.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await using var reconnector = new MeshClientReconnector(
            aliceClient,
            "Alice",
            _ => Task.FromResult<ITransport>(Volatile.Read(ref listenerHolder[0]).Connect()),
            retryDelay: TimeSpan.FromMilliseconds(50),
            connectTimeout: TimeSpan.FromSeconds(2));

        var reconnectedTcs = new TaskCompletionSource();
        reconnector.Reconnected += (_, _) => reconnectedTcs.TrySetResult();
        await reconnector.StartAsync();

        // Move to a replacement hub and force a reconnect.
        var secondListener = new InMemoryTransportListener();
        await using var secondHub = CreateHub(secondListener);
        await secondHub.StartAsync();
        Volatile.Write(ref listenerHolder[0], secondListener);
        await firstHub.StopAsync();
        await firstHub.DisposeAsync();
        await reconnectedTcs.Task.WaitAsync(WaitTimeout);

        // A new sender on the replacement hub looks Alice up and sends her a message.
        await using var bob = CreateClient();
        await bob.ConnectAsync(secondListener.Connect(), "Bob");
        Guid? aliceId = await bob.GetClientIdByNameAsync("Alice");
        Assert.Equal(aliceClient.Id, aliceId);

        byte[] payload = Encoding.UTF8.GetBytes("hi after reconnect");
        await bob.SendAsync(aliceId!.Value, payload);

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(bob.Id, received.SenderId);
        Assert.Equal(payload, received.Data.ToArray());

        await secondHub.StopAsync();
    }

    // Group membership restoration

    /// <summary>
    /// After an unexpected drop and reconnect, the reconnector re-joins the client's previous groups, so
    /// group messages resume without the application re-joining in the Reconnected handler.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task RestoresGroupMembership_AfterReconnect()
    {
        var firstListener = new InMemoryTransportListener();
        var firstHub = CreateHub(firstListener);
        await firstHub.StartAsync();
        var listenerHolder = new[] { firstListener };

        await using var aliceClient = CreateClient();

        // Subscribe once, before any reconnect.
        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        aliceClient.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await using var reconnector = new MeshClientReconnector(
            aliceClient,
            "Alice",
            _ => Task.FromResult<ITransport>(Volatile.Read(ref listenerHolder[0]).Connect()),
            retryDelay: TimeSpan.FromMilliseconds(50),
            connectTimeout: TimeSpan.FromSeconds(2));

        var reconnectedTcs = new TaskCompletionSource();
        reconnector.Reconnected += (_, _) => reconnectedTcs.TrySetResult();
        await reconnector.StartAsync();

        // Alice joins a group on the first hub, then the connection is moved to a replacement hub.
        await aliceClient.JoinGroupAsync("news");

        var secondListener = new InMemoryTransportListener();
        await using var secondHub = CreateHub(secondListener);
        await secondHub.StartAsync();
        Volatile.Write(ref listenerHolder[0], secondListener);
        await firstHub.StopAsync();
        await firstHub.DisposeAsync();
        await reconnectedTcs.Task.WaitAsync(WaitTimeout);

        // The reconnector should have re-joined "news" on the replacement hub before raising Reconnected.
        Assert.Contains("news", aliceClient.JoinedGroups);

        // A sender on the replacement hub addresses the group; Alice receives it as a group member.
        await using var bob = CreateClient();
        await bob.ConnectAsync(secondListener.Connect(), "Bob");
        byte[] payload = Encoding.UTF8.GetBytes("group message after reconnect");
        await bob.SendToGroupAsync("news", payload);

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("news", received.GroupName);
        Assert.Equal(bob.Id, received.SenderId);
        Assert.Equal(payload, received.Data.ToArray());

        await secondHub.StopAsync();
    }

    /// <summary>
    /// When group-membership restoration is disabled, the reconnector does not re-join the client's
    /// previous groups after a reconnect, leaving restoration to the application.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task DoesNotRestoreGroupMembership_WhenDisabled()
    {
        var firstListener = new InMemoryTransportListener();
        var firstHub = CreateHub(firstListener);
        await firstHub.StartAsync();
        var listenerHolder = new[] { firstListener };

        await using var aliceClient = CreateClient();
        await using var reconnector = new MeshClientReconnector(
            aliceClient,
            "Alice",
            _ => Task.FromResult<ITransport>(Volatile.Read(ref listenerHolder[0]).Connect()),
            retryDelay: TimeSpan.FromMilliseconds(50),
            connectTimeout: TimeSpan.FromSeconds(2),
            restoreGroupMembership: false);

        var reconnectedTcs = new TaskCompletionSource();
        reconnector.Reconnected += (_, _) => reconnectedTcs.TrySetResult();
        await reconnector.StartAsync();

        await aliceClient.JoinGroupAsync("news");

        var secondListener = new InMemoryTransportListener();
        await using var secondHub = CreateHub(secondListener);
        await secondHub.StartAsync();
        Volatile.Write(ref listenerHolder[0], secondListener);
        await firstHub.StopAsync();
        await firstHub.DisposeAsync();
        await reconnectedTcs.Task.WaitAsync(WaitTimeout);

        // With restoration disabled the client should not have re-joined any group.
        Assert.Empty(aliceClient.JoinedGroups);

        await secondHub.StopAsync();
    }

    // DisposeAsync

    /// <summary>
    /// Disposing the reconnector twice is safe and does not throw.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = CreateHub(listener);
        await hub.StartAsync();

        var client = CreateClient();
        var reconnector = new MeshClientReconnector(
            client, "Alice", _ => Task.FromResult<ITransport>(listener.Connect()));
        await reconnector.StartAsync();

        await reconnector.DisposeAsync();
        await reconnector.DisposeAsync();

        Assert.False(client.IsConnected);

        await hub.StopAsync();
    }

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
