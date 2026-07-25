using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.InMemory;
using AdamSalisbury.Meshworx.Transport.Tcp;
using AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

public sealed class MeshClientReconnectorTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);
    private static readonly string[] GroupsAbc = ["A", "B", "C"];
    private static readonly string[] GroupsAb = ["A", "B"];

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

    /// <summary>
    /// The TLS transport-factory idiom shown in the README really connects a reconnector to a
    /// TLS-secured hub, and the client ends up on an encrypted transport.
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task StartAsync_TlsTransportFactoryFromDocumentation_ConnectsOverEncryptedTransport()
    {
        using X509Certificate2 hubCertificate = TestCertificates.CreateSelfSigned("localhost");

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = hubCertificate });

        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        var tlsOptions = new SslClientAuthenticationOptions
        {
            RemoteCertificateValidationCallback = TestCertificates.PinnedTo(hubCertificate),
        };

        TcpTransport? established = null;

        await using var client = CreateClient();
        await using var reconnector = new MeshClientReconnector(
            client,
            "Alice",
            async ct =>
            {
                established = await TcpTransport.ConnectAsync("localhost", port, tlsOptions, ct);
                return established;
            });

        await reconnector.StartAsync();

        Assert.True(client.IsConnected);
        Assert.True(hub.IsClientRegistered(client.Id));
        Assert.True(established!.IsEncrypted);

        await hub.StopAsync();
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

    /// <summary>
    /// When the connection is lost in the window between the initial connect completing and StartAsync
    /// subscribing to Disconnected, the reconnector still reconnects. The drop is raised while nothing is
    /// subscribed, so the event is genuinely lost and the reconnect can only come from re-reading the
    /// connection state once the handler is attached.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task StartAsync_ConnectionLostBeforeSubscription_StillReconnects()
    {
        var transport = new Mock<ITransport>();
        transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var client = new Mock<IMeshClient>();
        client.Setup(c => c.DisconnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        client.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        // A single-element holder lets the connect callback publish the state the reconnector reads back.
        bool[] connectedHolder = [false];
        client.SetupGet(c => c.IsConnected).Returns(() => Volatile.Read(ref connectedHolder[0]));

        int connectAttempts = 0;

        client.Setup(c => c.ConnectAsync(
                It.IsAny<ITransport>(),
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                Volatile.Write(ref connectedHolder[0], true);

                if (Interlocked.Increment(ref connectAttempts) == 1)
                {
                    // The hub drops the connection the instant registration completes — the real client's
                    // receive loop is already running by the time ConnectAsync returns, so it can observe
                    // the drop before StartAsync gets as far as subscribing. Raising it here reproduces
                    // that ordering exactly: there is no subscriber, and the event is lost.
                    Volatile.Write(ref connectedHolder[0], false);
                    client.Raise(
                        c2 => c2.Disconnected += null,
                        new DisconnectedEventArgs { Reason = DisconnectReason.ConnectionLost });
                }

                return Task.CompletedTask;
            });

        await using var reconnector = new MeshClientReconnector(
            client.Object,
            "Alice",
            _ => Task.FromResult<ITransport>(transport.Object),
            retryDelay: TimeSpan.FromMilliseconds(10));

        var reconnectedTcs = new TaskCompletionSource();
        reconnector.Reconnected += (_, _) => reconnectedTcs.TrySetResult();

        await reconnector.StartAsync();

        await reconnectedTcs.Task.WaitAsync(WaitTimeout);

        Assert.True(client.Object.IsConnected);
        Assert.Equal(2, Volatile.Read(ref connectAttempts));
    }

    /// <summary>
    /// When the initial connection stays up, the post-connect state re-check does not queue a reconnect
    /// that is not needed: the client is connected exactly once and Reconnected never fires.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_ConnectionStaysUp_DoesNotReconnectSpuriously()
    {
        var transport = new Mock<ITransport>();
        transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var client = new Mock<IMeshClient>();
        client.Setup(c => c.DisconnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        client.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
        client.SetupGet(c => c.IsConnected).Returns(true);

        int connectAttempts = 0;

        client.Setup(c => c.ConnectAsync(
                It.IsAny<ITransport>(),
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                Interlocked.Increment(ref connectAttempts);
                return Task.CompletedTask;
            });

        await using var reconnector = new MeshClientReconnector(
            client.Object,
            "Alice",
            _ => Task.FromResult<ITransport>(transport.Object),
            retryDelay: TimeSpan.FromMilliseconds(10));

        int reconnectedCount = 0;
        reconnector.Reconnected += (_, _) => Interlocked.Increment(ref reconnectedCount);

        await reconnector.StartAsync();

        // A negative assertion needs settling time: a spurious signal queued by StartAsync would be
        // picked up by the reconnect loop well within this window, given the 10 ms retry delay.
        await Task.Delay(TimeSpan.FromMilliseconds(400));

        Assert.Equal(1, Volatile.Read(ref connectAttempts));
        Assert.Equal(0, Volatile.Read(ref reconnectedCount));
    }

    /// <summary>
    /// When the teardown straddles the subscription — the connection is already going down when StartAsync
    /// re-checks the state, but the Disconnected event is only raised afterwards, to the now-subscribed
    /// handler — the single drop is signalled twice. The reconnector must settle on one connection rather
    /// than retrying an already-connected client for ever.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task StartAsync_DropSignalledTwice_SettlesWithoutRetryingConnectedClient()
    {
        var transport = new Mock<ITransport>();
        transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var client = new Mock<IMeshClient>();
        client.Setup(c => c.DisconnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        client.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        bool[] connectedHolder = [false];
        client.SetupGet(c => c.IsConnected).Returns(() => Volatile.Read(ref connectedHolder[0]));

        int connectAttempts = 0;
        var reconnectedTcs = new TaskCompletionSource();

        client.Setup(c => c.ConnectAsync(
                It.IsAny<ITransport>(),
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                int attempt = Interlocked.Increment(ref connectAttempts);

                if (Volatile.Read(ref connectedHolder[0]))
                {
                    // Mirrors MeshClient, which refuses a connect unless it is fully disconnected.
                    throw new InvalidOperationException("Already connected to a hub.");
                }

                if (attempt == 1)
                {
                    // Registration succeeds, but the hub has already dropped the connection: the client
                    // is mid-teardown, so it reports itself disconnected and has not yet raised the event.
                    return Task.CompletedTask;
                }

                if (attempt == 2)
                {
                    // The teardown that began during the initial connect only completes now, so the
                    // Disconnected event reaches the handler StartAsync subscribed in the meantime. The
                    // same drop has now been signalled twice: once by the state re-check, once here.
                    client.Raise(
                        c2 => c2.Disconnected += null,
                        new DisconnectedEventArgs { Reason = DisconnectReason.ConnectionLost });
                }

                Volatile.Write(ref connectedHolder[0], true);
                return Task.CompletedTask;
            });

        await using var reconnector = new MeshClientReconnector(
            client.Object,
            "Alice",
            _ => Task.FromResult<ITransport>(transport.Object),
            retryDelay: TimeSpan.FromMilliseconds(10));

        reconnector.Reconnected += (_, _) => reconnectedTcs.TrySetResult();

        // The first connect returns with the drop already in flight: the client reports itself
        // disconnected, but has not yet raised the event.
        await reconnector.StartAsync();

        await reconnectedTcs.Task.WaitAsync(WaitTimeout);

        // Settling time: an unsatisfiable reconnect would keep attempting every 10 ms, so a stuck loop
        // shows up here as an attempt count well past two.
        await Task.Delay(TimeSpan.FromMilliseconds(400));

        Assert.True(client.Object.IsConnected);
        Assert.Equal(2, Volatile.Read(ref connectAttempts));
    }

    /// <summary>
    /// When a reconnect attempt is rejected before the client adopts the transport, that transport is
    /// disposed rather than abandoned — nothing else holds a reference with which to close it. The
    /// transports the client did adopt are left alone, because the client owns them.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ConnectWithRetry_AttemptRejected_DisposesTheAbandonedTransport()
    {
        var transports = new ConcurrentQueue<Mock<ITransport>>();

        var client = new Mock<IMeshClient>();
        client.Setup(c => c.DisconnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        client.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        bool[] connectedHolder = [false];
        client.SetupGet(c => c.IsConnected).Returns(() => Volatile.Read(ref connectedHolder[0]));

        int connectAttempts = 0;

        client.Setup(c => c.ConnectAsync(
                It.IsAny<ITransport>(),
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref connectAttempts) == 2)
                {
                    // The second attempt is refused, so the client never takes the transport on.
                    throw new IOException("the hub refused the connection");
                }

                Volatile.Write(ref connectedHolder[0], true);
                return Task.CompletedTask;
            });

        await using var reconnector = new MeshClientReconnector(
            client.Object,
            "Alice",
            _ =>
            {
                var attemptTransport = new Mock<ITransport>();
                attemptTransport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
                transports.Enqueue(attemptTransport);
                return Task.FromResult(attemptTransport.Object);
            },
            retryDelay: TimeSpan.FromMilliseconds(10));

        var reconnectedTcs = new TaskCompletionSource();
        reconnector.Reconnected += (_, _) => reconnectedTcs.TrySetResult();

        await reconnector.StartAsync();

        Volatile.Write(ref connectedHolder[0], false);
        client.Raise(
            c => c.Disconnected += null,
            new DisconnectedEventArgs { Reason = DisconnectReason.ConnectionLost });

        await reconnectedTcs.Task.WaitAsync(WaitTimeout);

        // Three transports were built: the initial connect, the refused attempt, and the retry that
        // succeeded. Only the refused one went unadopted, so only it should have been disposed.
        Mock<ITransport>[] built = [.. transports];
        Assert.Equal(3, built.Length);
        built[0].Verify(t => t.DisposeAsync(), Times.Never);
        built[1].Verify(t => t.DisposeAsync(), Times.Once);
        built[2].Verify(t => t.DisposeAsync(), Times.Never);
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

        // A sender on the replacement hub addresses the group; Alice receives it as a group member. Bob
        // joins first because sending to a group requires membership of it; his join and send travel the
        // same connection, so the hub applies them in that order.
        await using var bob = CreateClient();
        await bob.ConnectAsync(secondListener.Connect(), "Bob");
        await bob.JoinGroupAsync("news");
        byte[] payload = Encoding.UTF8.GetBytes("group message after reconnect");
        await bob.SendToGroupAsync("news", payload);

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("news", received.GroupName);
        Assert.Equal(bob.Id, received.SenderId);
        Assert.Equal(payload, received.Data.ToArray());

        await secondHub.StopAsync();
    }

    /// <summary>
    /// Restored membership is re-authorised by the hub rather than reinstated. The reconnector restores
    /// by re-joining over the wire, so a hub whose group authoriser has since changed its mind refuses
    /// the re-join exactly as it would a fresh one — the restore path cannot smuggle a client back into
    /// a group it is no longer entitled to.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task RestoredGroupMembership_IsAuthorisedAgainByTheHub()
    {
        int joinAttempts = 0;

        // Allow the first join and refuse every later one, so a restore that bypassed authorisation
        // would show up as a membership the hub never granted.
        GroupAuthoriser authoriser = (_, _) =>
            ValueTask.FromResult(Interlocked.Increment(ref joinAttempts) == 1);

        var firstListener = new InMemoryTransportListener();
        var firstHub = new MeshHub(
            new Mock<ILogger<MeshHub>>().Object, firstListener, groupAuthoriser: authoriser);
        await firstHub.StartAsync();
        var listenerHolder = new[] { firstListener };

        await using var aliceClient = CreateClient();

        var refusedTcs = new TaskCompletionSource<GroupJoinRefusedEventArgs>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        aliceClient.GroupJoinRefused += (_, e) => refusedTcs.TrySetResult(e);

        await using var reconnector = new MeshClientReconnector(
            aliceClient,
            "Alice",
            _ => Task.FromResult<ITransport>(Volatile.Read(ref listenerHolder[0]).Connect()),
            retryDelay: TimeSpan.FromMilliseconds(50),
            connectTimeout: TimeSpan.FromSeconds(2));

        await reconnector.StartAsync();
        await aliceClient.JoinGroupAsync("news");

        // Move the connection to a replacement hub that shares the authoriser, so the re-join is the
        // second attempt it sees.
        var secondListener = new InMemoryTransportListener();
        await using var secondHub = new MeshHub(
            new Mock<ILogger<MeshHub>>().Object, secondListener, groupAuthoriser: authoriser);
        await secondHub.StartAsync();
        Volatile.Write(ref listenerHolder[0], secondListener);
        await firstHub.StopAsync();
        await firstHub.DisposeAsync();

        GroupJoinRefusedEventArgs refused = await refusedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("news", refused.GroupName);
        Assert.Equal(2, Volatile.Read(ref joinAttempts));

        // The refused group is gone from the client's membership, so nothing goes on believing it is
        // restored — and the reconnector has no membership left to restore on a later drop.
        Assert.DoesNotContain("news", aliceClient.JoinedGroups);

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

    /// <summary>
    /// When a second disconnect interrupts restoration part-way through, the groups not yet re-joined are
    /// not lost: the follow-up reconnect restores them rather than dropping them from the pending set.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task RestoresGroupMembership_SurvivesInterruptedRestore()
    {
        var transport = new Mock<ITransport>();
        transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var client = new Mock<IMeshClient>();
        client.Setup(c => c.ConnectAsync(
                It.IsAny<ITransport>(),
                It.IsAny<string>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        client.Setup(c => c.DisconnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        client.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);

        var joinedGroups = new ConcurrentBag<string>();
        int cAttempts = 0;
        var cRestored = new TaskCompletionSource();

        client.Setup(c => c.JoinGroupAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns<string, CancellationToken>((group, _) =>
            {
                joinedGroups.Add(group);

                if (group == "C")
                {
                    if (Interlocked.Increment(ref cAttempts) == 1)
                    {
                        // Simulate the connection dropping mid-restore: the client reports the groups
                        // still live (A and B, already re-joined) and the join for C then fails.
                        client.Raise(
                            c2 => c2.Disconnected += null,
                            new DisconnectedEventArgs
                            {
                                Reason = DisconnectReason.ConnectionLost,
                                JoinedGroups = GroupsAb,
                            });
                        throw new IOException("connection dropped during restore");
                    }

                    cRestored.TrySetResult();
                }

                return Task.CompletedTask;
            });

        await using var reconnector = new MeshClientReconnector(
            client.Object,
            "Alice",
            _ => Task.FromResult<ITransport>(transport.Object),
            retryDelay: TimeSpan.FromMilliseconds(10));

        await reconnector.StartAsync();

        // First drop: the client was in A, B and C. Restoration re-joins A and B, is interrupted on C,
        // and the follow-up reconnect must restore C rather than losing it.
        client.Raise(
            c => c.Disconnected += null,
            new DisconnectedEventArgs
            {
                Reason = DisconnectReason.ConnectionLost,
                JoinedGroups = GroupsAbc,
            });

        await cRestored.Task.WaitAsync(WaitTimeout);

        Assert.Contains("C", joinedGroups);
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
