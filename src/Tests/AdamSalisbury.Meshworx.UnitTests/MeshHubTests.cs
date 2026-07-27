using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

public sealed class MeshHubTests
{
    private static readonly TimeSpan WaitTimeout = TestTimeouts.Wait;

    // Constructor

    /// <summary>
    /// When the MeshHub is constructed with a null logger, an ArgumentNullException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_NullLogger_ThrowsArgumentNullException()
    {
        await Task.CompletedTask;
        var listener = new Mock<ITransportListener>();

        Assert.Throws<ArgumentNullException>(() => new MeshHub(null!, listener.Object));
    }

    /// <summary>
    /// When the MeshHub is constructed with a null listener, an ArgumentNullException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_NullListener_ThrowsArgumentNullException()
    {
        await Task.CompletedTask;
        var logger = new Mock<ILogger<MeshHub>>();

        Assert.Throws<ArgumentNullException>(() => new MeshHub(logger.Object, null!));
    }

    // StartAsync

    /// <summary>
    /// When StartAsync is called on a hub that is not running, the underlying transport listener is started.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_NotRunning_StartsListener()
    {
        var fixture = new MeshHubFixture();

        await fixture.Hub.StartAsync();

        fixture.Listener.Verify(l => l.StartAsync(It.IsAny<CancellationToken>()), Times.Once);
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When StartAsync is called but the hub is already running, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_AlreadyRunning_ThrowsInvalidOperationException()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Hub.StartAsync());

        await fixture.Hub.StopAsync();
    }

    // StopAsync

    /// <summary>
    /// When StopAsync is called on a hub that is not running, it returns without error.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StopAsync_NotRunning_ReturnsWithoutError()
    {
        var fixture = new MeshHubFixture();

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When StopAsync is called on a running hub with connected clients, all client connections are disposed.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StopAsync_Running_DisposesAllClientConnections()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        client.Disconnect();
        await fixture.Hub.StopAsync();

        client.Transport.Verify(t => t.DisposeAsync(), Times.AtLeastOnce);
    }

    /// <summary>
    /// When StopAsync is called on a running hub, the client registry is cleared so that no clients are reported as registered.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StopAsync_Running_ClearsClientRegistry()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();

        Assert.False(fixture.Hub.IsClientRegistered(client.Id));
    }

    // StopAsync / DisposeAsync under concurrent invocation

    /// <summary>
    /// When StopAsync is called while a shutdown is already in flight, each connected client is notified
    /// once rather than once per caller.
    /// </summary>
    /// <remarks>
    /// The interleaving is pinned rather than hoped for: holding the client's transport open inside the
    /// hub's disconnect notification parks the first caller in the middle of the shutdown, after it has
    /// claimed the hub's state but before it has finished with it. StopAsync then takes its decision
    /// synchronously, so by the time the second call has handed back a task it has provably either joined
    /// that shutdown or begun one of its own — which is what the frame count distinguishes. No settling
    /// delay is involved.
    /// </remarks>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task StopAsync_CalledWhileAShutdownIsInFlight_NotifiesEachClientOnce()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterMultiMessageClientAsync();

        var notificationReached = new TaskCompletionSource();
        var releaseNotification = new TaskCompletionSource();
        int disconnectFrames = 0;

        ParkOnDisconnectFrame(client.Transport, notificationReached, releaseNotification, () => Interlocked.Increment(ref disconnectFrames));

        // Run the first stop on another thread: before the fix it blocks inside the notification rather
        // than returning a task, so the test would otherwise park itself.
        Task firstStop = Task.Run(() => fixture.Hub.StopAsync());
        await notificationReached.Task.WaitAsync(WaitTimeout);

        Task secondStop = fixture.Hub.StopAsync();

        Assert.Equal(1, Volatile.Read(ref disconnectFrames));

        releaseNotification.SetResult();
        await Task.WhenAll(firstStop, secondStop).WaitAsync(WaitTimeout);

        Assert.Equal(1, Volatile.Read(ref disconnectFrames));
    }

    /// <summary>
    /// When StopAsync is called while a shutdown is already in flight, it returns only once that shutdown
    /// has finished, rather than reporting the hub stopped while it is still stopping.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task StopAsync_CalledWhileAShutdownIsInFlight_ReturnsOnlyOnceTheShutdownCompletes()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterMultiMessageClientAsync();

        var notificationReached = new TaskCompletionSource();
        var releaseNotification = new TaskCompletionSource();

        ParkOnDisconnectFrame(client.Transport, notificationReached, releaseNotification, static () => { });

        Task firstStop = Task.Run(() => fixture.Hub.StopAsync());
        await notificationReached.Task.WaitAsync(WaitTimeout);

        Task secondStop = fixture.Hub.StopAsync();

        // The shutdown is provably still parked, so a completed task here would mean the caller had been
        // told the hub had stopped while it was still tearing itself down.
        Assert.False(secondStop.IsCompleted);

        releaseNotification.SetResult();
        await Task.WhenAll(firstStop, secondStop).WaitAsync(WaitTimeout);

        Assert.Equal(0, fixture.Hub.ConnectedClientCount);
    }

    /// <summary>
    /// When several StopAsync calls run at once against a running hub, every one of them completes without
    /// error and the hub is left stopped.
    /// </summary>
    /// <remarks>
    /// Against the unguarded shutdown this reproduces the defect outright: the caller that finishes first
    /// nulls the token source between another caller's null check and the dereference that follows it, and
    /// the second caller dies with a <see cref="NullReferenceException"/>. It is still a thread race rather
    /// than a pinned interleaving — it went red on all five attempts, but that is not a guarantee — so the
    /// deterministic guard beside it is
    /// <see cref="StopAsync_CalledWhileAShutdownIsInFlight_NotifiesEachClientOnce"/>, which admits no
    /// timing at all.
    /// </remarks>
    [Fact(Timeout = 10000)]
    public async Task StopAsync_CalledConcurrently_AllCallersCompleteWithoutError()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        await fixture.RegisterMultiMessageClientAsync();

        Task[] stops = [.. Enumerable.Range(0, 8).Select(_ => Task.Run(() => fixture.Hub.StopAsync()))];

        await Task.WhenAll(stops).WaitAsync(WaitTimeout);

        Assert.Equal(0, fixture.Hub.ConnectedClientCount);
    }

    /// <summary>
    /// When several DisposeAsync calls run at once, the hub is torn down once rather than once per caller.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task DisposeAsync_CalledConcurrently_TearsTheHubDownOnce()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        await fixture.RegisterMultiMessageClientAsync();

        Task[] disposals =
        [
            .. Enumerable.Range(0, 8).Select(_ => Task.Run(async () => await fixture.Hub.DisposeAsync()))
        ];

        await Task.WhenAll(disposals).WaitAsync(WaitTimeout);

        fixture.Listener.Verify(l => l.DisposeAsync(), Times.Once);
    }

    /// <summary>
    /// When StartAsync is called on a hub that has been disposed, an ObjectDisposedException is thrown
    /// rather than the hub coming back up on a listener that has been torn down.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => fixture.Hub.StartAsync());
    }

    /// <summary>
    /// When the listener fails to start, the hub does not hold on to the running slot it claimed, so it can
    /// be started again once the cause is cleared.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_ListenerFailsToStart_LeavesTheHubStartable()
    {
        var fixture = new MeshHubFixture();
        fixture.Listener.SetupSequence(l => l.StartAsync(It.IsAny<CancellationToken>()))
            .Throws(new InvalidOperationException("The listener could not bind."))
            .Returns(Task.CompletedTask);

        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Hub.StartAsync());

        await fixture.Hub.StartAsync();

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a shutdown completes, the hub releases its own running claim rather than staying wedged as
    /// permanently stopping.
    /// </summary>
    /// <remarks>
    /// This covers the hub's half only, which is all the hub controls. It is deliberately not a claim that
    /// a stopped hub can be restarted in general: <see cref="ITransportListener"/> has no stop, so the hub's
    /// shutdown leaves the endpoint bound, and both listeners in this library throw on a second
    /// <see cref="ITransportListener.StartAsync"/>. The second start below succeeds only because the
    /// fixture's listener is a mock that permits it.
    /// </remarks>
    [Fact(Timeout = 10000)]
    public async Task StopAsync_AfterCompleting_ReleasesTheHubsRunningClaim()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        await fixture.Hub.StopAsync();

        await fixture.Hub.StartAsync();

        fixture.Listener.Verify(l => l.StartAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When StopAsync runs while a start is still in progress, that start completes and the hub is left
    /// running over a listener it still owns, rather than abandoning a listener it has just bound.
    /// </summary>
    /// <remarks>
    /// The interleaving is pinned by gating the listener's own StartAsync, which parks the hub's start at
    /// the exact point where it has claimed the hub but not yet published an accept loop. A stop taking
    /// ownership there would leave the endpoint bound with nothing serving it and no way to recover, since
    /// the listener cannot be started a second time.
    /// </remarks>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task StopAsync_WhileAStartIsInProgress_LeavesTheStartedHubIntact()
    {
        var fixture = new MeshHubFixture();
        var listenerStarting = new TaskCompletionSource();
        var releaseListenerStart = new TaskCompletionSource();

        fixture.Listener.Setup(l => l.StartAsync(It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                listenerStarting.TrySetResult();
                await releaseListenerStart.Task.ConfigureAwait(false);
            });

        Task start = fixture.Hub.StartAsync();
        await listenerStarting.Task.WaitAsync(WaitTimeout);

        await fixture.Hub.StopAsync();

        releaseListenerStart.SetResult();
        await start.WaitAsync(WaitTimeout);

        // The hub is genuinely running, so the client the listener goes on to accept is served.
        var client = await fixture.RegisterClientAsync();
        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// Holds the hub's shutdown open inside the disconnect notification it sends to the given client, so a
    /// concurrent caller can be observed while the shutdown is provably mid-flight.
    /// </summary>
    private static void ParkOnDisconnectFrame(
        Mock<ITransport> transport,
        TaskCompletionSource reached,
        TaskCompletionSource release,
        Action onFrame)
    {
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns<ReadOnlyMemory<byte>, CancellationToken>(async (data, _) =>
            {
                if (data.Length == 0 || data.Span[0] != (byte)MessageType.Disconnect)
                {
                    return;
                }

                onFrame();
                reached.TrySetResult();
                await release.Task.ConfigureAwait(false);
            });
    }

    // ClientConnected / ClientDisconnected events

    /// <summary>
    /// When a client completes registration, the hub raises ClientConnected with the client's id and name.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_ValidRegistration_RaisesClientConnected()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var connectedTcs = new TaskCompletionSource<ClientConnectionEventArgs>();
        fixture.Hub.ClientConnected += (_, e) => connectedTcs.TrySetResult(e);

        var client = await fixture.RegisterClientAsync("Alpha");

        ClientConnectionEventArgs args = await connectedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(client.Id, args.ClientId);
        Assert.Equal("Alpha", args.ClientName);

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a registered client disconnects, the hub raises ClientDisconnected with the client's id and name.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_ClientDisconnects_RaisesClientDisconnected()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync("Beta");

        var disconnectedTcs = new TaskCompletionSource<ClientConnectionEventArgs>();
        fixture.Hub.ClientDisconnected += (_, e) => disconnectedTcs.TrySetResult(e);

        client.Disconnect();

        ClientConnectionEventArgs args = await disconnectedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(client.Id, args.ClientId);
        Assert.Equal("Beta", args.ClientName);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a registration is refused, ClientConnected is not raised because no client was registered.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_RegistrationRejected_DoesNotRaiseClientConnected()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();

        bool connectedRaised = false;
        fixture.Hub.ClientConnected += (_, _) => connectedRaised = true;

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((byte[]?)null);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.False(connectedRaised);

        await fixture.Hub.StopAsync();
    }

    // ConnectedClientCount

    /// <summary>
    /// ConnectedClientCount reflects the number of registered clients, rising as clients register and
    /// falling as they disconnect.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ConnectedClientCount_TracksRegisteredClients()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        Assert.Equal(0, fixture.Hub.ConnectedClientCount);

        var clientA = await fixture.RegisterClientAsync("Alpha");
        var clientB = await fixture.RegisterClientAsync("Beta");

        Assert.Equal(2, fixture.Hub.ConnectedClientCount);

        var disposedTcs = new TaskCompletionSource();
        clientB.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        clientB.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(1, fixture.Hub.ConnectedClientCount);

        clientA.Disconnect();
        await fixture.Hub.StopAsync();

        Assert.Equal(0, fixture.Hub.ConnectedClientCount);
    }

    // IsRunning

    /// <summary>
    /// IsRunning reflects the hub's lifecycle: false before StartAsync, true once it has started, and
    /// false again once StopAsync has completed.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task IsRunning_TracksHubLifecycle()
    {
        var fixture = new MeshHubFixture();

        Assert.False(fixture.Hub.IsRunning);

        await fixture.Hub.StartAsync();
        Assert.True(fixture.Hub.IsRunning);

        await fixture.Hub.StopAsync();
        Assert.False(fixture.Hub.IsRunning);
    }

    // MaxClients

    /// <summary>
    /// MaxClients reflects the value passed to the constructor.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task MaxClients_ConstructedWithExplicitValue_ReflectsThatValue()
    {
        await Task.CompletedTask;
        var fixture = new MeshHubFixture(maxClients: 5);

        Assert.Equal(5, fixture.Hub.MaxClients);
    }

    /// <summary>
    /// MaxClients defaults to one thousand when the constructor is not given an explicit value.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task MaxClients_NotSpecified_DefaultsToOneThousand()
    {
        await Task.CompletedTask;
        var fixture = new MeshHubFixture();

        Assert.Equal(1000, fixture.Hub.MaxClients);
    }

    // IsClientRegistered

    /// <summary>
    /// When IsClientRegistered is called with the identifier of a connected client, it returns true.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task IsClientRegistered_RegisteredClient_ReturnsTrue()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When IsClientRegistered is called with a Guid that does not match any connected client, it returns false.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task IsClientRegistered_UnregisteredClient_ReturnsFalse()
    {
        var fixture = new MeshHubFixture();

        Assert.False(fixture.Hub.IsClientRegistered(Guid.NewGuid()));
    }

    // HandleClient — registration

    /// <summary>
    /// When a client sends a valid RegistrationRequest, the hub registers the client so that IsClientRegistered returns true for the assigned identifier.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task HandleClient_ValidRegistration_RegistersClient()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client sends a valid RegistrationRequest, the hub responds with a RegistrationComplete message.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task HandleClient_ValidRegistration_SendsRegistrationComplete()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        Assert.Equal(0x01, client.RegistrationResponse[0]);

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client sends a valid RegistrationRequest, the RegistrationComplete response contains the Guid assigned to that client.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task HandleClient_ValidRegistration_ResponseContainsClientId()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        var responseId = new Guid(client.RegistrationResponse.AsSpan(1, 16));
        Assert.Equal(client.Id, responseId);

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // HandleClient — registration rejection

    /// <summary>
    /// When a client's transport returns null during the registration read, the transport is disposed and no client is registered.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_NullRegistrationData_DisposesTransport()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((byte[]?)null);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        await disposedTcs.Task.WaitAsync(WaitTimeout);
        transport.Verify(t => t.DisposeAsync(), Times.Once);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client sends a first frame with an unexpected message type, the transport is disposed and no client is registered.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_InvalidMessageType_DisposesTransport()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();

        byte[] badFrame = [0x02, 0x01, 0x02]; // SendMessage type, not RegistrationRequest
        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(badFrame);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        await disposedTcs.Task.WaitAsync(WaitTimeout);
        transport.Verify(t => t.DisposeAsync(), Times.Once);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client sends registration data shorter than three bytes, the transport is disposed and no client is registered.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_RegistrationDataTooShort_DisposesTransport()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();

        byte[] tooShort = [0x04]; // RegistrationRequest type but only 1 byte (< 3)
        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(tooShort);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        await disposedTcs.Task.WaitAsync(WaitTimeout);
        transport.Verify(t => t.DisposeAsync(), Times.Once);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client's registration is rejected for any reason, no entry is added to the client registry.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_RegistrationRejected_ClientNotRegistered()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((byte[]?)null);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        await disposedTcs.Task.WaitAsync(WaitTimeout);

        transport.Verify(
            t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()),
            Times.Never);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When an accepted connection does not send its registration request within the configured
    /// timeout, the hub drops the connection by disposing the transport.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_RegistrationTimesOut_DisposesTransport()
    {
        var fixture = new MeshHubFixture(TimeSpan.FromMilliseconds(100));
        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();

        // The client connects but never sends anything — ReceiveAsync blocks until cancelled.
        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct =>
            {
                await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
                return null;
            });
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        await disposedTcs.Task.WaitAsync(WaitTimeout);
        transport.Verify(t => t.DisposeAsync(), Times.Once);

        await fixture.Hub.StopAsync();
    }

    // HandleClient — duplicate name refusal

    /// <summary>
    /// When a client attempts to register with a name that is already taken, the hub sends an Error response containing the DuplicateClientName error code.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_DuplicateClientName_SendsErrorResponse()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var existing = await fixture.RegisterClientAsync("Alpha");

        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();
        var disposedTcs = new TaskCompletionSource();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Alpha"));
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]);
        Assert.Equal(0x01, sentData[1]);

        existing.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client is refused registration due to a duplicate name, the transport is disposed.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_DuplicateClientName_DisposesTransport()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var existing = await fixture.RegisterClientAsync("Alpha");

        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Alpha"));
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        transport.Verify(t => t.DisposeAsync(), Times.Once);

        existing.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client is refused registration due to a duplicate name, only the original client remains in the registry.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_DuplicateClientName_DoesNotRegisterSecondClient()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var existing = await fixture.RegisterClientAsync("Alpha");

        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Alpha"));
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.True(fixture.Hub.IsClientRegistered(existing.Id));

        existing.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // HandleClient — hub capacity

    /// <summary>
    /// When the hub has reached its configured maximum client count, a further registration is
    /// refused with an Error response carrying the HubAtCapacity error code, and the client is
    /// not added to the registry.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_HubAtCapacity_RefusesRegistration()
    {
        var fixture = new MeshHubFixture(maxClients: 1);
        await fixture.Hub.StartAsync();
        var existing = await fixture.RegisterClientAsync("First");

        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();
        var disposedTcs = new TaskCompletionSource();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Second"));
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]); // Error
        Assert.Equal(0x04, sentData[1]); // HubAtCapacity

        existing.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a concurrent registration has claimed the hub's last client slot but has not yet added
    /// itself to the client registry, a registration reaching the capacity decision at that moment is
    /// refused with HubAtCapacity rather than admitted. The claim, not the observable client count, is
    /// what maxClients is enforced against, so registrations that all read the same count cannot all be
    /// admitted and overshoot the cap.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_LastSlotClaimedButNotYetRegistered_RefusesRatherThanExceedingMaxClients()
    {
        var authenticatorReached = new TaskCompletionSource();
        var releaseAuthenticator = new TaskCompletionSource();
        var fixture = new MeshHubFixture(
            maxClients: 1,
            authenticator: async (_, ct) =>
            {
                authenticatorReached.TrySetResult();
                await releaseAuthenticator.Task.WaitAsync(ct);
                return true;
            });
        await fixture.Hub.StartAsync();

        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();
        var neverReceives = new TaskCompletionSource<byte[]?>();

        transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Racer"))
            .Returns(neverReceives.Task);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        fixture.EnqueueClient(transport.Object);

        // Park the registration inside the authenticator, past the pre-authentication capacity check and
        // immediately before the hub decides whether to admit it.
        await authenticatorReached.Task.WaitAsync(WaitTimeout);

        // Stand in for a registration that has taken the hub's only slot but has not yet put itself in
        // the client registry. ConnectedClientCount is still zero, so a capacity check reading the count
        // would wave the parked registration through and admit a second client to a one-client hub.
        Assert.True(fixture.Hub.TryReserveClientSlot());
        Assert.Equal(0, fixture.Hub.ConnectedClientCount);

        releaseAuthenticator.SetResult();

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]); // Error
        Assert.Equal(0x04, sentData[1]); // HubAtCapacity
        Assert.Equal(0, fixture.Hub.ConnectedClientCount);

        fixture.Hub.ReleaseClientSlot();
        neverReceives.TrySetResult(null);
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a registration claims a client slot and is then refused because its name is already taken,
    /// the slot is given back rather than leaked, so the capacity it briefly held is still available to
    /// the next client.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_RefusedForDuplicateNameAfterClaimingSlot_GivesTheSlotBack()
    {
        var fixture = new MeshHubFixture(maxClients: 2);
        await fixture.Hub.StartAsync();
        var first = await fixture.RegisterClientAsync("First");

        var duplicate = MeshHubFixture.CreateMockTransport();
        var duplicateSentTcs = new TaskCompletionSource<byte[]>();
        var duplicateDisposedTcs = new TaskCompletionSource();

        duplicate.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("First"));
        duplicate.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>(
                (data, _) => duplicateSentTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);
        duplicate.Setup(t => t.DisposeAsync())
            .Callback(() => duplicateDisposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(duplicate.Object);

        byte[] duplicateResponse = await duplicateSentTcs.Task.WaitAsync(WaitTimeout);
        await duplicateDisposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, duplicateResponse[0]); // Error
        Assert.Equal(0x01, duplicateResponse[1]); // DuplicateClientName

        // The second of the hub's two slots was claimed by the refused registration; it must be free
        // again, otherwise the hub is permanently one client short for every name collision it sees.
        var second = await fixture.RegisterClientAsync("Second");
        Assert.Equal(2, fixture.Hub.ConnectedClientCount);

        first.Disconnect();
        second.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client disconnects, the slot it held is given back, so a hub that was at its maximum
    /// client count can admit a replacement.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_ClientDisconnects_GivesItsSlotBackForAReplacement()
    {
        var fixture = new MeshHubFixture(maxClients: 1);
        await fixture.Hub.StartAsync();

        var disconnected = new TaskCompletionSource();
        fixture.Hub.ClientDisconnected += (_, _) => disconnected.TrySetResult();

        var first = await fixture.RegisterClientAsync("First");
        first.Disconnect();

        // The disconnected event is raised after the handler has released the slot, so waiting on it
        // pins the replacement to a point where the slot has definitely been given back.
        await disconnected.Task.WaitAsync(WaitTimeout);

        var replacement = await fixture.RegisterClientAsync("Second");

        Assert.Equal(1, fixture.Hub.ConnectedClientCount);
        Assert.True(fixture.Hub.IsClientRegistered(replacement.Id));

        replacement.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When the hub is constructed with a non-positive maximum client count, an
    /// ArgumentOutOfRangeException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_NonPositiveMaxClients_ThrowsArgumentOutOfRangeException()
    {
        await Task.CompletedTask;
        var logger = new Mock<ILogger<MeshHub>>();
        var listener = new Mock<ITransportListener>();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new MeshHub(logger.Object, listener.Object, maxClients: 0));
    }

    /// <summary>
    /// When the hub is constructed without an explicit maximum client count, the maximum defaults to
    /// 1000 rather than being unlimited, so a hub that never touches maxClients is not left open to
    /// unbounded resource consumption from an unauthenticated flood of registrations. Exercised through
    /// TryReserveClientSlot directly rather than registering 1000 real connections, which would make
    /// this test needlessly slow for what it is checking.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_MaxClientsNotSpecified_DefaultsToOneThousand()
    {
        await Task.CompletedTask;
        var fixture = new MeshHubFixture();

        for (int i = 0; i < 1000; i++)
        {
            Assert.True(fixture.Hub.TryReserveClientSlot());
        }

        Assert.False(fixture.Hub.TryReserveClientSlot());
    }

    /// <summary>
    /// When the hub is constructed without an explicit heartbeat interval, idle eviction defaults to a
    /// 30-second interval rather than being disabled, so a hub that never touches heartbeatInterval no
    /// longer lets a registered client sit silent indefinitely.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_HeartbeatIntervalNotSpecified_DefaultsToThirtySeconds()
    {
        await Task.CompletedTask;
        var fixture = new MeshHubFixture();

        Assert.Equal(TimeSpan.FromSeconds(30), fixture.Hub.GetHeartbeatIntervalForTesting());
    }

    /// <summary>
    /// Passing Timeout.InfiniteTimeSpan for the heartbeat interval is the explicit opt-out from idle
    /// eviction, distinct from simply not specifying one, which now takes the 30-second default.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_HeartbeatIntervalSetToInfinite_DisablesIdleEviction()
    {
        await Task.CompletedTask;
        var fixture = new MeshHubFixture(heartbeatInterval: Timeout.InfiniteTimeSpan);

        Assert.Null(fixture.Hub.GetHeartbeatIntervalForTesting());
    }

    /// <summary>
    /// When the hub is constructed with a non-positive heartbeat interval other than the
    /// Timeout.InfiniteTimeSpan opt-out, an ArgumentOutOfRangeException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_HeartbeatIntervalZero_ThrowsArgumentOutOfRangeException()
    {
        await Task.CompletedTask;
        var logger = new Mock<ILogger<MeshHub>>();
        var listener = new Mock<ITransportListener>();

        Assert.Throws<ArgumentOutOfRangeException>(() => new MeshHub(
            logger.Object, listener.Object, heartbeatInterval: TimeSpan.Zero));
    }

    /// <summary>
    /// When the hub is constructed with a non-positive maximum connections-per-remote-endpoint count,
    /// an ArgumentOutOfRangeException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_NonPositiveMaxConnectionsPerRemoteEndpoint_ThrowsArgumentOutOfRangeException()
    {
        await Task.CompletedTask;
        var logger = new Mock<ILogger<MeshHub>>();
        var listener = new Mock<ITransportListener>();

        Assert.Throws<ArgumentOutOfRangeException>(() => new MeshHub(
            logger.Object, listener.Object, maxConnectionsPerRemoteEndpoint: 0));
    }

    // AcceptLoop — per-remote-endpoint connection cap

    /// <summary>
    /// When the accept loop has already admitted the configured maximum number of connections from a
    /// remote address, a further connection from that same address is closed immediately — without
    /// ever reading a registration frame from it — while a connection from a different address is
    /// unaffected by that address's limit.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task AcceptLoop_TooManyConnectionsFromSameAddress_RefusesFurtherConnectionWithoutHandshake()
    {
        var fixture = new MeshHubFixture(maxConnectionsPerRemoteEndpoint: 1);
        await fixture.Hub.StartAsync();

        var floodedAddress = new IPEndPoint(IPAddress.Parse("203.0.113.10"), 51000);
        var otherAddress = new IPEndPoint(IPAddress.Parse("203.0.113.20"), 51000);

        var first = await fixture.RegisterClientAsync("First", floodedAddress);

        var refused = MeshHubFixture.CreateMockTransport(floodedAddress);
        var refusedDisposedTcs = new TaskCompletionSource();
        refused.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Refused"));
        refused.Setup(t => t.DisposeAsync())
            .Callback(() => refusedDisposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(refused.Object);

        await refusedDisposedTcs.Task.WaitAsync(WaitTimeout);
        refused.Verify(t => t.ReceiveAsync(It.IsAny<CancellationToken>()), Times.Never);

        // A connection from a different address is not affected by the flooded address's limit.
        var second = await fixture.RegisterClientAsync("Second", otherAddress);
        Assert.Equal(2, fixture.Hub.ConnectedClientCount);

        first.Disconnect();
        second.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a connection from a remote address that was at its connection limit disconnects, the slot
    /// it held is given back, so a replacement connection from that same address is then admitted.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task AcceptLoop_ConnectionFromCappedAddressDisconnects_FreesSlotForAnotherFromSameAddress()
    {
        var fixture = new MeshHubFixture(maxConnectionsPerRemoteEndpoint: 1);
        await fixture.Hub.StartAsync();

        var address = new IPEndPoint(IPAddress.Parse("203.0.113.30"), 51000);

        var disconnected = new TaskCompletionSource();
        fixture.Hub.ClientDisconnected += (_, _) => disconnected.TrySetResult();

        var first = await fixture.RegisterClientAsync("First", address);
        first.Disconnect();

        // The disconnected event is raised after the handler has released both the client slot and the
        // per-endpoint slot, so waiting on it pins the replacement to a point where the address's
        // connection is definitely free again.
        await disconnected.Task.WaitAsync(WaitTimeout);

        var replacement = await fixture.RegisterClientAsync("Second", address);

        Assert.Equal(1, fixture.Hub.ConnectedClientCount);
        Assert.True(fixture.Hub.IsClientRegistered(replacement.Id));

        replacement.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A transport that does not implement IRemoteEndPointTransport has no address for the hub to cap,
    /// so any number of such connections are admitted regardless of maxConnectionsPerRemoteEndpoint.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task AcceptLoop_TransportWithoutRemoteEndPoint_IsNeverCappedByAddress()
    {
        var fixture = new MeshHubFixture(maxConnectionsPerRemoteEndpoint: 1);
        await fixture.Hub.StartAsync();

        var first = await fixture.RegisterClientAsync("First");
        var second = await fixture.RegisterClientAsync("Second");

        Assert.Equal(2, fixture.Hub.ConnectedClientCount);

        first.Disconnect();
        second.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// Two IPv6 addresses that differ only in their interface identifier — the low 64 bits, freely
    /// chosen by the host they belong to — share the same /64 network prefix and so are treated as one
    /// source for the connection cap. A single host with a routine /64 allocation cannot defeat the cap
    /// by connecting from a different address within it each time.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task AcceptLoop_TwoIPv6AddressesInSamePrefix_ShareTheConnectionCap()
    {
        var fixture = new MeshHubFixture(maxConnectionsPerRemoteEndpoint: 1);
        await fixture.Hub.StartAsync();

        var firstAddressInPrefix = new IPEndPoint(IPAddress.Parse("2001:db8:1:1::1"), 51000);
        var secondAddressInSamePrefix = new IPEndPoint(IPAddress.Parse("2001:db8:1:1::2"), 51000);

        var first = await fixture.RegisterClientAsync("First", firstAddressInPrefix);

        var refused = MeshHubFixture.CreateMockTransport(secondAddressInSamePrefix);
        var refusedDisposedTcs = new TaskCompletionSource();
        refused.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Refused"));
        refused.Setup(t => t.DisposeAsync())
            .Callback(() => refusedDisposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(refused.Object);

        await refusedDisposedTcs.Task.WaitAsync(WaitTimeout);
        refused.Verify(t => t.ReceiveAsync(It.IsAny<CancellationToken>()), Times.Never);
        Assert.Equal(1, fixture.Hub.ConnectedClientCount);

        first.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// Two IPv6 addresses in different /64 network prefixes are distinct sources, each with its own
    /// share of the connection cap.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task AcceptLoop_TwoIPv6AddressesInDifferentPrefixes_HaveIndependentConnectionCaps()
    {
        var fixture = new MeshHubFixture(maxConnectionsPerRemoteEndpoint: 1);
        await fixture.Hub.StartAsync();

        var firstPrefix = new IPEndPoint(IPAddress.Parse("2001:db8:1:1::1"), 51000);
        var secondPrefix = new IPEndPoint(IPAddress.Parse("2001:db8:2:2::1"), 51000);

        var first = await fixture.RegisterClientAsync("First", firstPrefix);
        var second = await fixture.RegisterClientAsync("Second", secondPrefix);

        Assert.Equal(2, fixture.Hub.ConnectedClientCount);

        first.Disconnect();
        second.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // HandleClient — unsupported protocol version

    /// <summary>
    /// When a client's advertised protocol version range does not overlap the hub's supported range, the
    /// hub sends an Error response containing the UnsupportedProtocolVersion error code.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_UnsupportedProtocolVersion_SendsErrorResponse()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();
        var disposedTcs = new TaskCompletionSource();

        // RegistrationRequest advertising versions 1-2, which do not overlap the hub's version 4.
        byte[] badVersion = [0x04, 0x01, 0x02];
        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(badVersion);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]); // Error
        Assert.Equal(0x02, sentData[1]); // UnsupportedProtocolVersion

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client sends a registration request whose advertised minimum version is greater than its
    /// advertised maximum, the range is malformed and the hub refuses it as an unsupported version rather
    /// than attempting to interpret it.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_InvertedProtocolVersionRange_SendsErrorResponse()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();
        var disposedTcs = new TaskCompletionSource();

        // RegistrationRequest advertising min=5, max=4: an inverted, malformed range.
        byte[] invertedRange = [0x04, 0x05, 0x04];
        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(invertedRange);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]); // Error
        Assert.Equal(0x02, sentData[1]); // UnsupportedProtocolVersion

        await fixture.Hub.StopAsync();
    }

    // HandleClient — protocol version negotiation

    /// <summary>
    /// When a client advertises a version range wider than the hub's own supported range, the hub picks
    /// the highest version common to both — not merely an exact match — and echoes it in the
    /// RegistrationComplete response.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_ProtocolVersionRangeOverlapsHub_NegotiatesHighestSharedVersion()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();
        var blockingReceive = new TaskCompletionSource<byte[]?>();

        // A client that supports every version from the hub's minimum up to one past its maximum, so
        // the hub's own maximum is the highest shared version. The receive loop's next call parks
        // rather than spinning on the same registration frame once the client is admitted.
        byte[] wideRange = MeshHubFixture.CreateRegistrationRequest(
            "Alpha",
            versionMin: Protocol.MinSupportedVersion,
            versionMax: (byte)(Protocol.MaxSupportedVersion + 1));
        transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(wideRange)
            .Returns(blockingReceive.Task);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x01, sentData[0]); // RegistrationComplete
        Assert.Equal(18, sentData.Length);
        Assert.Equal(Protocol.MaxSupportedVersion, sentData[17]); // negotiated version

        blockingReceive.TrySetResult(null);
        await fixture.Hub.StopAsync();
    }

    // HandleClient — client name too long

    /// <summary>
    /// When a client sends a registration request with a name exceeding the maximum allowed length,
    /// the hub sends an Error response containing the ClientNameTooLong error code.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_ClientNameTooLong_SendsErrorResponse()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();
        var disposedTcs = new TaskCompletionSource();

        string longName = new('A', 257);
        byte[] payload = MeshHubFixture.CreateRegistrationRequest(longName);

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(payload);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]); // Error
        Assert.Equal(0x03, sentData[1]); // ClientNameTooLong

        await fixture.Hub.StopAsync();
    }

    // HandleClient — authentication

    /// <summary>
    /// When the hub has an authenticator that rejects the credential, registration is refused with the
    /// AuthenticationFailed error code and the client is not registered.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_AuthenticatorRejects_SendsAuthenticationFailedError()
    {
        var fixture = new MeshHubFixture(authenticator: (_, _) => ValueTask.FromResult(false));
        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Alpha", [0xDE, 0xAD]));
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]); // Error
        Assert.Equal(0x05, sentData[1]); // AuthenticationFailed
        Assert.Equal(0, fixture.Hub.ConnectedClientCount);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When the authenticator hangs, the hub refuses the client once the registration timeout elapses
    /// rather than holding the connection open indefinitely.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_AuthenticatorHangs_RefusesAfterRegistrationTimeout()
    {
        var neverCompletes = new TaskCompletionSource<bool>();
        var fixture = new MeshHubFixture(
            registrationTimeout: TimeSpan.FromMilliseconds(100),
            authenticator: (_, _) => new ValueTask<bool>(neverCompletes.Task));
        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Alpha"));
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]); // Error
        Assert.Equal(0x05, sentData[1]); // AuthenticationFailed
        Assert.Equal(0, fixture.Hub.ConnectedClientCount);

        neverCompletes.TrySetResult(true); // release the abandoned authenticator task
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When the authenticator throws, the client is refused with AuthenticationFailed rather than the
    /// exception faulting the handler.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_AuthenticatorThrows_SendsAuthenticationFailedError()
    {
        var fixture = new MeshHubFixture(
            authenticator: (_, _) => throw new InvalidOperationException("credential store unavailable"));
        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Alpha"));
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]); // Error
        Assert.Equal(0x05, sentData[1]); // AuthenticationFailed
        Assert.Equal(0, fixture.Hub.ConnectedClientCount);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When the authenticator throws a cancellation exception of its own — an outbound identity-provider
    /// call timing out, for example — the client is refused with AuthenticationFailed rather than the
    /// connection being dropped silently as though the hub were shutting down.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_AuthenticatorThrowsOperationCancelled_SendsAuthenticationFailedError()
    {
        var fixture = new MeshHubFixture(
            authenticator: (_, _) => throw new TaskCanceledException("the identity provider timed out"));
        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Alpha"));
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentDataTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        byte[] sentData = await sentDataTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x05, sentData[0]); // Error
        Assert.Equal(0x05, sentData[1]); // AuthenticationFailed
        Assert.Equal(0, fixture.Hub.ConnectedClientCount);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// Only maxConcurrentAuthentications authenticator callbacks run at once, so an unauthenticated peer
    /// cannot drive unbounded concurrent authentication work simply by connecting.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_ManyRegistrations_BoundsConcurrentAuthenticatorCalls()
    {
        const int ConcurrencyLimit = 2;
        const int ClientCount = 8;

        int concurrent = 0;
        int peakConcurrent = 0;
        var release = new TaskCompletionSource();
        var allBlocked = new TaskCompletionSource();

        var fixture = new MeshHubFixture(
            authenticator: async (_, _) =>
            {
                int now = Interlocked.Increment(ref concurrent);
                InterlockedRaiseMax(ref peakConcurrent, now);

                if (now >= ConcurrencyLimit)
                {
                    allBlocked.TrySetResult();
                }

                await release.Task;
                Interlocked.Decrement(ref concurrent);
                return true;
            },
            maxConcurrentAuthentications: ConcurrencyLimit);

        for (int i = 0; i < ClientCount; i++)
        {
            var transport = MeshHubFixture.CreateMockTransport();
            byte[] registration = MeshHubFixture.CreateRegistrationRequest($"Client{i}");
            transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(registration);
            transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            fixture.EnqueueClient(transport.Object);
        }

        await fixture.Hub.StartAsync();
        await allBlocked.Task.WaitAsync(WaitTimeout);

        // Give any unbounded callbacks a chance to pile in before sampling the peak.
        await Task.Delay(100);
        release.TrySetResult();

        Assert.True(
            Volatile.Read(ref peakConcurrent) <= ConcurrencyLimit,
            $"Expected at most {ConcurrencyLimit} concurrent authenticator calls, saw {Volatile.Read(ref peakConcurrent)}.");

        await fixture.Hub.StopAsync();
    }

    private static void InterlockedRaiseMax(ref int target, int value)
    {
        int observed = Volatile.Read(ref target);
        while (observed < value)
        {
            int previous = Interlocked.CompareExchange(ref target, value, observed);
            if (previous == observed)
            {
                return;
            }

            observed = previous;
        }
    }

    /// <summary>
    /// A registration frame declaring a zero-length name is malformed and is dropped, so the empty
    /// string is never reserved in the name registry.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_EmptyClientName_DropsConnectionWithoutRegistering()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();
        var blockingReceive = new TaskCompletionSource<byte[]?>();

        // The empty-name frame first, then a receive that parks. Were the guard to regress, the client
        // would be admitted and its receive loop would park rather than spin on a repeated frame, so
        // this test fails on its assertion instead of hanging the run.
        transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest(string.Empty))
            .Returns(blockingReceive.Task);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0, fixture.Hub.ConnectedClientCount);

        blockingReceive.TrySetResult(null);
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When the hub has an authenticator that accepts, the client is admitted, and the authenticator
    /// is given the client's name and the exact credential bytes it supplied.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_AuthenticatorAccepts_AdmitsClientWithNameAndCredential()
    {
        string? seenName = null;
        byte[]? seenCredential = null;
        var fixture = new MeshHubFixture(authenticator: (context, _) =>
        {
            seenName = context.ClientName;
            seenCredential = context.Credential.ToArray();
            return ValueTask.FromResult(true);
        });

        var transport = MeshHubFixture.CreateMockTransport();
        var registeredTcs = new TaskCompletionSource<byte[]>();
        var blockingReceive = new TaskCompletionSource<byte[]?>();

        // Registration first, then a receive that blocks until the hub is stopped, so the receive
        // loop parks instead of spinning on a repeated frame.
        transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Alpha", [1, 2, 3, 4]))
            .Returns(blockingReceive.Task);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => registeredTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        fixture.EnqueueClient(transport.Object);
        await fixture.Hub.StartAsync();

        byte[] response = await registeredTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x01, response[0]); // RegistrationComplete
        Assert.Equal("Alpha", seenName);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, seenCredential);

        // End the receive loop so the handler completes and StopAsync does not wait on it.
        blockingReceive.TrySetResult(null);
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When two clients register with different names, both are accepted and appear in the registry.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task HandleClient_UniqueClientNames_BothRegister()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var clientA = await fixture.RegisterClientAsync("Alpha");
        var clientB = await fixture.RegisterClientAsync("Beta");

        Assert.True(fixture.Hub.IsClientRegistered(clientA.Id));
        Assert.True(fixture.Hub.IsClientRegistered(clientB.Id));

        clientA.Disconnect();
        clientB.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // HandleClient — client lifecycle

    /// <summary>
    /// When a registered client disconnects by the transport returning null, the client is removed from the registry.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_ClientDisconnects_RemovesFromRegistry()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        client.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.False(fixture.Hub.IsClientRegistered(client.Id));

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a registered client disconnects, its transport connection is disposed.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_ClientDisconnects_DisposesConnection()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        client.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        client.Transport.Verify(t => t.DisposeAsync(), Times.Once);

        await fixture.Hub.StopAsync();
    }

    // RouteMessage

    /// <summary>
    /// When a client sends a message to a registered recipient, the hub delivers a payload containing the DeliverMessage type byte, the sender's Guid, and the original message data.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task RouteMessage_RecipientExists_DeliversCorrectPayload()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var clientA = await fixture.RegisterClientAsync("ClientA");
        var clientB = await fixture.RegisterClientAsync("ClientB");

        var deliveredTcs = new TaskCompletionSource<byte[]>();
        clientB.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => deliveredTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        var messageContent = new byte[] { 10, 20, 30 };
        var sendPayload = new byte[1 + 16 + messageContent.Length];
        sendPayload[0] = 0x02; // SendMessage
        clientB.Id.TryWriteBytes(sendPayload.AsSpan(1));
        messageContent.CopyTo(sendPayload, 17);

        clientA.DisconnectTcs.SetResult(sendPayload);

        byte[] deliveredData = await deliveredTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x03, deliveredData[0]);
        Assert.Equal(messageContent, deliveredData[17..]);

        clientB.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a message is delivered to a recipient, the payload contains the sender's Guid so the recipient knows who sent the message.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task RouteMessage_RecipientExists_PayloadContainsSenderId()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var clientA = await fixture.RegisterClientAsync("ClientA");
        var clientB = await fixture.RegisterClientAsync("ClientB");

        var deliveredTcs = new TaskCompletionSource<byte[]>();
        clientB.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => deliveredTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        var sendPayload = new byte[1 + 16];
        sendPayload[0] = 0x02; // SendMessage
        clientB.Id.TryWriteBytes(sendPayload.AsSpan(1));

        clientA.DisconnectTcs.SetResult(sendPayload);

        byte[] deliveredData = await deliveredTcs.Task.WaitAsync(WaitTimeout);

        var senderId = new Guid(deliveredData.AsSpan(1, 16));
        Assert.Equal(clientA.Id, senderId);

        clientB.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client sends a message to a Guid that does not match any registered client, the message is silently dropped without error.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task RouteMessage_RecipientDoesNotExist_SilentlyDrops()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        var unknownId = Guid.NewGuid();
        var sendPayload = new byte[1 + 16 + 3];
        sendPayload[0] = 0x02; // SendMessage
        unknownId.TryWriteBytes(sendPayload.AsSpan(1));
        new byte[] { 1, 2, 3 }.CopyTo(sendPayload, 17);

        client.DisconnectTcs.SetResult(sendPayload);
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        client.Transport.Verify(
            t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()),
            Times.Once); // only RegistrationComplete, no delivery bounce-back

        await fixture.Hub.StopAsync();
    }

    // RouteMessage — header envelope

    /// <summary>
    /// A header-bearing direct message is forwarded to a recipient negotiated at the header-envelope's
    /// minimum version as a DeliverMessageWithHeaders frame, carrying the header block through unchanged.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task RouteMessageWithHeaders_RecipientNegotiatedCurrentVersion_DeliversWithHeaders()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var clientA = await fixture.RegisterClientAsync("ClientA");
        var clientB = await fixture.RegisterClientAsync("ClientB", versionMin: 5, versionMax: 5);

        var deliveredTcs = new TaskCompletionSource<byte[]>();
        clientB.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => deliveredTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        var messageContent = new byte[] { 10, 20, 30 };
        var headers = new MessageHeaders([new("correlationId", "abc-123")]);
        byte[] sendPayload = MeshHubFixture.CreateDirectMessageWithHeaders(clientB.Id, headers, messageContent);

        clientA.DisconnectTcs.SetResult(sendPayload);

        byte[] deliveredData = await deliveredTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x12, deliveredData[0]); // DeliverMessageWithHeaders
        Assert.Equal(clientA.Id, new Guid(deliveredData.AsSpan(1, 16)));

        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(deliveredData.AsSpan(17, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(deliveredData.AsSpan(19), headerLength);
        Assert.Equal("abc-123", decoded["correlationId"]);
        Assert.Equal(messageContent, deliveredData[(19 + headerLength)..]);

        clientB.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A header-bearing direct message routed to a recipient negotiated below the header-envelope's
    /// minimum version has its header block stripped: the recipient gets the plain DeliverMessage frame
    /// it already understands, rather than an opcode it would not recognise.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task RouteMessageWithHeaders_RecipientNegotiatedOldVersion_StripsHeadersAndDeliversPlainFrame()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var clientA = await fixture.RegisterClientAsync("ClientA");
        var clientB = await fixture.RegisterClientAsync("ClientB"); // negotiates version 4 by default

        var deliveredTcs = new TaskCompletionSource<byte[]>();
        clientB.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => deliveredTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        Assert.Equal(4, clientB.NegotiatedProtocolVersion);

        var messageContent = new byte[] { 10, 20, 30 };
        var headers = new MessageHeaders([new("correlationId", "abc-123")]);
        byte[] sendPayload = MeshHubFixture.CreateDirectMessageWithHeaders(clientB.Id, headers, messageContent);

        clientA.DisconnectTcs.SetResult(sendPayload);

        byte[] deliveredData = await deliveredTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x03, deliveredData[0]); // DeliverMessage — the plain frame, headers stripped
        Assert.Equal(clientA.Id, new Guid(deliveredData.AsSpan(1, 16)));
        Assert.Equal(messageContent, deliveredData[17..]);

        clientB.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A header-bearing direct message addressed to a Guid that does not match any registered client is
    /// silently dropped, mirroring the plain SendMessage behaviour.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task RouteMessageWithHeaders_RecipientDoesNotExist_SilentlyDrops()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        var headers = new MessageHeaders([new("correlationId", "abc-123")]);
        byte[] sendPayload = MeshHubFixture.CreateDirectMessageWithHeaders(Guid.NewGuid(), headers, [1, 2, 3]);

        client.DisconnectTcs.SetResult(sendPayload);
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        client.Transport.Verify(
            t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()),
            Times.Once); // only RegistrationComplete, no delivery bounce-back

        await fixture.Hub.StopAsync();
    }

    // Message expiry (TTL)

    private static string ExpiryHeaderValue(TimeSpan fromNow)
    {
        return DateTimeOffset.UtcNow.Add(fromNow).ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// A direct message whose expiry has already passed is dropped by the send loop rather than
    /// delivered stale. Proved the same way as the other queue-ordering tests: a non-expiring message
    /// queued immediately afterwards on the same connection is what actually arrives, and the expired
    /// one queued ahead of it never does — because the hub processes one client's queued frames in
    /// order, its arrival proves the expired frame was already dealt with.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task SendLoop_ExpiredDirectMessage_IsDroppedNotDelivered()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var recipient = await fixture.RegisterClientAsync("Recipient", versionMin: 5, versionMax: 5);
        var recipientFrames = new FrameRecorder(recipient.Transport);

        var expiredHeaders = new MessageHeaders(
        [
            new(MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds, ExpiryHeaderValue(TimeSpan.FromMinutes(-1))),
        ]);
        sender.EnqueueMessage(
            MeshHubFixture.CreateDirectMessageWithHeaders(recipient.Id, expiredHeaders, [1, 2, 3]));

        var barrierHeaders = new MessageHeaders([new("marker", "barrier")]);
        sender.EnqueueMessage(
            MeshHubFixture.CreateDirectMessageWithHeaders(recipient.Id, barrierHeaders, [9]));

        byte[] delivered = await recipientFrames.WaitForAsync(f => f[0] == 0x12) // DeliverMessageWithHeaders
            .WaitAsync(WaitTimeout);

        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(delivered.AsSpan(17, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(delivered.AsSpan(19), headerLength);
        Assert.Equal("barrier", decoded["marker"]);
        Assert.Equal(new byte[] { 9 }, delivered[(19 + headerLength)..]);

        // Exactly one DeliverMessageWithHeaders frame reached the recipient: the barrier. The expired
        // one queued ahead of it never did.
        Assert.Single(recipientFrames.Frames, f => f[0] == 0x12);

        sender.Disconnect();
        recipient.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A direct message with a time-to-live that has not yet elapsed is delivered exactly as a header-
    /// bearing message without one would be.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task SendLoop_NonExpiredDirectMessage_IsDelivered()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var clientA = await fixture.RegisterClientAsync("ClientA");
        var clientB = await fixture.RegisterClientAsync("ClientB", versionMin: 5, versionMax: 5);

        var deliveredTcs = new TaskCompletionSource<byte[]>();
        clientB.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => deliveredTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        var headers = new MessageHeaders(
        [
            new(MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds, ExpiryHeaderValue(TimeSpan.FromHours(1))),
        ]);
        byte[] sendPayload = MeshHubFixture.CreateDirectMessageWithHeaders(clientB.Id, headers, [4, 5, 6]);

        clientA.DisconnectTcs.SetResult(sendPayload);

        byte[] deliveredData = await deliveredTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x12, deliveredData[0]); // DeliverMessageWithHeaders
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(deliveredData.AsSpan(17, 2));
        Assert.Equal(new byte[] { 4, 5, 6 }, deliveredData[(19 + headerLength)..]);

        clientB.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// An expiry value that parses as a valid integer but falls outside the range DateTimeOffset can
    /// represent is tolerated as "does not expire" rather than crashing the send loop — the header
    /// block is sender-controlled bytes, so a hostile or merely malformed expiry value must not be able
    /// to fault this connection (or, for a shared group frame, every member's connection at once).
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task SendLoop_OutOfRangeExpiryValue_MessageIsDeliveredNotDropped()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var clientA = await fixture.RegisterClientAsync("ClientA");
        var clientB = await fixture.RegisterClientAsync("ClientB", versionMin: 5, versionMax: 5);

        var deliveredTcs = new TaskCompletionSource<byte[]>();
        clientB.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => deliveredTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        var headers = new MessageHeaders(
        [
            new(
                MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds,
                long.MaxValue.ToString(CultureInfo.InvariantCulture)),
        ]);
        byte[] sendPayload = MeshHubFixture.CreateDirectMessageWithHeaders(clientB.Id, headers, [7, 8, 9]);

        clientA.DisconnectTcs.SetResult(sendPayload);

        byte[] deliveredData = await deliveredTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x12, deliveredData[0]); // DeliverMessageWithHeaders — delivered, connection intact
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(deliveredData.AsSpan(17, 2));
        Assert.Equal(new byte[] { 7, 8, 9 }, deliveredData[(19 + headerLength)..]);

        clientB.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// The expiry check applies to group messages too: an already-expired one queued for a member is
    /// dropped, mirroring the direct-message case.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task SendLoop_ExpiredGroupMessage_IsDroppedNotDelivered()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var member = await fixture.RegisterMultiMessageClientAsync("Member", versionMin: 5, versionMax: 5);
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var memberFrames = new FrameRecorder(member.Transport);
        var senderFrames = new FrameRecorder(sender.Transport);

        member.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(member, memberFrames, "Sender");

        sender.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(sender, senderFrames, "Member");

        var expiredHeaders = new MessageHeaders(
        [
            new(MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds, ExpiryHeaderValue(TimeSpan.FromMinutes(-1))),
        ]);
        sender.EnqueueMessage(MeshHubFixture.CreateGroupMessageWithHeaders("team", expiredHeaders, [1, 2, 3]));

        var barrierHeaders = new MessageHeaders([new("marker", "barrier")]);
        sender.EnqueueMessage(MeshHubFixture.CreateGroupMessageWithHeaders("team", barrierHeaders, [9]));

        byte[] delivered = await memberFrames.WaitForAsync(f => f[0] == 0x14) // DeliverGroupMessageWithHeaders
            .WaitAsync(WaitTimeout);

        int nameLength = BinaryPrimitives.ReadUInt16BigEndian(delivered.AsSpan(17, 2));
        int headerLengthOffset = 19 + nameLength;
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(delivered.AsSpan(headerLengthOffset, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(delivered.AsSpan(headerLengthOffset + 2), headerLength);
        Assert.Equal("barrier", decoded["marker"]);

        Assert.Single(memberFrames.Frames, f => f[0] == 0x14);

        member.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // BroadcastMessage

    /// <summary>
    /// When a client broadcasts a message, the hub delivers it to every other registered client but
    /// not back to the sender.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task BroadcastMessage_DeliversToAllOtherClients()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var first = await fixture.RegisterClientAsync("First");
        var second = await fixture.RegisterClientAsync("Second");

        var firstTcs = new TaskCompletionSource<byte[]>();
        first.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((d, _) => firstTcs.TrySetResult(d.ToArray()))
            .Returns(Task.CompletedTask);
        var secondTcs = new TaskCompletionSource<byte[]>();
        second.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((d, _) => secondTcs.TrySetResult(d.ToArray()))
            .Returns(Task.CompletedTask);

        var messageContent = new byte[] { 1, 2, 3 };
        var broadcastFrame = new byte[1 + messageContent.Length];
        broadcastFrame[0] = 0x0B; // BroadcastMessage
        messageContent.CopyTo(broadcastFrame, 1);
        sender.EnqueueMessage(broadcastFrame);

        byte[] firstData = await firstTcs.Task.WaitAsync(WaitTimeout);
        byte[] secondData = await secondTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x03, firstData[0]); // DeliverMessage
        Assert.Equal(sender.Id, new Guid(firstData.AsSpan(1, 16)));
        Assert.Equal(messageContent, firstData[17..]);
        Assert.Equal(0x03, secondData[0]);
        Assert.Equal(messageContent, secondData[17..]);

        // The sender's transport only ever saw its own RegistrationComplete — never the broadcast.
        sender.Transport.Verify(
            t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once);

        sender.Disconnect();
        first.Disconnect();
        second.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // HandleClient — client lookup

    /// <summary>
    /// When the hub receives an empty frame, it is ignored without faulting the receive loop,
    /// so a subsequent SendMessage from the same client is still routed.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_EmptyFrame_IsIgnoredAndRoutingContinues()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var recipient = await fixture.RegisterClientAsync("Recipient");

        var deliveredTcs = new TaskCompletionSource<byte[]>();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => deliveredTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        // An empty frame must be ignored rather than crashing the loop ...
        sender.EnqueueMessage([]);

        // ... so this subsequent message is still routed to the recipient.
        var sendPayload = new byte[1 + 16 + 3];
        sendPayload[0] = 0x02; // SendMessage
        recipient.Id.TryWriteBytes(sendPayload.AsSpan(1));
        new byte[] { 1, 2, 3 }.CopyTo(sendPayload, 17);
        sender.EnqueueMessage(sendPayload);

        byte[] deliveredData = await deliveredTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x03, deliveredData[0]); // DeliverMessage

        sender.Disconnect();
        recipient.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client sends a lookup request for a name that is registered, the hub responds with a found indicator and the matching client's Guid.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_LookupRequestForExistingName_SendsFoundResponse()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var clientA = await fixture.RegisterClientAsync("ClientA");
        var clientB = await fixture.RegisterClientAsync("ClientB");

        var lookupResponseTcs = new TaskCompletionSource<byte[]>();
        clientB.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => lookupResponseTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        byte[] nameBytes = System.Text.Encoding.UTF8.GetBytes("ClientA");
        var lookupRequest = new byte[5 + nameBytes.Length];
        lookupRequest[0] = 0x06; // ClientLookupRequest
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(lookupRequest.AsSpan(1, 4), 42);
        nameBytes.CopyTo(lookupRequest, 5);

        clientB.DisconnectTcs.SetResult(lookupRequest);

        byte[] response = await lookupResponseTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x07, response[0]);
        Assert.Equal(42, System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(response.AsSpan(1, 4)));
        Assert.Equal(0x01, response[5]);
        var foundId = new Guid(response.AsSpan(6, 16));
        Assert.Equal(clientA.Id, foundId);

        clientA.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a client sends a lookup request for a name that is not registered, the hub responds with a not-found indicator.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_LookupRequestForUnknownName_SendsNotFoundResponse()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        var lookupResponseTcs = new TaskCompletionSource<byte[]>();
        client.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => lookupResponseTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        byte[] nameBytes = System.Text.Encoding.UTF8.GetBytes("NobodyHere");
        var lookupRequest = new byte[5 + nameBytes.Length];
        lookupRequest[0] = 0x06;
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(lookupRequest.AsSpan(1, 4), 7);
        nameBytes.CopyTo(lookupRequest, 5);

        client.DisconnectTcs.SetResult(lookupRequest);

        byte[] response = await lookupResponseTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(0x07, response[0]);
        Assert.Equal(7, System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(response.AsSpan(1, 4)));
        Assert.Equal(0x00, response[5]);
        Assert.Equal(6, response.Length);

        await fixture.Hub.StopAsync();
    }

    // HandleClient — message filtering

    /// <summary>
    /// When the hub receives a frame from a client with an unrecognised message type, the frame is ignored and no routing occurs.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_WrongMessageType_IgnoresFrame()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        var badPayload = new byte[17];
        badPayload[0] = 0xFF; // unrecognised message type
        client.DisconnectTcs.SetResult(badPayload);

        await disposedTcs.Task.WaitAsync(WaitTimeout);

        client.Transport.Verify(
            t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()),
            Times.Once); // only RegistrationComplete

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When the hub receives a frame from a client shorter than 17 bytes, the frame is ignored and no routing occurs.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_MessageTooShort_IgnoresFrame()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        byte[] shortPayload = [0x02, 0x01]; // SendMessage type but too short
        client.DisconnectTcs.SetResult(shortPayload);

        await disposedTcs.Task.WaitAsync(WaitTimeout);

        client.Transport.Verify(
            t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()),
            Times.Once); // only RegistrationComplete

        await fixture.Hub.StopAsync();
    }

    // HandleClient — transport errors

    /// <summary>
    /// When a registered client's transport throws an IOException during the receive loop, the client
    /// is removed from the registry and its transport is disposed.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_IOExceptionDuringReceiveLoop_RemovesClientFromRegistry()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterMultiMessageClientAsync();

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        // Reconfigure ReceiveAsync to throw IOException on the next read.
        client.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("Connection reset"));

        // Trigger the reconfigured mock by writing a dummy value into the channel —
        // the channel-based mock was replaced above, so the hub's next ReceiveAsync call
        // will throw IOException.
        client.EnqueueMessage([]);

        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.False(fixture.Hub.IsClientRegistered(client.Id));
        client.Transport.Verify(t => t.DisposeAsync(), Times.AtLeastOnce);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a recipient's transport throws an IOException during message delivery, the recipient
    /// is evicted from the registry and the sender remains connected.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_RecipientTransportFailsDuringRouting_EvictsRecipient()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var recipient = await fixture.RegisterMultiMessageClientAsync("Recipient");

        // Track when the recipient's transport is disposed (indicating eviction).
        var recipientDisposedTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => recipientDisposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        // Make the recipient's transport throw on the next SendAsync (the delivery attempt).
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("Recipient pipe broken"));

        // Send a message from sender to recipient — the IOException is caught by RouteMessageAsync,
        // which evicts the recipient rather than killing the sender.
        var sendPayload = new byte[1 + 16 + 3];
        sendPayload[0] = 0x02; // SendMessage
        recipient.Id.TryWriteBytes(sendPayload.AsSpan(1));
        new byte[] { 1, 2, 3 }.CopyTo(sendPayload, 17);
        sender.EnqueueMessage(sendPayload);

        await recipientDisposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.False(fixture.Hub.IsClientRegistered(recipient.Id));
        Assert.True(fixture.Hub.IsClientRegistered(sender.Id));

        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A transport's SendAsync can reject an oversized delivery frame with an ArgumentException — as
    /// TcpTransport does when a near-maximum-size inbound payload grows past the frame cap once the
    /// hub adds routing fields (and, for a header-bearing message, the header block). This must be
    /// treated exactly like a transport fault: the recipient is evicted with its slot, name and group
    /// memberships released, not left to fault the send loop's awaiting task and skip that cleanup.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_RecipientTransportRejectsOversizedDelivery_EvictsRecipientAndReleasesItsSlot()
    {
        var fixture = new MeshHubFixture(maxClients: 2);
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var recipient = await fixture.RegisterMultiMessageClientAsync("Recipient");

        var recipientDisposedTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => recipientDisposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ArgumentException("Payload size exceeds the maximum frame payload of 1048576 bytes."));

        var sendPayload = new byte[1 + 16 + 3];
        sendPayload[0] = 0x02; // SendMessage
        recipient.Id.TryWriteBytes(sendPayload.AsSpan(1));
        new byte[] { 1, 2, 3 }.CopyTo(sendPayload, 17);
        sender.EnqueueMessage(sendPayload);

        await recipientDisposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.False(fixture.Hub.IsClientRegistered(recipient.Id));
        Assert.True(fixture.Hub.IsClientRegistered(sender.Id));

        // The recipient's claimed slot must have been released too, not just its registry entry
        // removed — otherwise a third client could never register against the 2-client cap.
        var thirdClient = MeshHubFixture.CreateMockTransport();
        var thirdRegisteredTcs = new TaskCompletionSource<byte[]>();
        thirdClient.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(MeshHubFixture.CreateRegistrationRequest("Third"));
        thirdClient.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => thirdRegisteredTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);
        fixture.EnqueueClient(thirdClient.Object);

        byte[] thirdResponse = await thirdRegisteredTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(0x01, thirdResponse[0]); // RegistrationComplete, not HubAtCapacity

        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // HandleClient — heartbeat

    /// <summary>
    /// When heartbeats are enabled and a client stays idle, the hub probes it with a Ping frame.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_IdleClientWithHeartbeatEnabled_SendsPing()
    {
        var fixture = new MeshHubFixture(
            heartbeatInterval: TimeSpan.FromMilliseconds(50), maxMissedHeartbeats: 10);
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterMultiMessageClientAsync("Idle");

        var pingTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                if (data.Length >= 1 && data.Span[0] == 0x09) // Ping
                {
                    pingTcs.TrySetResult();
                }
            })
            .Returns(Task.CompletedTask);

        await pingTcs.Task.WaitAsync(WaitTimeout);

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When heartbeats are enabled and a client never sends any frame in response, the hub evicts it
    /// after the configured number of missed intervals.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_ClientMissesHeartbeats_IsEvicted()
    {
        var fixture = new MeshHubFixture(
            heartbeatInterval: TimeSpan.FromMilliseconds(50), maxMissedHeartbeats: 2);
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterMultiMessageClientAsync("Silent");

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);
        client.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask); // swallow pings, never reply

        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.False(fixture.Hub.IsClientRegistered(client.Id));

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A silent client is evicted on the maxMissedHeartbeats'th consecutive idle interval, not the one
    /// after it. The hub therefore probes it exactly maxMissedHeartbeats minus one times before
    /// evicting, which pins the eviction interval: an extra ping means eviction ran an interval late.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_SilentClient_IsEvictedOnConfiguredIntervalNotTheOneAfter()
    {
        const int MaxMissedHeartbeats = 3;

        // A 100 ms interval rather than 50 ms. The assertion counts pings observed at the transport, so
        // the last ping — enqueued one interval before eviction — must be drained by the send loop
        // before eviction cancels the connection. The wider interval keeps that flush window generous
        // on a loaded runner. The count itself is interval-based, so a stall cannot change it.
        var fixture = new MeshHubFixture(
            heartbeatInterval: TimeSpan.FromMilliseconds(100), maxMissedHeartbeats: MaxMissedHeartbeats);
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterMultiMessageClientAsync("Silent");

        int pingCount = 0;
        int pingsAtEviction = -1;
        var disposedTcs = new TaskCompletionSource();

        client.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                if (data.Length >= 1 && data.Span[0] == 0x09) // Ping
                {
                    Interlocked.Increment(ref pingCount);
                }
            })
            .Returns(Task.CompletedTask); // swallow pings, never reply
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() =>
            {
                // Snapshot inside the eviction teardown so no later send can inflate the count.
                Volatile.Write(ref pingsAtEviction, Volatile.Read(ref pingCount));
                disposedTcs.TrySetResult();
            })
            .Returns(ValueTask.CompletedTask);

        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.False(fixture.Hub.IsClientRegistered(client.Id));
        Assert.Equal(MaxMissedHeartbeats - 1, Volatile.Read(ref pingsAtEviction));

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// With maxMissedHeartbeats set to 1 the client is evicted on the very first idle interval, so the
    /// hub never gets to probe it. This is the documented boundary of the silent-interval count.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task HandleClient_SilentClientWithSingleMissedHeartbeat_IsEvictedWithoutPinging()
    {
        // A 100 ms interval rather than 50 ms: eviction here lands on the very first tick, so the
        // mock callbacks below must be in place before it fires. The wider interval keeps that margin
        // comfortable on a loaded CI runner.
        var fixture = new MeshHubFixture(
            heartbeatInterval: TimeSpan.FromMilliseconds(100), maxMissedHeartbeats: 1);
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterMultiMessageClientAsync("Silent");

        int pingCount = 0;
        int pingsAtEviction = -1;
        var disposedTcs = new TaskCompletionSource();

        client.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                if (data.Length >= 1 && data.Span[0] == 0x09) // Ping
                {
                    Interlocked.Increment(ref pingCount);
                }
            })
            .Returns(Task.CompletedTask);
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() =>
            {
                Volatile.Write(ref pingsAtEviction, Volatile.Read(ref pingCount));
                disposedTcs.TrySetResult();
            })
            .Returns(ValueTask.CompletedTask);

        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.False(fixture.Hub.IsClientRegistered(client.Id));
        Assert.Equal(0, Volatile.Read(ref pingsAtEviction));

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A client that keeps sending frames is never evicted, however many heartbeat intervals pass. The
    /// tightened eviction threshold must not clip clients that are demonstrably alive.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task HandleClient_ClientSendingFramesEveryInterval_IsNotEvicted()
    {
        // maxMissedHeartbeats of 3 rather than the default 2, and a 10 ms send cadence against the
        // 50 ms interval: eviction then needs three consecutive silent intervals (150 ms), so a
        // scheduling stall on a loaded runner cannot masquerade as a genuine eviction.
        var fixture = new MeshHubFixture(
            heartbeatInterval: TimeSpan.FromMilliseconds(50), maxMissedHeartbeats: 3);
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterMultiMessageClientAsync("Chatty");

        // Send a frame well inside every interval for comfortably longer than the eviction window.
        for (int i = 0; i < 30; i++)
        {
            client.EnqueueMessage([0x0A]); // Pong
            await Task.Delay(TimeSpan.FromMilliseconds(10));
        }

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // AcceptLoop — resilience

    /// <summary>
    /// When accepting one connection throws a transient error, the accept loop survives and
    /// continues to accept subsequent clients rather than tearing down the hub.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task AcceptLoop_TransientAcceptFailure_ContinuesAcceptingClients()
    {
        var fixture = new MeshHubFixture();
        fixture.FailNextAccept(new IOException("transient accept failure"));

        await fixture.Hub.StartAsync();

        // The first accept throws; the loop must recover and accept this client.
        var client = await fixture.RegisterClientAsync("AfterFailure");

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // HandleClient — concurrent registration

    /// <summary>
    /// When ten clients register concurrently, all are accepted with unique identifiers and appear
    /// in the registry.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task HandleClient_TenConcurrentRegistrations_AllRegisteredWithUniqueIds()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var registrationTasks = Enumerable.Range(0, 10)
            .Select(i => fixture.RegisterClientAsync($"Client{i}"))
            .ToList();

        RegisteredClient[] clients = await Task.WhenAll(registrationTasks);

        var uniqueIds = clients.Select(c => c.Id).ToHashSet();
        Assert.Equal(10, uniqueIds.Count);

        foreach (RegisteredClient client in clients)
        {
            Assert.True(fixture.Hub.IsClientRegistered(client.Id));
            client.Disconnect();
        }

        await fixture.Hub.StopAsync();
    }

    // RouteMessage — multiple messages

    /// <summary>
    /// When a client sends three messages to another client, all three are delivered in order
    /// with the correct payloads.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task RouteMessage_ThreeConsecutiveMessages_AllDeliveredInOrder()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var clientA = await fixture.RegisterMultiMessageClientAsync("ClientA");
        var clientB = await fixture.RegisterMultiMessageClientAsync("ClientB");

        var deliveredMessages = new List<byte[]>();
        var allDeliveredTcs = new TaskCompletionSource();

        clientB.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                deliveredMessages.Add(data.ToArray());
                if (deliveredMessages.Count == 3)
                {
                    allDeliveredTcs.TrySetResult();
                }
            })
            .Returns(Task.CompletedTask);

        byte[][] messages = [[1, 2], [3, 4, 5], [6]];

        foreach (byte[] messageContent in messages)
        {
            var sendPayload = new byte[1 + 16 + messageContent.Length];
            sendPayload[0] = 0x02; // SendMessage
            clientB.Id.TryWriteBytes(sendPayload.AsSpan(1));
            messageContent.CopyTo(sendPayload, 17);
            clientA.EnqueueMessage(sendPayload);
        }

        await allDeliveredTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(3, deliveredMessages.Count);

        for (int i = 0; i < messages.Length; i++)
        {
            Assert.Equal(0x03, deliveredMessages[i][0]);
            var senderId = new Guid(deliveredMessages[i].AsSpan(1, 16));
            Assert.Equal(clientA.Id, senderId);
            Assert.Equal(messages[i], deliveredMessages[i][17..]);
        }

        clientA.Disconnect();
        clientB.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // Groups as an authorisation boundary

    /// <summary>
    /// Enqueues a lookup on a client's connection and waits for its response. The hub processes one
    /// client's frames in order, so this proves every frame that client enqueued beforehand has been
    /// applied — a barrier that needs no sleeping.
    /// </summary>
    private static async Task ApplyPendingFramesAsync(
        MultiMessageRegisteredClient client, FrameRecorder recorder, string lookupName)
    {
        client.EnqueueMessage(MeshHubFixture.CreateLookupRequest(0, lookupName));
        await recorder.WaitForAsync(f => f[0] == 0x07).WaitAsync(WaitTimeout); // ClientLookupResponse
    }

    /// <summary>
    /// A client that never joined a group cannot send to it: the hub drops the group message rather than
    /// fanning it out to the members. Proved by a direct message sent immediately afterwards on the same
    /// connection — it arrives, and the group message it was queued behind never does.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task SendToGroup_SenderIsNotAMember_MessageIsNotDelivered()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var member = await fixture.RegisterMultiMessageClientAsync("Member");
        var outsider = await fixture.RegisterMultiMessageClientAsync("Outsider");
        var memberFrames = new FrameRecorder(member.Transport);

        member.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(member, memberFrames, "Outsider");

        outsider.EnqueueMessage(MeshHubFixture.CreateGroupMessage("team", [1, 2, 3]));
        outsider.EnqueueMessage(MeshHubFixture.CreateDirectMessage(member.Id, [9]));

        await memberFrames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout); // DeliverMessage
        Assert.DoesNotContain(memberFrames.Frames, f => f[0] == 0x0F); // DeliverGroupMessage

        member.Disconnect();
        outsider.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A client that has joined a group may send to it, and the message reaches the other members. The
    /// counterpart to the non-member case above, so the membership gate cannot pass by rejecting
    /// everything.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task SendToGroup_SenderIsAMember_MessageIsDeliveredToOtherMembers()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var first = await fixture.RegisterMultiMessageClientAsync("First");
        var second = await fixture.RegisterMultiMessageClientAsync("Second");
        var secondFrames = new FrameRecorder(second.Transport);

        second.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(second, secondFrames, "First");

        first.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        first.EnqueueMessage(MeshHubFixture.CreateGroupMessage("team", [1, 2, 3]));

        byte[] delivered = await secondFrames.WaitForAsync(f => f[0] == 0x0F).WaitAsync(WaitTimeout);

        Assert.Equal(first.Id, new Guid(delivered.AsSpan(1, 16)));
        Assert.Equal("team", Encoding.UTF8.GetString(delivered.AsSpan(19, 4)));
        Assert.Equal(new byte[] { 1, 2, 3 }, delivered[23..]);

        first.Disconnect();
        second.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A header-bearing group message fans out with the frame shape each member's own negotiated
    /// protocol version can understand: a member negotiated at the header-envelope's minimum version
    /// gets DeliverGroupMessageWithHeaders with the header block intact, while a member negotiated
    /// below it gets the plain DeliverGroupMessage frame with the headers stripped — built once each,
    /// not per recipient.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task SendToGroupWithHeaders_MixedNegotiatedVersionMembers_EachGetsItsOwnFrameShape()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var currentVersionMember = await fixture.RegisterMultiMessageClientAsync(
            "CurrentVersionMember", versionMin: 5, versionMax: 5);
        var oldVersionMember = await fixture.RegisterMultiMessageClientAsync(
            "OldVersionMember", versionMin: 4, versionMax: 4);
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");

        var currentVersionFrames = new FrameRecorder(currentVersionMember.Transport);
        var oldVersionFrames = new FrameRecorder(oldVersionMember.Transport);

        currentVersionMember.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(currentVersionMember, currentVersionFrames, "Sender");

        oldVersionMember.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(oldVersionMember, oldVersionFrames, "Sender");

        // The sender must itself be a member to send to the group at all; queued on its own connection,
        // ahead of the group message, so it is guaranteed to have been applied first.
        var headers = new MessageHeaders([new("priority", "high")]);
        sender.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        sender.EnqueueMessage(MeshHubFixture.CreateGroupMessageWithHeaders("team", headers, [1, 2, 3]));

        byte[] withHeadersFrame = await currentVersionFrames.WaitForAsync(f => f[0] == 0x14)
            .WaitAsync(WaitTimeout); // DeliverGroupMessageWithHeaders
        byte[] strippedFrame = await oldVersionFrames.WaitForAsync(f => f[0] == 0x0F)
            .WaitAsync(WaitTimeout); // DeliverGroupMessage

        Assert.Equal(sender.Id, new Guid(withHeadersFrame.AsSpan(1, 16)));
        int withHeadersNameLength = BinaryPrimitives.ReadUInt16BigEndian(withHeadersFrame.AsSpan(17, 2));
        Assert.Equal("team", Encoding.UTF8.GetString(withHeadersFrame.AsSpan(19, withHeadersNameLength)));
        int headerLengthOffset = 19 + withHeadersNameLength;
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(withHeadersFrame.AsSpan(headerLengthOffset, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(withHeadersFrame.AsSpan(headerLengthOffset + 2), headerLength);
        Assert.Equal("high", decoded["priority"]);
        Assert.Equal(new byte[] { 1, 2, 3 }, withHeadersFrame[(headerLengthOffset + 2 + headerLength)..]);

        Assert.Equal(sender.Id, new Guid(strippedFrame.AsSpan(1, 16)));
        int strippedNameLength = BinaryPrimitives.ReadUInt16BigEndian(strippedFrame.AsSpan(17, 2));
        Assert.Equal("team", Encoding.UTF8.GetString(strippedFrame.AsSpan(19, strippedNameLength)));
        Assert.Equal(new byte[] { 1, 2, 3 }, strippedFrame[(19 + strippedNameLength)..]);

        currentVersionMember.Disconnect();
        oldVersionMember.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a group authoriser refuses a join, the client is told so, is not admitted to the group, and
    /// therefore receives none of its traffic.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task JoinGroup_AuthoriserRefuses_ClientIsToldAndReceivesNoGroupMessages()
    {
        var fixture = new MeshHubFixture(
            groupAuthoriser: (context, _) => ValueTask.FromResult(context.ClientName == "Insider"));
        await fixture.Hub.StartAsync();

        var insider = await fixture.RegisterMultiMessageClientAsync("Insider");
        var outsider = await fixture.RegisterMultiMessageClientAsync("Outsider");
        var outsiderFrames = new FrameRecorder(outsider.Transport);

        outsider.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("secret"));

        byte[] refusal = await outsiderFrames.WaitForAsync(f => f[0] == 0x10).WaitAsync(WaitTimeout);
        Assert.Equal("secret", Encoding.UTF8.GetString(refusal.AsSpan(1)));

        // The insider joins and sends to the group, then sends the outsider a direct message. The direct
        // message arriving proves the refused outsider was not fanned out to first.
        insider.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("secret"));
        insider.EnqueueMessage(MeshHubFixture.CreateGroupMessage("secret", [1, 2, 3]));
        insider.EnqueueMessage(MeshHubFixture.CreateDirectMessage(outsider.Id, [9]));

        await outsiderFrames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);
        Assert.DoesNotContain(outsiderFrames.Frames, f => f[0] == 0x0F);

        insider.Disconnect();
        outsider.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// An authorised join admits the client to the group, and the authoriser is given the joining
    /// client's hub-assigned id, its registered name, and the group it asked for.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task JoinGroup_AuthoriserAllows_AdmitsClientAndSeesItsIdentity()
    {
        GroupJoinContext? seen = null;
        var fixture = new MeshHubFixture(groupAuthoriser: (context, _) =>
        {
            seen = context;
            return ValueTask.FromResult(true);
        });
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var member = await fixture.RegisterMultiMessageClientAsync("Member");
        var memberFrames = new FrameRecorder(member.Transport);

        member.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(member, memberFrames, "Sender");

        Assert.NotNull(seen);
        Assert.Equal(member.Id, seen.ClientId);
        Assert.Equal("Member", seen.ClientName);
        Assert.Equal("team", seen.GroupName);

        sender.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        sender.EnqueueMessage(MeshHubFixture.CreateGroupMessage("team", [7]));

        byte[] delivered = await memberFrames.WaitForAsync(f => f[0] == 0x0F).WaitAsync(WaitTimeout);
        Assert.Equal(sender.Id, new Guid(delivered.AsSpan(1, 16)));

        sender.Disconnect();
        member.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// An authoriser that throws refuses the join rather than faulting the client handler — the decision
    /// fails closed, and the connection survives.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task JoinGroup_AuthoriserThrows_JoinIsRefused()
    {
        var fixture = new MeshHubFixture(
            groupAuthoriser: (_, _) => throw new InvalidOperationException("policy store unavailable"));
        await fixture.Hub.StartAsync();

        var client = await fixture.RegisterMultiMessageClientAsync("Client");
        var frames = new FrameRecorder(client.Transport);

        client.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));

        byte[] refusal = await frames.WaitForAsync(f => f[0] == 0x10).WaitAsync(WaitTimeout);
        Assert.Equal("team", Encoding.UTF8.GetString(refusal.AsSpan(1)));

        // The connection is still live: the throwing callback refused the join, it did not drop the client.
        await ApplyPendingFramesAsync(client, frames, "Client");
        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// An authoriser that cancels of its own accord — an external policy lookup timing out is the usual
    /// cause — refuses the join rather than unwinding into the receive loop and dropping the connection.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task JoinGroup_AuthoriserCancels_JoinIsRefused()
    {
        var fixture = new MeshHubFixture(
            groupAuthoriser: (_, _) => throw new OperationCanceledException());
        await fixture.Hub.StartAsync();

        var client = await fixture.RegisterMultiMessageClientAsync("Client");
        var frames = new FrameRecorder(client.Transport);

        client.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));

        await frames.WaitForAsync(f => f[0] == 0x10).WaitAsync(WaitTimeout);

        await ApplyPendingFramesAsync(client, frames, "Client");
        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// An authoriser that never returns refuses the join once the group authorisation timeout elapses,
    /// so a hanging integrator callback cannot wedge the calling client's receive loop indefinitely.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task JoinGroup_AuthoriserHangs_JoinIsRefusedAtTheTimeout()
    {
        var neverCompletes = new TaskCompletionSource<bool>();
        var fixture = new MeshHubFixture(
            groupAuthoriser: (_, _) => new ValueTask<bool>(neverCompletes.Task),
            groupAuthorisationTimeout: TimeSpan.FromMilliseconds(100));
        await fixture.Hub.StartAsync();

        var client = await fixture.RegisterMultiMessageClientAsync("Client");
        var frames = new FrameRecorder(client.Transport);

        client.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));

        byte[] refusal = await frames.WaitForAsync(f => f[0] == 0x10).WaitAsync(WaitTimeout);
        Assert.Equal("team", Encoding.UTF8.GetString(refusal.AsSpan(1)));

        // Release the callback so the abandoned task does not outlive the test.
        neverCompletes.TrySetResult(true);

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// Group membership is not carried across connections. A client that reconnects and re-joins — which
    /// is exactly what MeshClientReconnector's membership restoration does — is authorised again on its
    /// own merits, so a restore cannot reinstate a membership the authoriser would now refuse.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task JoinGroup_AfterReconnect_IsAuthorisedAgainRatherThanRestored()
    {
        int memberJoinAttempts = 0;
        var fixture = new MeshHubFixture(groupAuthoriser: (context, _) =>
        {
            // Everyone but the reconnecting client is admitted, so the group still has a member other
            // than the sender at the end and a delivery genuinely happens. Of the reconnecting client's
            // joins, the first is allowed and the re-join refused, so a restore that skipped
            // authorisation would show up as an admitted second membership.
            return context.ClientName == "Member"
                ? ValueTask.FromResult(Interlocked.Increment(ref memberJoinAttempts) == 1)
                : ValueTask.FromResult(true);
        });
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var bystander = await fixture.RegisterMultiMessageClientAsync("Bystander");
        var bystanderFrames = new FrameRecorder(bystander.Transport);

        var first = await fixture.RegisterMultiMessageClientAsync("Member");
        var firstFrames = new FrameRecorder(first.Transport);
        first.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(first, firstFrames, "Sender");

        // Drop the connection and register again under the same name, as a reconnect does.
        first.Disconnect();
        while (fixture.Hub.IsClientRegistered(first.Id))
        {
            await Task.Yield();
        }

        var second = await fixture.RegisterMultiMessageClientAsync("Member");
        var secondFrames = new FrameRecorder(second.Transport);
        second.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));

        byte[] refusal = await secondFrames.WaitForAsync(f => f[0] == 0x10).WaitAsync(WaitTimeout);
        Assert.Equal("team", Encoding.UTF8.GetString(refusal.AsSpan(1)));
        Assert.Equal(2, Volatile.Read(ref memberJoinAttempts));

        // The bystander keeps the group alive, so the send below really does fan out to a member. The
        // refused client must not be among them: its direct message arriving proves no group frame was
        // queued for it first, and the bystander's arrival proves the send was not a no-op.
        bystander.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(bystander, bystanderFrames, "Sender");

        sender.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        sender.EnqueueMessage(MeshHubFixture.CreateGroupMessage("team", [1]));
        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(second.Id, [9]));

        await bystanderFrames.WaitForAsync(f => f[0] == 0x0F).WaitAsync(WaitTimeout);
        await secondFrames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);
        Assert.DoesNotContain(secondFrames.Frames, f => f[0] == 0x0F);

        sender.Disconnect();
        bystander.Disconnect();
        second.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A refusal echoes the group-name bytes the client sent rather than re-encoding the string they
    /// were decoded from. Re-encoding is not size-preserving: a byte that is not valid UTF-8 decodes to
    /// U+FFFD and encodes back as three, so a name of invalid bytes would triple. Group names are not
    /// length-capped, so a tripled refusal can exceed the transport's maximum payload and throw on send,
    /// faulting the send loop — and a faulted send loop is awaited during the connection's teardown,
    /// abandoning the rest of it including the release of the client's slot.
    /// <para>
    /// What is asserted is the invariant that prevents all of that: the refusal is exactly the inbound
    /// frame's length and carries its bytes verbatim. The cap itself is deliberately not exercised here —
    /// the fixture drives the hub through a mocked transport that enforces no size limit — so this test
    /// does not depend on what that limit happens to be. The name is nonetheless sized so that tripling
    /// it would breach the 1 MiB one, which is what makes the invariant worth pinning.
    /// </para>
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task JoinGroup_AuthoriserRefusesAnInvalidUtf8Name_RefusalEchoesTheNameWithoutGrowingIt()
    {
        var fixture = new MeshHubFixture(groupAuthoriser: (_, _) => ValueTask.FromResult(false));
        await fixture.Hub.StartAsync();

        var client = await fixture.RegisterMultiMessageClientAsync("Client");
        var frames = new FrameRecorder(client.Transport);

        byte[] nameBytes = new byte[400_000];
        Array.Fill(nameBytes, (byte)0xFF);
        var joinFrame = new byte[1 + nameBytes.Length];
        joinFrame[0] = 0x0C; // JoinGroup
        nameBytes.CopyTo(joinFrame, 1);

        client.EnqueueMessage(joinFrame);

        byte[] refusal = await frames.WaitForAsync(f => f[0] == 0x10).WaitAsync(WaitTimeout);

        Assert.Equal(joinFrame.Length, refusal.Length);
        Assert.Equal(nameBytes, refusal[1..]);

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A refusal revokes an existing membership rather than merely declining to add one. Re-joining a
    /// group the client is already in is legal and idempotent, so an authoriser that has since changed
    /// its mind must be able to use it to revoke: leaving the earlier membership in place would mean a
    /// deliberate refusal left the client still receiving the group's traffic.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task JoinGroup_AuthoriserRefusesAnExistingMember_RevokesTheMembership()
    {
        bool allowJoins = true;
        var fixture = new MeshHubFixture(
            groupAuthoriser: (_, _) => ValueTask.FromResult(Volatile.Read(ref allowJoins)));
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var member = await fixture.RegisterMultiMessageClientAsync("Member");
        var memberFrames = new FrameRecorder(member.Transport);

        // The member joins while joins are allowed, and the sender joins so it may send at all.
        member.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        sender.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(member, memberFrames, "Sender");

        // Policy changes, and the member re-joins the group it is already in. The refusal must take the
        // membership away, not leave it standing.
        Volatile.Write(ref allowJoins, false);
        member.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await memberFrames.WaitForAsync(f => f[0] == 0x10).WaitAsync(WaitTimeout);

        sender.EnqueueMessage(MeshHubFixture.CreateGroupMessage("team", [1]));
        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(member.Id, [9]));

        await memberFrames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);
        Assert.DoesNotContain(memberFrames.Frames, f => f[0] == 0x0F);

        sender.Disconnect();
        member.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// With no group authoriser configured, joins are unrestricted — the default behaviour is unchanged.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task JoinGroup_NoAuthoriser_AdmitsAnyClient()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var member = await fixture.RegisterMultiMessageClientAsync("Member");
        var memberFrames = new FrameRecorder(member.Transport);

        member.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        await ApplyPendingFramesAsync(member, memberFrames, "Sender");

        sender.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("team"));
        sender.EnqueueMessage(MeshHubFixture.CreateGroupMessage("team", [4]));

        await memberFrames.WaitForAsync(f => f[0] == 0x0F).WaitAsync(WaitTimeout);
        Assert.DoesNotContain(memberFrames.Frames, f => f[0] == 0x10);

        sender.Disconnect();
        member.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When the MeshHub is constructed with a non-positive group authorisation timeout, an
    /// ArgumentOutOfRangeException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_NonPositiveGroupAuthorisationTimeout_ThrowsArgumentOutOfRangeException()
    {
        await Task.CompletedTask;
        var logger = new Mock<ILogger<MeshHub>>();
        var listener = new Mock<ITransportListener>();

        Assert.Throws<ArgumentOutOfRangeException>(() => new MeshHub(
            logger.Object, listener.Object, groupAuthorisationTimeout: TimeSpan.Zero));
    }

    // DisposeAsync

    /// <summary>
    /// When DisposeAsync is called on a running hub, the hub is stopped and all client connections are cleaned up.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_Running_StopsHub()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        client.Disconnect();
        await fixture.Hub.DisposeAsync();

        Assert.False(fixture.Hub.IsClientRegistered(client.Id));
    }

    /// <summary>
    /// When DisposeAsync is called, the underlying transport listener is disposed.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_Running_DisposesListener()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        await fixture.Hub.DisposeAsync();

        fixture.Listener.Verify(l => l.DisposeAsync(), Times.Once);
    }
}
