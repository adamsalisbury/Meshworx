using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

public sealed class MeshHubTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

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

    // ClientConnected / ClientDisconnected events

    /// <summary>
    /// When a client completes registration, the hub raises ClientConnected with the client's id and name.
    /// </summary>
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 2000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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

    // HandleClient — unsupported protocol version

    /// <summary>
    /// When a client sends a registration request with an unsupported protocol version, the hub sends
    /// an Error response containing the UnsupportedProtocolVersion error code.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task HandleClient_UnsupportedProtocolVersion_SendsErrorResponse()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var sentDataTcs = new TaskCompletionSource<byte[]>();
        var disposedTcs = new TaskCompletionSource();

        byte[] badVersion = [0x04, 0xFF, 0x41]; // RegistrationRequest + bad version + 'A'
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

    // HandleClient — client name too long

    /// <summary>
    /// When a client sends a registration request with a name exceeding the maximum allowed length,
    /// the hub sends an Error response containing the ClientNameTooLong error code.
    /// </summary>
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 2000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 5000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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

    // BroadcastMessage

    /// <summary>
    /// When a client broadcasts a message, the hub delivers it to every other registered client but
    /// not back to the sender.
    /// </summary>
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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
    [Fact(Timeout = 1000)]
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

    // HandleClient — heartbeat

    /// <summary>
    /// When heartbeats are enabled and a client stays idle, the hub probes it with a Ping frame.
    /// </summary>
    [Fact(Timeout = 2000)]
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
    [Fact(Timeout = 2000)]
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
    [Fact(Timeout = 5000)]
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
    [Fact(Timeout = 5000)]
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
    [Fact(Timeout = 1000)]
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
