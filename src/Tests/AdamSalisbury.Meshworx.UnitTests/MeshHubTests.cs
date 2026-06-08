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
        byte[] nameBytes = System.Text.Encoding.UTF8.GetBytes(longName);
        var payload = new byte[2 + nameBytes.Length];
        payload[0] = 0x04; // RegistrationRequest
        payload[1] = 0x02; // Protocol version
        nameBytes.CopyTo(payload, 2);

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
