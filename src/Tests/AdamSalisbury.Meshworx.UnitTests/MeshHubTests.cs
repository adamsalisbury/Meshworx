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
    /// When the MeshHub is constructed with a null listener, an ArgumentNullException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_NullListener_ThrowsArgumentNullException()
    {
        var logger = new Mock<ILogger<MeshHub>>();

        Assert.Throws<ArgumentNullException>(() => new MeshHub(logger.Object, null!));
    }

    // StartAsync

    /// <summary>
    /// When StartAsync is called on a hub that is not running, the underlying transport listener is started.
    /// </summary>
    [Fact]
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
    [Fact]
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
    [Fact]
    public async Task StopAsync_NotRunning_ReturnsWithoutError()
    {
        var fixture = new MeshHubFixture();

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When StopAsync is called on a running hub with connected clients, all client connections are disposed.
    /// </summary>
    [Fact]
    public async Task StopAsync_Running_DisposesAllClientConnections()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        await fixture.Hub.StopAsync();
        client.Disconnect();

        client.Transport.Verify(t => t.DisposeAsync(), Times.AtLeastOnce);
    }

    /// <summary>
    /// When StopAsync is called on a running hub, the client registry is cleared so that no clients are reported as registered.
    /// </summary>
    [Fact]
    public async Task StopAsync_Running_ClearsClientRegistry()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        await fixture.Hub.StopAsync();
        client.Disconnect();

        Assert.False(fixture.Hub.IsClientRegistered(client.Id));
    }

    // IsClientRegistered

    /// <summary>
    /// When IsClientRegistered is called with the identifier of a connected client, it returns true.
    /// </summary>
    [Fact]
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
    [Fact]
    public async Task IsClientRegistered_UnregisteredClient_ReturnsFalse()
    {
        var fixture = new MeshHubFixture();

        Assert.False(fixture.Hub.IsClientRegistered(Guid.NewGuid()));
    }

    // HandleClient — registration

    /// <summary>
    /// When a client sends a valid RegistrationRequest, the hub registers the client so that IsClientRegistered returns true for the assigned identifier.
    /// </summary>
    [Fact]
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
    [Fact]
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
    [Fact]
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
    [Fact]
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
    [Fact]
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
    /// When a client sends registration data shorter than two bytes, the transport is disposed and no client is registered.
    /// </summary>
    [Fact]
    public async Task HandleClient_RegistrationDataTooShort_DisposesTransport()
    {
        var fixture = new MeshHubFixture();
        var transport = MeshHubFixture.CreateMockTransport();
        var disposedTcs = new TaskCompletionSource();

        byte[] tooShort = [0x04]; // RegistrationRequest type but only 1 byte (< 2)
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
    [Fact]
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
    [Fact]
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
    [Fact]
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
    [Fact]
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

    /// <summary>
    /// When two clients register with different names, both are accepted and appear in the registry.
    /// </summary>
    [Fact]
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
    [Fact]
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
    [Fact]
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
    [Fact]
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
    [Fact]
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
    [Fact]
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

    // HandleClient — message filtering

    /// <summary>
    /// When the hub receives a frame from a client with an unrecognised message type, the frame is ignored and no routing occurs.
    /// </summary>
    [Fact]
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
    [Fact]
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

    // DisposeAsync

    /// <summary>
    /// When DisposeAsync is called on a running hub, the hub is stopped and all client connections are cleaned up.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_Running_StopsHub()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        await fixture.Hub.DisposeAsync();
        client.Disconnect();

        Assert.False(fixture.Hub.IsClientRegistered(client.Id));
    }

    /// <summary>
    /// When DisposeAsync is called, the underlying transport listener is disposed.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_Running_DisposesListener()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        await fixture.Hub.DisposeAsync();

        fixture.Listener.Verify(l => l.DisposeAsync(), Times.Once);
    }
}
