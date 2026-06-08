using System.Text;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

public sealed class MeshClientTests
{
    // Constructor

    /// <summary>
    /// When the MeshClient is constructed with a null logger, an ArgumentNullException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Constructor_NullLogger_ThrowsArgumentNullException()
    {
        await Task.CompletedTask;
        Assert.Throws<ArgumentNullException>(() => new MeshClient(null!));
    }

    // ConnectAsync — argument validation

    /// <summary>
    /// When ConnectAsync is called with a null transport, an ArgumentNullException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_NullTransport_ThrowsArgumentNullException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => fixture.Client.ConnectAsync(null!, "TestClient"));
    }

    /// <summary>
    /// When ConnectAsync is called with a null client name, an ArgumentNullException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_NullClientName_ThrowsArgumentNullException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => fixture.Client.ConnectAsync(fixture.Transport.Object, null!));
    }

    /// <summary>
    /// When ConnectAsync is called with an empty client name, an ArgumentException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_EmptyClientName_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.ConnectAsync(fixture.Transport.Object, string.Empty));
    }

    // ConnectAsync — state validation

    /// <summary>
    /// When ConnectAsync is called but the client is already connected to a hub, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_AlreadyConnected_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var secondTransport = new Mock<ITransport>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.ConnectAsync(secondTransport.Object, "TestClient"));
    }

    // ConnectAsync — registration handshake

    /// <summary>
    /// When ConnectAsync completes the handshake, the first message sent to the transport is a RegistrationRequest containing the client name encoded as UTF-8.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_ValidRegistration_SendsRegistrationRequest()
    {
        var fixture = new MeshClientFixture();
        byte[]? sentData = null;

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        fixture.Transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(fixture.CreateRegistrationResponse())
            .ReturnsAsync((byte[]?)null);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.NotNull(sentData);
        Assert.Equal(0x04, sentData[0]);
        Assert.Equal(0x02, sentData[1]);
        Assert.Equal("TestClient", Encoding.UTF8.GetString(sentData.AsSpan(2)));
    }

    /// <summary>
    /// When ConnectAsync receives a valid RegistrationComplete response, the client's Id property is set to the Guid contained in the response payload.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_ValidRegistration_SetsIdFromResponse()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        Assert.Equal(fixture.AssignedId, fixture.Client.Id);
    }

    /// <summary>
    /// When ConnectAsync completes successfully, the client's Name property is set to the client name that was provided.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_ValidRegistration_SetsName()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync("MyClient");

        Assert.Equal("MyClient", fixture.Client.Name);
    }

    // ConnectAsync — registration failure

    /// <summary>
    /// When the hub returns null during the registration handshake, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_HubReturnsNull_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((byte[]?)null);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient"));
    }

    /// <summary>
    /// When the hub returns a response with an unexpected message type during the registration handshake, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_HubReturnsWrongMessageType_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();
        var badResponse = new byte[17];
        badResponse[0] = 0x02; // SendMessage instead of RegistrationComplete
        Guid.NewGuid().TryWriteBytes(badResponse.AsSpan(1));

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(badResponse);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient"));
    }

    /// <summary>
    /// When the hub returns a response whose payload length does not match the expected 17 bytes, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_HubReturnsWrongPayloadLength_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();
        var shortResponse = new byte[] { 0x01, 0x00 };

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(shortResponse);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient"));
    }

    /// <summary>
    /// When the registration handshake fails, the transport is disposed as part of cleanup.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_RegistrationFails_DisposesTransport()
    {
        var fixture = new MeshClientFixture();
        bool transportDisposed = false;

        fixture.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => transportDisposed = true)
            .Returns(ValueTask.CompletedTask);
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((byte[]?)null);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient"));

        Assert.True(transportDisposed);
    }

    // ConnectAsync — error response

    /// <summary>
    /// When the hub returns an Error response during registration, a RegistrationRefusedException is thrown containing the error code from the payload.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_HubReturnsError_ThrowsRegistrationRefusedException()
    {
        var fixture = new MeshClientFixture();
        byte[] errorResponse = [0x05, 0x01]; // Error + DuplicateClientName

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(errorResponse);

        var exception = await Assert.ThrowsAsync<RegistrationRefusedException>(
            () => fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient"));

        Assert.Equal(RegistrationErrorCode.DuplicateClientName, exception.ErrorCode);
    }

    /// <summary>
    /// When the hub returns an Error response during registration, the transport is disposed as part of cleanup.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_HubReturnsError_DisposesTransport()
    {
        var fixture = new MeshClientFixture();
        bool transportDisposed = false;
        byte[] errorResponse = [0x05, 0x01];

        fixture.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => transportDisposed = true)
            .Returns(ValueTask.CompletedTask);
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(errorResponse);

        await Assert.ThrowsAsync<RegistrationRefusedException>(
            () => fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient"));

        Assert.True(transportDisposed);
    }

    /// <summary>
    /// When the hub returns an Error response with trailing bytes beyond the error code, the client still correctly extracts the error code and throws RegistrationRefusedException.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_HubReturnsErrorWithExtraBytes_ThrowsRegistrationRefusedException()
    {
        var fixture = new MeshClientFixture();
        byte[] errorResponse = [0x05, 0x01, 0xFF, 0xFF]; // extra trailing bytes

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(errorResponse);

        var exception = await Assert.ThrowsAsync<RegistrationRefusedException>(
            () => fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient"));

        Assert.Equal(RegistrationErrorCode.DuplicateClientName, exception.ErrorCode);
    }

    // DisconnectAsync

    /// <summary>
    /// When DisconnectAsync is called on a client that is not connected, it returns without error.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisconnectAsync_NotConnected_ReturnsWithoutError()
    {
        var fixture = new MeshClientFixture();

        await fixture.Client.DisconnectAsync();
    }

    /// <summary>
    /// When DisconnectAsync is called on a connected client, the Id property is reset to Guid.Empty.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisconnectAsync_Connected_ResetsIdToEmpty()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await fixture.Client.DisconnectAsync();

        Assert.Equal(Guid.Empty, fixture.Client.Id);
    }

    /// <summary>
    /// When DisconnectAsync is called on a connected client, the Name property is reset to an empty string.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisconnectAsync_Connected_ResetsNameToEmpty()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await fixture.Client.DisconnectAsync();

        Assert.Equal(string.Empty, fixture.Client.Name);
    }

    /// <summary>
    /// When DisconnectAsync is called on a connected client, the underlying transport is disposed.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisconnectAsync_Connected_DisposesTransport()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await fixture.Client.DisconnectAsync();

        fixture.Transport.Verify(t => t.DisposeAsync(), Times.Once);
    }

    // SendAsync

    /// <summary>
    /// When SendAsync is called on a client that is not connected to a hub, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_NotConnected_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }));
    }

    /// <summary>
    /// When SendAsync is called on a connected client, the payload sent to the transport contains the SendMessage type byte, followed by the recipient Guid, followed by the message bytes.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_Connected_SendsCorrectPayloadFormat()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        var recipientId = Guid.NewGuid();
        var message = new byte[] { 1, 2, 3 };
        await fixture.Client.SendAsync(recipientId, message);

        Assert.NotNull(sentData);
        Assert.Equal(0x02, sentData[0]);
        Assert.Equal(recipientId, new Guid(sentData.AsSpan(1, 16)));
        Assert.Equal(message, sentData[17..]);
    }

    // GetClientIdByNameAsync

    /// <summary>
    /// When GetClientIdByNameAsync is called on a client that is not connected to a hub, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task GetClientIdByNameAsync_NotConnected_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.GetClientIdByNameAsync("test"));
    }

    /// <summary>
    /// When GetClientIdByNameAsync is called with a null name, an ArgumentNullException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task GetClientIdByNameAsync_NullName_ThrowsArgumentNullException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => fixture.Client.GetClientIdByNameAsync(null!));
    }

    /// <summary>
    /// When GetClientIdByNameAsync is called with an empty name, an ArgumentException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task GetClientIdByNameAsync_EmptyName_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.GetClientIdByNameAsync(string.Empty));
    }

    /// <summary>
    /// When the hub responds with a found result, GetClientIdByNameAsync returns the Guid from the response.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task GetClientIdByNameAsync_HubReturnsFound_ReturnsGuid()
    {
        var fixture = new MeshClientFixture();
        var expectedId = Guid.NewGuid();
        fixture.SetupWithLookupResponse(MeshClientFixture.CreateLookupFoundResponse(expectedId));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");
        Guid? result = await fixture.Client.GetClientIdByNameAsync("Other");

        Assert.Equal(expectedId, result);
    }

    /// <summary>
    /// When the hub responds with a not-found result, GetClientIdByNameAsync returns null.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task GetClientIdByNameAsync_HubReturnsNotFound_ReturnsNull()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupWithLookupResponse(MeshClientFixture.CreateLookupNotFoundResponse());

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");
        Guid? result = await fixture.Client.GetClientIdByNameAsync("Unknown");

        Assert.Null(result);
    }

    /// <summary>
    /// When GetClientIdByNameAsync is called, the client sends a ClientLookupRequest containing the requested name encoded as UTF-8.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task GetClientIdByNameAsync_Connected_SendsLookupRequest()
    {
        var fixture = new MeshClientFixture();
        var lookupTcs = new TaskCompletionSource<byte[]?>();
        byte[]? lookupPayload = null;
        int sendCount = 0;

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                if (Interlocked.Increment(ref sendCount) == 2)
                {
                    lookupPayload = data.ToArray();
                    lookupTcs.TrySetResult(MeshClientFixture.CreateLookupNotFoundResponse());
                }
            })
            .Returns(Task.CompletedTask);

        fixture.Transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(fixture.CreateRegistrationResponse())
            .Returns(lookupTcs.Task)
            .ReturnsAsync((byte[]?)null);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");
        await fixture.Client.GetClientIdByNameAsync("Target");

        Assert.NotNull(lookupPayload);
        Assert.Equal(0x06, lookupPayload[0]);
        Assert.Equal("Target", System.Text.Encoding.UTF8.GetString(lookupPayload.AsSpan(5)));
    }

    // ReceiveLoop (tested indirectly via MessageReceived event)

    /// <summary>
    /// When the receive loop processes a valid DeliverMessage frame, the MessageReceived event is raised.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_DeliverMessageReceived_RaisesMessageReceivedEvent()
    {
        var fixture = new MeshClientFixture();
        byte[] deliverPayload = fixture.CreateDeliverMessagePayload(Guid.NewGuid(), [1, 2, 3]);
        fixture.SetupSuccessfulRegistration(deliverPayload);

        bool eventRaised = false;
        fixture.Client.MessageReceived += (_, _) => eventRaised = true;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.True(eventRaised);
    }

    /// <summary>
    /// When the receive loop raises a MessageReceived event, the event args contain the correct sender Guid extracted from the delivered message.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_DeliverMessageReceived_EventContainsCorrectSenderId()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        byte[] deliverPayload = fixture.CreateDeliverMessagePayload(senderId, [1, 2, 3]);
        fixture.SetupSuccessfulRegistration(deliverPayload);

        MessageReceivedEventArgs? receivedArgs = null;
        fixture.Client.MessageReceived += (_, args) => receivedArgs = args;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.NotNull(receivedArgs);
        Assert.Equal(senderId, receivedArgs.SenderId);
    }

    /// <summary>
    /// When the receive loop raises a MessageReceived event, the event args contain the correct message data extracted from the delivered message.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_DeliverMessageReceived_EventContainsCorrectData()
    {
        var fixture = new MeshClientFixture();
        var messageContent = new byte[] { 10, 20, 30 };
        byte[] deliverPayload = fixture.CreateDeliverMessagePayload(Guid.NewGuid(), messageContent);
        fixture.SetupSuccessfulRegistration(deliverPayload);

        MessageReceivedEventArgs? receivedArgs = null;
        fixture.Client.MessageReceived += (_, args) => receivedArgs = args;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.NotNull(receivedArgs);
        Assert.Equal(messageContent, receivedArgs.Data.ToArray());
    }

    /// <summary>
    /// When the receive loop processes a frame with an unrecognised message type, the MessageReceived event is not raised.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_WrongMessageType_DoesNotRaiseEvent()
    {
        var fixture = new MeshClientFixture();
        var wrongTypePayload = new byte[17];
        wrongTypePayload[0] = 0x02; // SendMessage, not DeliverMessage
        Guid.NewGuid().TryWriteBytes(wrongTypePayload.AsSpan(1));
        fixture.SetupSuccessfulRegistration(wrongTypePayload);

        bool eventRaised = false;
        fixture.Client.MessageReceived += (_, _) => eventRaised = true;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.False(eventRaised);
    }

    /// <summary>
    /// When the receive loop processes a frame shorter than 17 bytes, the MessageReceived event is not raised.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_MessageTooShort_DoesNotRaiseEvent()
    {
        var fixture = new MeshClientFixture();
        byte[] shortPayload = [0x03, 0x01, 0x02]; // DeliverMessage type but too short
        fixture.SetupSuccessfulRegistration(shortPayload);

        bool eventRaised = false;
        fixture.Client.MessageReceived += (_, _) => eventRaised = true;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.False(eventRaised);
    }

    /// <summary>
    /// When the receive loop receives null from the transport, the loop exits cleanly without raising an event.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_NullReceived_ExitsLoop()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await fixture.Client.DisconnectAsync();

        fixture.Transport.Verify(t => t.DisposeAsync(), Times.Once);
    }

    // ReceiveLoop — transport errors

    /// <summary>
    /// When the transport throws an IOException during the receive loop, the loop exits cleanly
    /// and the client can be disconnected without error.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_IOExceptionDuringReceive_ExitsLoopCleanly()
    {
        var fixture = new MeshClientFixture();

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        fixture.Transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(fixture.CreateRegistrationResponse())
            .ThrowsAsync(new IOException("Connection reset"));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        // The receive loop has already faulted via IOException — disconnect should still work.
        await fixture.Client.DisconnectAsync();

        Assert.Equal(Guid.Empty, fixture.Client.Id);
    }

    // DisposeAsync

    /// <summary>
    /// When DisposeAsync is called on a connected client, it disconnects from the hub and disposes the transport.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_Connected_Disconnects()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await fixture.Client.DisposeAsync();

        Assert.Equal(Guid.Empty, fixture.Client.Id);
        fixture.Transport.Verify(t => t.DisposeAsync(), Times.Once);
    }

    // Regression tests

    /// <summary>
    /// When the receive loop processes an empty frame, the frame is ignored without faulting the loop,
    /// so a subsequent DeliverMessage is still raised as an event.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_EmptyFrame_IsIgnoredAndLoopContinues()
    {
        var fixture = new MeshClientFixture();
        byte[] deliverPayload = fixture.CreateDeliverMessagePayload(Guid.NewGuid(), [1, 2, 3]);
        fixture.SetupSuccessfulRegistration([], deliverPayload);

        bool eventRaised = false;
        fixture.Client.MessageReceived += (_, _) => eventRaised = true;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.True(eventRaised);
    }

    /// <summary>
    /// When a MessageReceived handler throws, the receive loop survives so subsequent messages
    /// are still delivered to handlers.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_HandlerThrows_DoesNotHaltDelivery()
    {
        var fixture = new MeshClientFixture();
        byte[] firstPayload = fixture.CreateDeliverMessagePayload(Guid.NewGuid(), [1]);
        byte[] secondPayload = fixture.CreateDeliverMessagePayload(Guid.NewGuid(), [2]);
        fixture.SetupSuccessfulRegistration(firstPayload, secondPayload);

        int handlerInvocations = 0;
        fixture.Client.MessageReceived += (_, _) =>
        {
            Interlocked.Increment(ref handlerInvocations);
            throw new InvalidOperationException("Handler failure");
        };

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.Equal(2, handlerInvocations);
    }

    /// <summary>
    /// When a lookup response carries a correlation id that does not match the pending request,
    /// it is discarded, and only the response with the matching correlation id resolves the lookup.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task GetClientIdByNameAsync_StaleCorrelationId_IsDiscarded()
    {
        var fixture = new MeshClientFixture();
        var expectedId = Guid.NewGuid();

        var receiveChannel = Channel.CreateUnbounded<byte[]?>();
        receiveChannel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        int sendCount = 0;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((_, _) =>
            {
                // The first lookup of a fresh client uses correlation id 0.
                if (Interlocked.Increment(ref sendCount) == 2)
                {
                    receiveChannel.Writer.TryWrite(MeshClientFixture.CreateLookupFoundResponse(Guid.NewGuid(), correlationId: 99));
                    receiveChannel.Writer.TryWrite(MeshClientFixture.CreateLookupFoundResponse(expectedId, correlationId: 0));
                }
            })
            .Returns(Task.CompletedTask);

        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await receiveChannel.Reader.ReadAsync(ct).ConfigureAwait(false));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");
        Guid? result = await fixture.Client.GetClientIdByNameAsync("Target");

        Assert.Equal(expectedId, result);
    }
}
