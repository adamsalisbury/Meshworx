using System.Buffers.Binary;
using System.Text;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Microsoft.Extensions.Logging;
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
        Assert.Equal(0x03, sentData[1]);
        int nameLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(2, 2));
        Assert.Equal("TestClient", Encoding.UTF8.GetString(sentData.AsSpan(4, nameLength)));
        // No credential supplied, so the name is the whole remaining payload.
        Assert.Equal(4 + nameLength, sentData.Length);
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

    // Send policy (timeout and retry)

    /// <summary>
    /// Connects a freshly built client whose transport yields a registration response and then routes
    /// every send through <paramref name="onSend"/>. The first send is the registration request (send
    /// number 1); subsequent numbers are the sends the test drives.
    /// </summary>
    private static async Task ConnectWithScriptedSendAsync(
        MeshClient client,
        Mock<ITransport> transport,
        Func<int, CancellationToken, Task> onSend)
    {
        var assignedId = Guid.NewGuid();
        var registrationResponse = new byte[17];
        registrationResponse[0] = 0x01; // RegistrationComplete
        assignedId.TryWriteBytes(registrationResponse.AsSpan(1));

        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(registrationResponse);

        transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct));

        int sendCount = 0;
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns<ReadOnlyMemory<byte>, CancellationToken>((_, ct) => onSend(Interlocked.Increment(ref sendCount), ct));

        await client.ConnectAsync(transport.Object, "TestClient");
    }

    /// <summary>
    /// When a send fails with a transient transport error and retries are configured, the client retries
    /// and the send ultimately succeeds.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_TransientFailure_RetriesThenSucceeds()
    {
        var transport = new Mock<ITransport>();
        await using var client = new MeshClient(
            new Mock<ILogger<MeshClient>>().Object,
            maxSendAttempts: 3,
            sendRetryDelay: TimeSpan.FromMilliseconds(1));

        // Send 1 is registration. Data sends are 2, 3, 4: the first two fail transiently, the third works.
        int dataSendCount = 0;
        await ConnectWithScriptedSendAsync(client, transport, (send, _) =>
        {
            if (send == 1)
            {
                return Task.CompletedTask;
            }

            return Interlocked.Increment(ref dataSendCount) <= 2
                ? Task.FromException(new IOException("transient transport failure"))
                : Task.CompletedTask;
        });

        await client.SendAsync(Guid.NewGuid(), new byte[] { 1 });

        Assert.Equal(3, Volatile.Read(ref dataSendCount));
    }

    /// <summary>
    /// With the default policy (a single attempt), a transient transport failure is not retried and
    /// surfaces to the caller immediately, preserving the original fire-and-forget behaviour.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_TransientFailure_DefaultPolicy_DoesNotRetry()
    {
        var transport = new Mock<ITransport>();
        await using var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);

        int dataSendCount = 0;
        await ConnectWithScriptedSendAsync(client, transport, (send, _) =>
        {
            // Fail only the caller's data send (send 2), not registration or the teardown disconnect.
            if (send != 2)
            {
                return Task.CompletedTask;
            }

            Interlocked.Increment(ref dataSendCount);
            return Task.FromException(new IOException("transient transport failure"));
        });

        await Assert.ThrowsAsync<IOException>(() => client.SendAsync(Guid.NewGuid(), new byte[] { 1 }));
        Assert.Equal(1, Volatile.Read(ref dataSendCount));
    }

    /// <summary>
    /// When a send does not complete within the configured send timeout, it is abandoned with a
    /// TimeoutException.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_ExceedsSendTimeout_ThrowsTimeoutException()
    {
        var transport = new Mock<ITransport>();
        await using var client = new MeshClient(
            new Mock<ILogger<MeshClient>>().Object,
            sendTimeout: TimeSpan.FromMilliseconds(100));

        // The data send (send 2) stalls but honours cancellation, so the timeout cancels it; other sends
        // (registration, teardown disconnect) complete normally.
        await ConnectWithScriptedSendAsync(
            client,
            transport,
            (send, ct) => send == 2 ? Task.Delay(Timeout.Infinite, ct) : Task.CompletedTask);

        await Assert.ThrowsAsync<TimeoutException>(() => client.SendAsync(Guid.NewGuid(), new byte[] { 1 }));
    }

    /// <summary>
    /// A cancelled token is honoured while a send is in flight: the send fails with an
    /// OperationCanceledException rather than being retried or timing out.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_TokenCancelledDuringSend_HonoursCancellation()
    {
        var transport = new Mock<ITransport>();
        await using var client = new MeshClient(
            new Mock<ILogger<MeshClient>>().Object,
            sendTimeout: TimeSpan.FromSeconds(30),
            maxSendAttempts: 3,
            sendRetryDelay: TimeSpan.FromMilliseconds(1));

        // The data send (send 2) stalls but honours cancellation; other sends complete normally.
        await ConnectWithScriptedSendAsync(
            client,
            transport,
            (send, ct) => send == 2 ? Task.Delay(Timeout.Infinite, ct) : Task.CompletedTask);

        using var cts = new CancellationTokenSource();
        Task sendTask = client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, cts.Token);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sendTask);
    }

    // BroadcastAsync

    /// <summary>
    /// When BroadcastAsync is called on a client that is not connected to a hub, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task BroadcastAsync_NotConnected_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.BroadcastAsync(new byte[] { 1 }));
    }

    /// <summary>
    /// When BroadcastAsync is called on a connected client, the payload sent to the transport is the
    /// BroadcastMessage type byte followed by the message bytes.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task BroadcastAsync_Connected_SendsBroadcastFrame()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        var message = new byte[] { 9, 8, 7 };
        await fixture.Client.BroadcastAsync(message);

        Assert.NotNull(sentData);
        Assert.Equal(0x0B, sentData[0]); // BroadcastMessage
        Assert.Equal(message, sentData[1..]);
    }

    // Groups

    /// <summary>
    /// When JoinGroupAsync is called on a connected client, the payload sent is the JoinGroup type byte
    /// followed by the UTF-8 group name.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task JoinGroupAsync_Connected_SendsJoinGroupFrame()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        await fixture.Client.JoinGroupAsync("team");

        Assert.NotNull(sentData);
        Assert.Equal(0x0C, sentData[0]); // JoinGroup
        Assert.Equal("team", Encoding.UTF8.GetString(sentData.AsSpan(1)));
    }

    /// <summary>
    /// When SendToGroupAsync is called on a connected client, the payload is the GroupMessage type byte,
    /// a 2-byte big-endian group-name length, the UTF-8 group name, then the message bytes.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendToGroupAsync_Connected_SendsGroupMessageFrame()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        var message = new byte[] { 5, 6 };
        await fixture.Client.SendToGroupAsync("team", message);

        Assert.NotNull(sentData);
        Assert.Equal(0x0E, sentData[0]); // GroupMessage
        int nameLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(1, 2));
        Assert.Equal("team", Encoding.UTF8.GetString(sentData.AsSpan(3, nameLength)));
        Assert.Equal(message, sentData[(3 + nameLength)..]);
    }

    /// <summary>
    /// When a group operation is invoked on a disconnected client, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendToGroupAsync_NotConnected_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.SendToGroupAsync("team", new byte[] { 1 }));
    }

    /// <summary>
    /// When a group operation is invoked with an empty group name, an ArgumentException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task JoinGroupAsync_EmptyGroupName_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.JoinGroupAsync(string.Empty));
    }

    /// <summary>
    /// When the receive loop processes a DeliverGroupMessage frame, the GroupMessageReceived event is
    /// raised with the sender id, the group name, and the message data.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_DeliverGroupMessage_RaisesGroupMessageReceived()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        var message = new byte[] { 1, 2, 3 };
        byte[] nameBytes = Encoding.UTF8.GetBytes("team");

        var frame = new byte[1 + 16 + 2 + nameBytes.Length + message.Length];
        frame[0] = 0x0F; // DeliverGroupMessage
        senderId.TryWriteBytes(frame.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(17, 2), (ushort)nameBytes.Length);
        nameBytes.CopyTo(frame, 19);
        message.CopyTo(frame, 19 + nameBytes.Length);
        fixture.SetupSuccessfulRegistration(frame);

        GroupMessageReceivedEventArgs? args = null;
        fixture.Client.GroupMessageReceived += (_, e) => args = e;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.NotNull(args);
        Assert.Equal(senderId, args.SenderId);
        Assert.Equal("team", args.GroupName);
        Assert.Equal(message, args.Data.ToArray());
    }

    // Connection state and group membership

    /// <summary>
    /// IsConnected reflects the connection lifecycle: false before connecting, true while connected,
    /// and false again after disconnecting.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task IsConnected_ReflectsConnectionState()
    {
        var fixture = new MeshClientFixture();

        Assert.False(fixture.Client.IsConnected);

        await fixture.ConnectAsync();
        Assert.True(fixture.Client.IsConnected);

        await fixture.Client.DisconnectAsync();
        Assert.False(fixture.Client.IsConnected);
    }

    /// <summary>
    /// JoinedGroups reflects the groups the client has joined and left.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task JoinedGroups_TracksJoinsAndLeaves()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        Assert.Empty(fixture.Client.JoinedGroups);

        await fixture.Client.JoinGroupAsync("a");
        await fixture.Client.JoinGroupAsync("b");
        Assert.Equal(2, fixture.Client.JoinedGroups.Count);
        Assert.Contains("a", fixture.Client.JoinedGroups);
        Assert.Contains("b", fixture.Client.JoinedGroups);

        await fixture.Client.LeaveGroupAsync("a");
        Assert.Equal("b", Assert.Single(fixture.Client.JoinedGroups));
    }

    /// <summary>
    /// When the client disconnects, its joined-group membership is cleared.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task JoinedGroups_ClearedOnDisconnect()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();
        await fixture.Client.JoinGroupAsync("a");

        await fixture.Client.DisconnectAsync();

        Assert.Empty(fixture.Client.JoinedGroups);
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

    /// <summary>
    /// When the connection drops while a lookup is in flight, the pending GetClientIdByNameAsync
    /// is faulted rather than hanging indefinitely, even when no cancellation token is supplied.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task GetClientIdByNameAsync_ConnectionDropsWhilePending_ThrowsInsteadOfHanging()
    {
        var fixture = new MeshClientFixture();

        var receiveChannel = Channel.CreateUnbounded<byte[]?>();
        receiveChannel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        int sendCount = 0;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((_, _) =>
            {
                // When the lookup request is sent, simulate the hub closing the connection
                // (null frame) before any lookup response is returned.
                if (Interlocked.Increment(ref sendCount) == 2)
                {
                    receiveChannel.Writer.TryWrite(null);
                }
            })
            .Returns(Task.CompletedTask);

        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await receiveChannel.Reader.ReadAsync(ct).ConfigureAwait(false));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.GetClientIdByNameAsync("Target"));
    }

    // Heartbeat

    /// <summary>
    /// When an idle timeout is configured and no frame arrives from the hub within it, the client
    /// treats the connection as lost and raises Disconnected with the ConnectionLost reason.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task ReceiveLoop_IdleTimeoutElapses_RaisesDisconnectedConnectionLost()
    {
        var fixture = new MeshClientFixture(idleTimeout: TimeSpan.FromMilliseconds(100));
        fixture.SetupSuccessfulRegistration(); // registers, then no further frames arrive

        var reasonTcs = new TaskCompletionSource<DisconnectReason>();
        fixture.Client.Disconnected += (_, e) => reasonTcs.TrySetResult(e.Reason);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        DisconnectReason reason = await reasonTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(DisconnectReason.ConnectionLost, reason);
    }

    /// <summary>
    /// When the hub sends a Ping frame, the client replies with a Pong frame so the hub can
    /// confirm the client is alive.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_HubSendsPing_ClientRepliesWithPong()
    {
        var fixture = new MeshClientFixture();
        byte[] pingFrame = [0x09]; // Ping
        fixture.SetupSuccessfulRegistration(pingFrame);

        var pongTcs = new TaskCompletionSource<byte[]>();
        int sendCount = 0;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                // The first send is the registration request; the next is the Pong reply.
                if (Interlocked.Increment(ref sendCount) >= 2)
                {
                    pongTcs.TrySetResult(data.ToArray());
                }
            })
            .Returns(Task.CompletedTask);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        byte[] pong = await pongTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.Single(pong);
        Assert.Equal(0x0A, pong[0]); // Pong
    }

    // Disconnected event

    /// <summary>
    /// When the hub sends a Disconnect frame, the Disconnected event is raised with the
    /// RemoteDisconnect reason.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_HubSendsDisconnect_RaisesDisconnectedWithRemoteReason()
    {
        var fixture = new MeshClientFixture();
        byte[] disconnectFrame = [0x08]; // Disconnect
        fixture.SetupSuccessfulRegistration(disconnectFrame);

        var reasonTcs = new TaskCompletionSource<DisconnectReason>();
        fixture.Client.Disconnected += (_, e) => reasonTcs.TrySetResult(e.Reason);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        DisconnectReason reason = await reasonTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(DisconnectReason.RemoteDisconnect, reason);
    }

    /// <summary>
    /// When the underlying transport reports the connection closed (ReceiveAsync returns null),
    /// the Disconnected event is raised with the ConnectionLost reason.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_ConnectionClosed_RaisesDisconnectedWithConnectionLostReason()
    {
        var fixture = new MeshClientFixture();

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(fixture.CreateRegistrationResponse())
            .ReturnsAsync((byte[]?)null);

        var reasonTcs = new TaskCompletionSource<DisconnectReason>();
        fixture.Client.Disconnected += (_, e) => reasonTcs.TrySetResult(e.Reason);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        DisconnectReason reason = await reasonTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(DisconnectReason.ConnectionLost, reason);
    }

    /// <summary>
    /// When the connection ends, the Disconnected event carries the groups the client was a member of,
    /// captured before the client clears its membership as it resets.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_ConnectionClosed_DisconnectedCarriesJoinedGroups()
    {
        var fixture = new MeshClientFixture();
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct));

        var argsTcs = new TaskCompletionSource<DisconnectedEventArgs>();
        fixture.Client.Disconnected += (_, e) => argsTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");
        await fixture.Client.JoinGroupAsync("alpha");
        await fixture.Client.JoinGroupAsync("beta");

        // Close the connection: the receive loop reads null and tears the connection down.
        channel.Writer.TryWrite(null);

        DisconnectedEventArgs args = await argsTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(DisconnectReason.ConnectionLost, args.Reason);
        Assert.Equal(2, args.JoinedGroups.Count);
        Assert.Contains("alpha", args.JoinedGroups);
        Assert.Contains("beta", args.JoinedGroups);
    }

    /// <summary>
    /// When the connection is lost remotely, the client resets to a disconnected state so its
    /// Id is cleared and further sends are rejected.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_ConnectionClosed_ResetsToDisconnectedState()
    {
        var fixture = new MeshClientFixture();

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(fixture.CreateRegistrationResponse())
            .ReturnsAsync((byte[]?)null);

        var disconnectedTcs = new TaskCompletionSource();
        fixture.Client.Disconnected += (_, _) => disconnectedTcs.TrySetResult();

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");
        await disconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.Equal(Guid.Empty, fixture.Client.Id);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }));
    }

    /// <summary>
    /// When the application disconnects locally via DisconnectAsync, the Disconnected event is
    /// not raised — it signals only unexpected, remote-initiated disconnects.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisconnectAsync_LocalDisconnect_DoesNotRaiseDisconnected()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        bool eventRaised = false;
        fixture.Client.Disconnected += (_, _) => eventRaised = true;

        await fixture.Client.DisconnectAsync();

        Assert.False(eventRaised);
    }

    /// <summary>
    /// When the receive loop wins the race to claim the teardown at the very moment the application
    /// calls DisconnectAsync, the disconnect the application asked for still wins: the Disconnected
    /// event the loop was about to raise is suppressed rather than reported as a lost connection.
    /// The outcome must not depend on which side of the race gets there first.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisconnectAsync_RacesReceiveLoopTeardown_DoesNotRaiseDisconnected()
    {
        var fixture = new MeshClientFixture();

        var receiveChannel = Channel.CreateUnbounded<byte[]?>();
        receiveChannel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await receiveChannel.Reader.ReadAsync(ct).ConfigureAwait(false));

        // The teardown disposes the transport after it has claimed the connection but before it
        // decides whether to raise Disconnected. Holding that disposal open pins the interleaving
        // exactly, so the race is reproduced deterministically rather than hoped for.
        var teardownClaimed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseTeardown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Transport.Setup(t => t.DisposeAsync())
            .Returns(() =>
            {
                teardownClaimed.TrySetResult();
                return new ValueTask(releaseTeardown.Task);
            });

        var disconnectedRaised = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Client.Disconnected += (_, _) => disconnectedRaised.TrySetResult();

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        // Lose the connection remotely, then wait until the receive loop is inside its teardown.
        receiveChannel.Writer.TryWrite(null);
        await teardownClaimed.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // The application disconnects while that teardown is still in flight: the simultaneous case.
        // The client is no longer Connected, so this returns without doing the teardown itself.
        await fixture.Client.DisconnectAsync();

        // Release the teardown so it runs on to the point at which it would raise the event.
        releaseTeardown.TrySetResult();

        await Task.WhenAny(disconnectedRaised.Task, Task.Delay(TimeSpan.FromSeconds(1)));

        Assert.False(
            disconnectedRaised.Task.IsCompleted,
            "Disconnected was raised for a disconnect the application had already requested.");
    }

    /// <summary>
    /// The claim a DisconnectAsync lays on an in-flight teardown belongs to that connection alone.
    /// A later connection that is genuinely lost must still raise Disconnected, so the suppression
    /// cannot leak forward and silence a real drop.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task Disconnected_AfterAConcurrentDisconnectClaimedATeardown_StillRaisedOnTheNextDrop()
    {
        await using var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);

        // First transport: the outgoing Disconnect frame is held open, which parks DisconnectAsync
        // in the disconnecting state for as long as the test needs.
        var disconnectFrameSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseDisconnectFrame = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstTransport = new Mock<ITransport>();
        firstTransport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
        firstTransport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                if (data.Span[0] != 0x08) // Disconnect
                {
                    return Task.CompletedTask;
                }

                disconnectFrameSent.TrySetResult();
                return releaseDisconnectFrame.Task;
            });
        var firstChannel = Channel.CreateUnbounded<byte[]?>();
        firstChannel.Writer.TryWrite(RegistrationComplete(Guid.NewGuid()));
        firstTransport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await firstChannel.Reader.ReadAsync(ct).ConfigureAwait(false));

        // Second transport: registers, then is lost remotely.
        var secondTransport = new Mock<ITransport>();
        secondTransport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
        secondTransport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var secondChannel = Channel.CreateUnbounded<byte[]?>();
        secondChannel.Writer.TryWrite(RegistrationComplete(Guid.NewGuid()));
        secondTransport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await secondChannel.Reader.ReadAsync(ct).ConfigureAwait(false));

        var reasonTcs = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.Disconnected += (_, e) => reasonTcs.TrySetResult(e.Reason);

        await client.ConnectAsync(firstTransport.Object, "Racer");

        Task localDisconnect = client.DisconnectAsync();
        await disconnectFrameSent.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // A second, redundant DisconnectAsync arrives while the first is still disconnecting and
        // claims the teardown. Nothing consumes that claim, so it must not survive the reconnect.
        await client.DisconnectAsync();

        releaseDisconnectFrame.TrySetResult();
        await localDisconnect;

        await client.ConnectAsync(secondTransport.Object, "Racer");
        secondChannel.Writer.TryWrite(null);

        DisconnectReason reason = await reasonTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(DisconnectReason.ConnectionLost, reason);
    }

    /// <summary>
    /// When a MessageReceived handler disconnects the client, DisconnectAsync must not deadlock by
    /// waiting on the receive loop it is being invoked from.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task DisconnectAsync_CalledFromMessageReceivedHandler_DoesNotDeadlock()
    {
        var fixture = new MeshClientFixture();

        var receiveChannel = Channel.CreateUnbounded<byte[]?>();
        receiveChannel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await receiveChannel.Reader.ReadAsync(ct).ConfigureAwait(false));

        var handlerDone = new TaskCompletionSource();
        fixture.Client.MessageReceived += (_, _) =>
        {
            // A synchronous handler that disconnects in response to a message: this blocks on the
            // receive loop's own task, which previously deadlocked.
            fixture.Client.DisconnectAsync().GetAwaiter().GetResult();
            handlerDone.TrySetResult();
        };

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        // Deliver the message after connect so the handler runs on the assigned receive-loop task.
        receiveChannel.Writer.TryWrite(fixture.CreateDeliverMessagePayload(Guid.NewGuid(), [1]));

        await handlerDone.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(Guid.Empty, fixture.Client.Id);
    }

    /// <summary>
    /// A Disconnected handler may reconnect via ConnectAsync, and the resulting connection is the
    /// current, usable one — subsequent sends go out on the new transport. Covers the documented
    /// reconnect-from-handler contract, including the case where the first loop terminates
    /// synchronously from a buffered disconnect.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task Disconnected_HandlerReconnects_EstablishesUsableConnection()
    {
        await using var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var secondId = Guid.NewGuid();

        // First transport registers, then the hub immediately disconnects.
        var firstTransport = new Mock<ITransport>();
        firstTransport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
        firstTransport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var firstChannel = Channel.CreateUnbounded<byte[]?>();
        firstChannel.Writer.TryWrite(RegistrationComplete(Guid.NewGuid()));
        firstChannel.Writer.TryWrite([0x08]); // Disconnect
        firstTransport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await firstChannel.Reader.ReadAsync(ct).ConfigureAwait(false));

        // Second transport registers, then stays connected.
        var secondTransport = new Mock<ITransport>();
        secondTransport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
        byte[]? lastSent = null;
        secondTransport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((d, _) => lastSent = d.ToArray())
            .Returns(Task.CompletedTask);
        var secondChannel = Channel.CreateUnbounded<byte[]?>();
        secondChannel.Writer.TryWrite(RegistrationComplete(secondId));
        secondTransport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await secondChannel.Reader.ReadAsync(ct).ConfigureAwait(false));

        var reconnectedTcs = new TaskCompletionSource();
        int disconnects = 0;
        client.Disconnected += async (_, _) =>
        {
            if (Interlocked.Increment(ref disconnects) != 1)
            {
                return;
            }

            try
            {
                await client.ConnectAsync(secondTransport.Object, "Rejoiner");
                reconnectedTcs.TrySetResult();
            }
            catch (Exception ex)
            {
                reconnectedTcs.TrySetException(ex);
            }
        };

        await client.ConnectAsync(firstTransport.Object, "Rejoiner");
        await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.Equal(secondId, client.Id);

        await client.SendAsync(Guid.NewGuid(), new byte[] { 7 });
        Assert.NotNull(lastSent);
        Assert.Equal(0x02, lastSent[0]); // SendMessage routed over the second transport
    }

    private static byte[] RegistrationComplete(Guid id)
    {
        var response = new byte[17];
        response[0] = 0x01; // RegistrationComplete
        id.TryWriteBytes(response.AsSpan(1));
        return response;
    }
}
