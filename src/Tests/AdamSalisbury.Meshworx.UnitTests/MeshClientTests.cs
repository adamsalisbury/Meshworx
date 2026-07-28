using System.Buffers.Binary;
using System.Globalization;
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
        Assert.Equal(Protocol.MinSupportedVersion, sentData[1]);
        Assert.Equal(Protocol.MaxSupportedVersion, sentData[2]);
        int nameLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(3, 2));
        Assert.Equal("TestClient", Encoding.UTF8.GetString(sentData.AsSpan(5, nameLength)));
        // No credential supplied, so the name is the whole remaining payload.
        Assert.Equal(5 + nameLength, sentData.Length);
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

    /// <summary>
    /// When ConnectAsync completes successfully, the client's NegotiatedProtocolVersion property is set
    /// to the version the hub echoed in its RegistrationComplete response.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_ValidRegistration_SetsNegotiatedProtocolVersion()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        Assert.Equal(Protocol.MaxSupportedVersion, fixture.Client.NegotiatedProtocolVersion);
    }

    /// <summary>
    /// When the hub echoes a version lower than the client's own maximum — as an older hub negotiating
    /// down would — the client accepts the handshake and records the negotiated version rather than the
    /// version it originally advertised.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_HubNegotiatesDownToOlderVersion_RecordsNegotiatedVersion()
    {
        var fixture = new MeshClientFixture();
        const byte olderHubVersion = 2;

        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion(olderHubVersion);
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.Equal(olderHubVersion, fixture.Client.NegotiatedProtocolVersion);
        Assert.Equal(fixture.AssignedId, fixture.Client.Id);
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
    /// When the hub returns a response whose payload length does not match the expected 18 bytes, an InvalidOperationException is thrown.
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

    /// <summary>
    /// Calling the headers overload with <see cref="MessageHeaders.Empty"/> writes exactly the same
    /// frame as the plain overload — no header block, byte-for-byte identical — so a message sent
    /// without headers costs nothing extra over today's frame.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_EmptyHeadersOverload_ProducesByteIdenticalPayloadToPlainOverload()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var recipientId = Guid.NewGuid();
        var message = new byte[] { 1, 2, 3 };

        byte[]? plainPayload = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => plainPayload = data.ToArray())
            .Returns(Task.CompletedTask);
        await fixture.Client.SendAsync(recipientId, message);

        byte[]? headersPayload = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => headersPayload = data.ToArray())
            .Returns(Task.CompletedTask);
        await fixture.Client.SendAsync(recipientId, message, MessageHeaders.Empty);

        Assert.Equal(plainPayload, headersPayload);
    }

    /// <summary>
    /// When SendAsync is called with a non-empty MessageHeaders, the payload uses the
    /// SendMessageWithHeaders type, followed by the recipient Guid, the header-block length, the
    /// encoded header block, then the message bytes.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_WithHeaders_SendsSendMessageWithHeadersFrame()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        var recipientId = Guid.NewGuid();
        var message = new byte[] { 1, 2, 3 };
        var headers = new MessageHeaders([new("correlationId", "abc-123")]);
        await fixture.Client.SendAsync(recipientId, message, headers);

        Assert.NotNull(sentData);
        Assert.Equal(0x11, sentData[0]); // SendMessageWithHeaders
        Assert.Equal(recipientId, new Guid(sentData.AsSpan(1, 16)));

        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(17, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(sentData.AsSpan(19), headerLength);
        Assert.Equal("abc-123", decoded["correlationId"]);
        Assert.Equal(message, sentData[(19 + headerLength)..]);
    }

    /// <summary>
    /// When the hub negotiated a protocol version that predates the header envelope, attaching headers
    /// throws rather than silently sending them without a body the hub understands, or silently
    /// dropping them.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_WithHeadersOnOldNegotiatedVersion_ThrowsNotSupportedException()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion(4);
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        var headers = new MessageHeaders([new("correlationId", "abc-123")]);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, headers));
    }

    /// <summary>
    /// Passing a null MessageHeaders throws rather than being treated as empty, so a caller cannot
    /// mistake a missing argument for an explicit "no headers" choice.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_NullHeaders_ThrowsArgumentNullException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, null!));
    }

    /// <summary>
    /// Headers whose combined encoded length exceeds what the wire format's 2-byte block-length prefix
    /// can represent are rejected before anything is sent, rather than silently truncating the length
    /// written to the wire and corrupting the frame for the recipient.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_HeadersAggregateTooLarge_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        string largeValue = new('v', 65000);
        var headers = new MessageHeaders(
        [
            new("first", largeValue),
            new("second", largeValue),
        ]);

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, headers));
    }

    /// <summary>
    /// The request/response helper's correlation-id header key is reserved: a caller that happens to
    /// set it directly is rejected loudly rather than having the receive loop silently swallow a
    /// perfectly ordinary message on the recipient's side because it coincidentally looked like RPC
    /// plumbing.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_HeadersContainReservedCorrelationIdKey_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var headers = new MessageHeaders([new(RequestReplyHeaderKeys.CorrelationId, "1")]);

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, headers));
    }

    /// <summary>
    /// As above, for the reply-flag header key.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_HeadersContainReservedReplyKey_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var headers = new MessageHeaders([new(RequestReplyHeaderKeys.Reply, "1")]);

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, headers));
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
        var registrationResponse = new byte[18];
        registrationResponse[0] = 0x01; // RegistrationComplete
        assignedId.TryWriteBytes(registrationResponse.AsSpan(1, 16));
        registrationResponse[17] = Protocol.MaxSupportedVersion;

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
    /// Calling the headers overload with <see cref="MessageHeaders.Empty"/> writes exactly the same
    /// frame as the plain overload, so a group message sent without headers costs nothing extra.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendToGroupAsync_EmptyHeadersOverload_ProducesByteIdenticalPayloadToPlainOverload()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var message = new byte[] { 5, 6 };

        byte[]? plainPayload = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => plainPayload = data.ToArray())
            .Returns(Task.CompletedTask);
        await fixture.Client.SendToGroupAsync("team", message);

        byte[]? headersPayload = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => headersPayload = data.ToArray())
            .Returns(Task.CompletedTask);
        await fixture.Client.SendToGroupAsync("team", message, MessageHeaders.Empty);

        Assert.Equal(plainPayload, headersPayload);
    }

    /// <summary>
    /// When SendToGroupAsync is called with a non-empty MessageHeaders, the payload uses the
    /// GroupMessageWithHeaders type, the group name, the header-block length, the encoded header
    /// block, then the message bytes.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendToGroupAsync_WithHeaders_SendsGroupMessageWithHeadersFrame()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        var message = new byte[] { 5, 6 };
        var headers = new MessageHeaders([new("priority", "high")]);
        await fixture.Client.SendToGroupAsync("team", message, headers);

        Assert.NotNull(sentData);
        Assert.Equal(0x13, sentData[0]); // GroupMessageWithHeaders
        int nameLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(1, 2));
        Assert.Equal("team", Encoding.UTF8.GetString(sentData.AsSpan(3, nameLength)));

        int headerLengthOffset = 3 + nameLength;
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(headerLengthOffset, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(sentData.AsSpan(headerLengthOffset + 2), headerLength);
        Assert.Equal("high", decoded["priority"]);
        Assert.Equal(message, sentData[(headerLengthOffset + 2 + headerLength)..]);
    }

    /// <summary>
    /// As with the direct-send overload, attaching headers on a connection negotiated below the
    /// header-envelope minimum version throws rather than silently sending or dropping them.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendToGroupAsync_WithHeadersOnOldNegotiatedVersion_ThrowsNotSupportedException()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion(4);
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        var headers = new MessageHeaders([new("priority", "high")]);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => fixture.Client.SendToGroupAsync("team", new byte[] { 1 }, headers));
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
        Assert.Empty(args.Headers);
    }

    /// <summary>
    /// When the receive loop processes a DeliverMessageWithHeaders frame, the MessageReceived event is
    /// raised with the sender id, the decoded headers, and the message data.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_DeliverMessageWithHeaders_RaisesMessageReceivedWithHeaders()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        var message = new byte[] { 1, 2, 3 };
        var headers = new MessageHeaders([new("correlationId", "abc-123")]);
        byte[] frame = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(senderId, headers, message);
        fixture.SetupSuccessfulRegistration(frame);

        MessageReceivedEventArgs? args = null;
        fixture.Client.MessageReceived += (_, e) => args = e;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.NotNull(args);
        Assert.Equal(senderId, args.SenderId);
        Assert.Equal(message, args.Data.ToArray());
        Assert.Equal("abc-123", args.Headers["correlationId"]);
    }

    /// <summary>
    /// When the receive loop processes a DeliverGroupMessageWithHeaders frame, the
    /// GroupMessageReceived event is raised with the sender id, the group name, the decoded headers,
    /// and the message data.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_DeliverGroupMessageWithHeaders_RaisesGroupMessageReceivedWithHeaders()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        var message = new byte[] { 1, 2, 3 };
        var headers = new MessageHeaders([new("priority", "high")]);
        byte[] frame = MeshClientFixture.CreateDeliverGroupMessageWithHeadersPayload(
            senderId, "team", headers, message);
        fixture.SetupSuccessfulRegistration(frame);

        GroupMessageReceivedEventArgs? args = null;
        fixture.Client.GroupMessageReceived += (_, e) => args = e;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.NotNull(args);
        Assert.Equal(senderId, args.SenderId);
        Assert.Equal("team", args.GroupName);
        Assert.Equal(message, args.Data.ToArray());
        Assert.Equal("high", args.Headers["priority"]);
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

    /// <summary>
    /// When the hub refuses a group join, the client stops claiming the membership and tells the
    /// application, so it does not go on believing it is in a group it will receive nothing from.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task GroupJoinRefused_RemovesTheGroupAndRaisesTheEvent()
    {
        var fixture = new MeshClientFixture();
        var inbound = Channel.CreateUnbounded<byte[]?>();
        inbound.Writer.TryWrite(fixture.CreateRegistrationResponse());

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await inbound.Reader.ReadAsync(ct));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Alice");

        var refusedTcs = new TaskCompletionSource<GroupJoinRefusedEventArgs>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Client.GroupJoinRefused += (_, e) => refusedTcs.TrySetResult(e);

        await fixture.Client.JoinGroupAsync("secret");
        Assert.Contains("secret", fixture.Client.JoinedGroups);

        inbound.Writer.TryWrite(MeshClientFixture.CreateGroupJoinRefusal("secret"));

        GroupJoinRefusedEventArgs refused = await refusedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("secret", refused.GroupName);
        Assert.DoesNotContain("secret", fixture.Client.JoinedGroups);
    }

    /// <summary>
    /// A refusal that arrives before JoinGroupAsync has resumed still leaves the client out of the group.
    /// The hub can refuse the instant it reads the join frame, so the client must record the membership
    /// before sending rather than after: recording it afterwards would reinstate a group the refusal had
    /// already removed. The interleaving is pinned by holding the send open until the refusal has been
    /// handled, rather than raced for.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task GroupJoinRefused_ArrivingBeforeTheJoinReturns_LeavesTheClientOutOfTheGroup()
    {
        var fixture = new MeshClientFixture();
        var inbound = Channel.CreateUnbounded<byte[]?>();
        inbound.Writer.TryWrite(fixture.CreateRegistrationResponse());

        var refusalHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns<ReadOnlyMemory<byte>, CancellationToken>(async (data, ct) =>
            {
                if (data.Span[0] != 0x0C) // JoinGroup
                {
                    return;
                }

                inbound.Writer.TryWrite(MeshClientFixture.CreateGroupJoinRefusal("secret"));
                await refusalHandled.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
            });
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await inbound.Reader.ReadAsync(ct));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Alice");
        fixture.Client.GroupJoinRefused += (_, _) => refusalHandled.TrySetResult();

        await fixture.Client.JoinGroupAsync("secret");

        Assert.DoesNotContain("secret", fixture.Client.JoinedGroups);
    }

    /// <summary>
    /// A failed re-join of a group the client is already in does not roll back the record its earlier,
    /// successful join owns. Rolling it back would leave JoinedGroups missing a group the hub still has
    /// the client in — and the reconnector restores from that snapshot, so the group would silently not
    /// be restored after a drop.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task JoinGroupAsync_FailedRejoinOfAJoinedGroup_KeepsTheExistingMembershipRecord()
    {
        var fixture = new MeshClientFixture();
        var inbound = Channel.CreateUnbounded<byte[]?>();
        inbound.Writer.TryWrite(fixture.CreateRegistrationResponse());

        bool failSends = false;

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns<ReadOnlyMemory<byte>, CancellationToken>((_, _) => Volatile.Read(ref failSends)
                ? Task.FromException(new IOException("transport failed"))
                : Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await inbound.Reader.ReadAsync(ct));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Alice");

        await fixture.Client.JoinGroupAsync("team");
        Assert.Contains("team", fixture.Client.JoinedGroups);

        Volatile.Write(ref failSends, true);
        await Assert.ThrowsAsync<IOException>(() => fixture.Client.JoinGroupAsync("team"));

        Assert.Contains("team", fixture.Client.JoinedGroups);
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

    // RequestAsync / ReplyAsync

    /// <summary>
    /// When RequestAsync is called on a client that is not connected to a hub, an
    /// InvalidOperationException is thrown, matching every other send-shaped method.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task RequestAsync_NotConnected_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.RequestAsync(Guid.NewGuid(), new byte[] { 1 }, TimeSpan.FromSeconds(1)));
    }

    /// <summary>
    /// A zero or negative timeout is rejected before anything is sent, rather than resolving
    /// immediately as a timeout would.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task RequestAsync_ZeroTimeout_ThrowsArgumentOutOfRangeException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => fixture.Client.RequestAsync(Guid.NewGuid(), new byte[] { 1 }, TimeSpan.Zero));
    }

    /// <summary>
    /// When a matching reply frame (correlation id echoed back with the reply header set) arrives,
    /// RequestAsync completes with the reply's payload rather than raising it through MessageReceived.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task RequestAsync_ReplyArrives_ReturnsReplyPayloadWithoutRaisingMessageReceived()
    {
        var fixture = new MeshClientFixture();
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        var responderId = Guid.NewGuid();
        byte[] replyBody = [9, 9, 9];

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                byte[] sent = data.ToArray();
                if (sent[0] != 0x11)
                {
                    // Not the request frame (e.g. the registration handshake); nothing to reply to.
                    return;
                }

                int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sent.AsSpan(17, 2));
                MessageHeaders sentHeaders = HeaderEnvelope.Read(sent.AsSpan(19), headerLength);
                string correlationId = sentHeaders[RequestReplyHeaderKeys.CorrelationId];

                var replyHeaders = new MessageHeaders(
                [
                    new(RequestReplyHeaderKeys.CorrelationId, correlationId),
                    new(RequestReplyHeaderKeys.Reply, "1"),
                ]);
                channel.Writer.TryWrite(
                    MeshClientFixture.CreateDeliverMessageWithHeadersPayload(responderId, replyHeaders, replyBody));
            })
            .Returns(Task.CompletedTask);

        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));

        bool messageReceivedRaised = false;
        fixture.Client.MessageReceived += (_, _) => messageReceivedRaised = true;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        ReadOnlyMemory<byte> reply = await fixture.Client.RequestAsync(
            responderId, new byte[] { 1, 2, 3 }, TimeSpan.FromSeconds(1));

        Assert.Equal(replyBody, reply.ToArray());
        Assert.False(messageReceivedRaised);
    }

    /// <summary>
    /// Concurrent requests are tracked independently by their own correlation id: each resolves with
    /// the reply addressed to it, not a reply meant for the other.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task RequestAsync_ConcurrentRequests_AreIndependent()
    {
        var fixture = new MeshClientFixture();
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        var responderId = Guid.NewGuid();

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                byte[] sent = data.ToArray();
                if (sent[0] != 0x11)
                {
                    return;
                }

                int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sent.AsSpan(17, 2));
                MessageHeaders sentHeaders = HeaderEnvelope.Read(sent.AsSpan(19), headerLength);
                string correlationId = sentHeaders[RequestReplyHeaderKeys.CorrelationId];
                byte[] requestBody = sent[(19 + headerLength)..];

                // Echo the request body back behind a marker, so each reply is distinguishable and can
                // be matched back to the request that produced it.
                var replyBody = new byte[9 + requestBody.Length];
                Encoding.UTF8.GetBytes("reply-to:").CopyTo(replyBody, 0);
                requestBody.CopyTo(replyBody, 9);

                var replyHeaders = new MessageHeaders(
                [
                    new(RequestReplyHeaderKeys.CorrelationId, correlationId),
                    new(RequestReplyHeaderKeys.Reply, "1"),
                ]);
                channel.Writer.TryWrite(
                    MeshClientFixture.CreateDeliverMessageWithHeadersPayload(responderId, replyHeaders, replyBody));
            })
            .Returns(Task.CompletedTask);

        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Task<ReadOnlyMemory<byte>> first = fixture.Client.RequestAsync(
            responderId, new byte[] { 1 }, TimeSpan.FromSeconds(1));
        Task<ReadOnlyMemory<byte>> second = fixture.Client.RequestAsync(
            responderId, new byte[] { 2 }, TimeSpan.FromSeconds(1));

        ReadOnlyMemory<byte>[] results = await Task.WhenAll(first, second);

        Assert.Equal(Encoding.UTF8.GetBytes("reply-to:").Append((byte)1), results[0].ToArray());
        Assert.Equal(Encoding.UTF8.GetBytes("reply-to:").Append((byte)2), results[1].ToArray());
    }

    /// <summary>
    /// When no reply arrives within the given timeout, RequestAsync fails with a TimeoutException
    /// rather than hanging indefinitely.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task RequestAsync_NoReplyWithinTimeout_ThrowsTimeoutException()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistration();

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<TimeoutException>(
            () => fixture.Client.RequestAsync(Guid.NewGuid(), new byte[] { 1 }, TimeSpan.FromMilliseconds(50)));
    }

    /// <summary>
    /// A reply that arrives after its request has already timed out is discarded rather than being
    /// misrouted to a later, unrelated request that happens to reuse the same correlation id, and does
    /// not surface through MessageReceived either.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task RequestAsync_LateReplyAfterTimeout_IsDiscardedNotMisrouted()
    {
        var fixture = new MeshClientFixture();
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<TimeoutException>(
            () => fixture.Client.RequestAsync(Guid.NewGuid(), new byte[] { 1 }, TimeSpan.FromMilliseconds(20)));

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        // A late reply for the now-expired correlation id ("1", the first and only request this fresh
        // client has made).
        var lateReplyHeaders = new MessageHeaders(
        [
            new(RequestReplyHeaderKeys.CorrelationId, "1"),
            new(RequestReplyHeaderKeys.Reply, "1"),
        ]);
        channel.Writer.TryWrite(
            MeshClientFixture.CreateDeliverMessageWithHeadersPayload(Guid.NewGuid(), lateReplyHeaders, [9]));

        // A barrier message that follows it: once this is observed, the loop must already have
        // processed (and silently dropped) the late reply immediately before it.
        var barrierHeaders = new MessageHeaders([new("marker", "barrier")]);
        channel.Writer.TryWrite(
            MeshClientFixture.CreateDeliverMessageWithHeadersPayload(Guid.NewGuid(), barrierHeaders, [5]));

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(5, received.Data.Span[0]);
    }

    /// <summary>
    /// A reply frame claiming the correct correlation id but arriving from a client other than the one
    /// the request was addressed to is discarded rather than resolving the request — otherwise any
    /// other client connected to the same hub could forge a reply for a request meant for someone else.
    /// The genuinely addressed responder's reply, arriving afterwards, still completes the request: the
    /// forged reply must not have consumed or stranded the pending slot.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task RequestAsync_ReplyFromWrongSender_IsDiscardedAndGenuineReplyStillCompletes()
    {
        var fixture = new MeshClientFixture();
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        var intendedResponderId = Guid.NewGuid();
        var attackerId = Guid.NewGuid();
        byte[] genuineReplyBody = [7, 7, 7];

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                byte[] sent = data.ToArray();
                if (sent[0] != 0x11)
                {
                    return;
                }

                int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sent.AsSpan(17, 2));
                MessageHeaders sentHeaders = HeaderEnvelope.Read(sent.AsSpan(19), headerLength);
                string correlationId = sentHeaders[RequestReplyHeaderKeys.CorrelationId];

                var replyHeaders = new MessageHeaders(
                [
                    new(RequestReplyHeaderKeys.CorrelationId, correlationId),
                    new(RequestReplyHeaderKeys.Reply, "1"),
                ]);

                // A forged reply from a client that was never the addressed recipient, immediately
                // followed by the genuine responder's own reply — both racing to resolve the same
                // pending request.
                channel.Writer.TryWrite(
                    MeshClientFixture.CreateDeliverMessageWithHeadersPayload(attackerId, replyHeaders, [9, 9, 9]));
                channel.Writer.TryWrite(
                    MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
                        intendedResponderId, replyHeaders, genuineReplyBody));
            })
            .Returns(Task.CompletedTask);

        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        ReadOnlyMemory<byte> reply = await fixture.Client.RequestAsync(
            intendedResponderId, new byte[] { 1 }, TimeSpan.FromSeconds(1));

        Assert.Equal(genuineReplyBody, reply.ToArray());
    }

    /// <summary>
    /// If the connection is torn down before a reply arrives, the pending RequestAsync call is faulted
    /// rather than left hanging forever.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task RequestAsync_ConnectionClosedBeforeReplyArrives_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                if (data.Span[0] == 0x11)
                {
                    // Simulate the connection dropping immediately after the request is sent, before
                    // any reply arrives — the receive loop's next read observes a closed connection.
                    channel.Writer.TryWrite(null);
                }
            })
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.RequestAsync(Guid.NewGuid(), new byte[] { 1 }, TimeSpan.FromSeconds(5)));
    }

    /// <summary>
    /// A message carrying a request correlation id (but not the reply flag) is an incoming request: it
    /// is raised through MessageReceived as normal, with its CorrelationId populated so a handler knows
    /// to answer it via ReplyAsync.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_MessageCarriesRequestCorrelationId_SetsCorrelationIdOnEventArgs()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        var headers = new MessageHeaders([new(RequestReplyHeaderKeys.CorrelationId, "7")]);
        byte[] payload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, headers, new byte[] { 1 });
        fixture.SetupSuccessfulRegistration(payload);

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(7L, received.CorrelationId);
    }

    /// <summary>
    /// Replying to a message that was not a request (no CorrelationId) throws, rather than sending a
    /// reply frame nothing is waiting for.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReplyAsync_MessageWasNotARequest_ThrowsInvalidOperationException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var request = new MessageReceivedEventArgs { SenderId = Guid.NewGuid(), Data = new byte[] { 1 } };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => fixture.Client.ReplyAsync(request, new byte[] { 2 }));
    }

    /// <summary>
    /// A null request argument throws rather than being dereferenced.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReplyAsync_NullRequest_ThrowsArgumentNullException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => fixture.Client.ReplyAsync(null!, new byte[] { 1 }));
    }

    /// <summary>
    /// ReplyAsync addresses the reply back to the request's sender and carries both the correlation id
    /// and the reply flag, so the original requester's RequestAsync call resolves rather than the reply
    /// itself being mistaken for a fresh request.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReplyAsync_ValidRequest_SendsReplyFrameCarryingCorrelationIdAndReplyFlag()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        var requesterId = Guid.NewGuid();
        var request = new MessageReceivedEventArgs
        {
            SenderId = requesterId,
            Data = new byte[] { 1 },
            CorrelationId = 42,
        };

        await fixture.Client.ReplyAsync(request, new byte[] { 9, 9 });

        Assert.NotNull(sentData);
        Assert.Equal(0x11, sentData[0]); // SendMessageWithHeaders
        Assert.Equal(requesterId, new Guid(sentData.AsSpan(1, 16)));

        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(17, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(sentData.AsSpan(19), headerLength);
        Assert.Equal("42", decoded[RequestReplyHeaderKeys.CorrelationId]);
        Assert.Equal("1", decoded[RequestReplyHeaderKeys.Reply]);
        Assert.Equal(new byte[] { 9, 9 }, sentData[(19 + headerLength)..]);
    }

    // SendAsync(DeliveryOptions) / delivery acknowledgements

    /// <summary>
    /// DeliveryOptions.None behaves exactly like the plain SendAsync overload: no header block, no
    /// extra frame, and the call completes as soon as the hub has accepted the send.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_DeliveryOptionsNone_SendsPlainFrame()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        await fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1, 2, 3 }, DeliveryOptions.None);

        Assert.NotNull(sentData);
        Assert.Equal(0x02, sentData[0]); // SendMessage — no header block written.
    }

    /// <summary>
    /// A send requesting acknowledgement carries the ack-request flag and a correlation id in the
    /// header block, so the recipient's client knows to answer it.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_RequireAck_SendsAckRequestHeaders()
    {
        var fixture = new MeshClientFixture();
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                byte[] sent = data.ToArray();
                if (sent[0] == 0x11)
                {
                    sentData = sent;
                }
            })
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Task sendTask = fixture.Client.SendAsync(
            Guid.NewGuid(), new byte[] { 1 }, DeliveryOptions.RequireAck(TimeSpan.FromSeconds(5)));

        // Give the send a moment to reach the transport before inspecting it; the outstanding call is
        // left pending (never acknowledged) — this test only cares about the outgoing frame shape.
        await Task.Delay(50);

        Assert.NotNull(sentData);
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(17, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(sentData.AsSpan(19), headerLength);
        Assert.Equal("1", decoded[DeliveryAcknowledgementHeaderKeys.Request]);
        Assert.True(decoded.ContainsKey(DeliveryAcknowledgementHeaderKeys.CorrelationId));

        _ = sendTask; // Deliberately left pending; the client is disposed with the fixture's scope.
    }

    /// <summary>
    /// When the recipient's client sends back an acknowledgement, the pending SendAsync call completes
    /// successfully rather than waiting out its timeout.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task SendAsync_RequireAck_AcknowledgementArrives_CompletesSuccessfully()
    {
        var fixture = new MeshClientFixture();
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        var recipientId = Guid.NewGuid();

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                byte[] sent = data.ToArray();
                if (sent[0] != 0x11)
                {
                    return;
                }

                int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sent.AsSpan(17, 2));
                MessageHeaders sentHeaders = HeaderEnvelope.Read(sent.AsSpan(19), headerLength);
                string correlationId = sentHeaders[DeliveryAcknowledgementHeaderKeys.CorrelationId];

                var ackHeaders = new MessageHeaders(
                [
                    new(DeliveryAcknowledgementHeaderKeys.CorrelationId, correlationId),
                    new(DeliveryAcknowledgementHeaderKeys.Ack, "1"),
                ]);
                channel.Writer.TryWrite(
                    MeshClientFixture.CreateDeliverMessageWithHeadersPayload(recipientId, ackHeaders, []));
            })
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));

        bool messageReceivedRaised = false;
        fixture.Client.MessageReceived += (_, _) => messageReceivedRaised = true;

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await fixture.Client.SendAsync(
            recipientId, new byte[] { 1 }, DeliveryOptions.RequireAck(TimeSpan.FromSeconds(1)));

        Assert.False(messageReceivedRaised);
    }

    /// <summary>
    /// If no acknowledgement arrives within the timeout, the send fails with a TimeoutException.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task SendAsync_RequireAck_NoAcknowledgementWithinTimeout_ThrowsTimeoutException()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistration();

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<TimeoutException>(
            () => fixture.Client.SendAsync(
                Guid.NewGuid(), new byte[] { 1 }, DeliveryOptions.RequireAck(TimeSpan.FromMilliseconds(50))));
    }

    /// <summary>
    /// An acknowledgement claiming the correct correlation id but arriving from a client other than the
    /// one the message was addressed to is discarded rather than resolving the send — otherwise any
    /// other client connected to the same hub could forge an acknowledgement for a delivery meant for
    /// someone else. The genuinely addressed recipient's acknowledgement, arriving afterwards, still
    /// completes the send.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task SendAsync_RequireAck_AcknowledgementFromWrongSender_IsDiscardedAndGenuineAckStillCompletes()
    {
        var fixture = new MeshClientFixture();
        var channel = Channel.CreateUnbounded<byte[]?>();
        channel.Writer.TryWrite(fixture.CreateRegistrationResponse());

        var intendedRecipientId = Guid.NewGuid();
        var attackerId = Guid.NewGuid();

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                byte[] sent = data.ToArray();
                if (sent[0] != 0x11)
                {
                    return;
                }

                int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sent.AsSpan(17, 2));
                MessageHeaders sentHeaders = HeaderEnvelope.Read(sent.AsSpan(19), headerLength);
                string correlationId = sentHeaders[DeliveryAcknowledgementHeaderKeys.CorrelationId];

                var ackHeaders = new MessageHeaders(
                [
                    new(DeliveryAcknowledgementHeaderKeys.CorrelationId, correlationId),
                    new(DeliveryAcknowledgementHeaderKeys.Ack, "1"),
                ]);

                channel.Writer.TryWrite(
                    MeshClientFixture.CreateDeliverMessageWithHeadersPayload(attackerId, ackHeaders, []));
                channel.Writer.TryWrite(
                    MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
                        intendedRecipientId, ackHeaders, []));
            })
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await fixture.Client.SendAsync(
            intendedRecipientId, new byte[] { 1 }, DeliveryOptions.RequireAck(TimeSpan.FromSeconds(1)));
    }

    /// <summary>
    /// The delivery-acknowledgement header keys are reserved: a caller that happens to set one directly
    /// via the headers overload is rejected loudly rather than the receive loop silently swallowing an
    /// ordinary message that coincidentally looked like acknowledgement plumbing.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_HeadersContainReservedAckKeys_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var headers = new MessageHeaders([new(DeliveryAcknowledgementHeaderKeys.Ack, "1")]);

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, headers));
    }

    /// <summary>
    /// A message that requested an acknowledgement still raises MessageReceived as normal (delivery
    /// acknowledgement is additive, not a replacement for the ordinary receive path), and the client
    /// sends back an acknowledgement frame addressed to the sender carrying the same correlation id.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_MessageRequestsAcknowledgement_RaisesMessageReceivedAndSendsAcknowledgement()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        var requestHeaders = new MessageHeaders(
        [
            new(DeliveryAcknowledgementHeaderKeys.CorrelationId, "99"),
            new(DeliveryAcknowledgementHeaderKeys.Request, "1"),
        ]);
        byte[] payload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, requestHeaders, new byte[] { 1 });
        fixture.SetupSuccessfulRegistration(payload);

        byte[]? ackData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                byte[] sent = data.ToArray();
                if (sent[0] == 0x11)
                {
                    ackData = sent;
                }
            })
            .Returns(Task.CompletedTask);

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(1, received.Data.Span[0]);

        // The acknowledgement send happens after the event dispatch, on the same receive-loop
        // iteration; give it a moment to reach the mocked transport.
        for (int i = 0; i < 20 && ackData is null; i++)
        {
            await Task.Delay(25);
        }

        Assert.NotNull(ackData);
        Assert.Equal(senderId, new Guid(ackData.AsSpan(1, 16)));
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(ackData.AsSpan(17, 2));
        MessageHeaders ackHeaders = HeaderEnvelope.Read(ackData.AsSpan(19), headerLength);
        Assert.Equal("99", ackHeaders[DeliveryAcknowledgementHeaderKeys.CorrelationId]);
        Assert.Equal("1", ackHeaders[DeliveryAcknowledgementHeaderKeys.Ack]);
        Assert.Empty(ackData[(19 + headerLength)..]);
    }

    /// <summary>
    /// The acknowledgement send is fired and forgotten rather than awaited inline: a stalled write back
    /// to the peer that requested the first message's acknowledgement must not head-of-line-block the
    /// receive loop from processing a second, unrelated frame that arrives straight after it.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task ReceiveLoop_SlowAcknowledgementSend_DoesNotBlockSubsequentFrameProcessing()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();

        var ackRequestHeaders = new MessageHeaders(
        [
            new(DeliveryAcknowledgementHeaderKeys.CorrelationId, "1"),
            new(DeliveryAcknowledgementHeaderKeys.Request, "1"),
        ]);
        byte[] ackRequestingPayload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, ackRequestHeaders, new byte[] { 1 });

        var plainHeaders = new MessageHeaders([new("marker", "second")]);
        byte[] plainPayload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, plainHeaders, new byte[] { 2 });

        fixture.SetupSuccessfulRegistration(ackRequestingPayload, plainPayload);

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns<ReadOnlyMemory<byte>, CancellationToken>((data, ct) =>
            {
                byte[] sent = data.ToArray();
                if (sent[0] == 0x11)
                {
                    int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sent.AsSpan(17, 2));
                    MessageHeaders sentHeaders = HeaderEnvelope.Read(sent.AsSpan(19), headerLength);
                    if (sentHeaders.TryGetValue(DeliveryAcknowledgementHeaderKeys.Ack, out string? ack)
                        && ack == "1")
                    {
                        // Simulates a stalled write to the peer — never completes on its own, only
                        // honours cancellation once the connection tears down.
                        return Task.Delay(Timeout.Infinite, ct);
                    }
                }

                return Task.CompletedTask;
            });

        var receivedMessages = new List<byte>();
        var secondReceivedTcs = new TaskCompletionSource();
        fixture.Client.MessageReceived += (_, e) =>
        {
            receivedMessages.Add(e.Data.Span[0]);
            if (e.Data.Span[0] == 2)
            {
                secondReceivedTcs.TrySetResult();
            }
        };

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        // If the stalled acknowledgement send triggered by the first frame blocked the receive loop,
        // this would never complete within the test's timeout.
        await secondReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.Equal(new byte[] { 1, 2 }, receivedMessages);
    }

    /// <summary>
    /// A message that did not request an acknowledgement does not cause one to be sent.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_MessageWithoutAcknowledgementRequest_DoesNotSendAcknowledgement()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        var headers = new MessageHeaders([new("marker", "plain")]);
        byte[] payload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, headers, new byte[] { 1 });
        fixture.SetupSuccessfulRegistration(payload);

        int sendCount = 0;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref sendCount))
            .Returns(Task.CompletedTask);

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Only the registration request itself should have been sent; nothing else in reaction to a
        // plain message.
        Assert.Equal(1, sendCount);
    }

    // SendAsync(DeliveryOptions.AwaitCapacity) / QueueSaturated (#30)

    /// <summary>
    /// DeliveryOptions.AwaitingCapacity() carries the await-capacity header and no acknowledgement
    /// request, distinct from DeliveryOptions.RequireAck.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_AwaitingCapacity_SendsAwaitCapacityHeader()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        await fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, DeliveryOptions.AwaitingCapacity());

        Assert.NotNull(sentData);
        Assert.Equal(0x11, sentData[0]); // SendMessageWithHeaders
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(17, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(sentData.AsSpan(19), headerLength);
        Assert.Equal("1", decoded[BackpressureHeaderKeys.AwaitCapacity]);
        Assert.False(decoded.ContainsKey(DeliveryAcknowledgementHeaderKeys.Request));
    }

    /// <summary>
    /// DeliveryOptions.RequireAck(...).WithAwaitCapacity() carries both the acknowledgement-request
    /// headers and the await-capacity header on the same send.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_RequireAckWithAwaitCapacity_SendsBothHeaders()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        Task sendTask = fixture.Client.SendAsync(
            Guid.NewGuid(),
            new byte[] { 1 },
            DeliveryOptions.RequireAck(TimeSpan.FromSeconds(5)).WithAwaitCapacity());

        await Task.Delay(50);

        Assert.NotNull(sentData);
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(17, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(sentData.AsSpan(19), headerLength);
        Assert.Equal("1", decoded[DeliveryAcknowledgementHeaderKeys.Request]);
        Assert.Equal("1", decoded[BackpressureHeaderKeys.AwaitCapacity]);

        _ = sendTask; // Deliberately left pending; the client is disposed with the fixture's scope.
    }

    /// <summary>
    /// A QueueSaturated control frame from the hub raises SendRejected, naming the recipient whose queue
    /// was full, so an application can observe a drop the hub was configured to report.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task QueueSaturatedFrame_FromHub_RaisesSendRejectedEvent()
    {
        var fixture = new MeshClientFixture();
        var inbound = Channel.CreateUnbounded<byte[]?>();
        inbound.Writer.TryWrite(fixture.CreateRegistrationResponse());

        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        fixture.Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await inbound.Reader.ReadAsync(ct));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Alice");

        var rejectedTcs = new TaskCompletionSource<SendRejectedEventArgs>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Client.SendRejected += (_, e) => rejectedTcs.TrySetResult(e);

        var saturatedRecipientId = Guid.NewGuid();
        var frame = new byte[17];
        frame[0] = 0x15; // QueueSaturated
        saturatedRecipientId.TryWriteBytes(frame.AsSpan(1));
        inbound.Writer.TryWrite(frame);

        SendRejectedEventArgs rejected = await rejectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(saturatedRecipientId, rejected.RecipientId);
    }

    // SendAsync(TimeSpan) / message expiry

    /// <summary>
    /// A zero or negative time-to-live is rejected before anything is sent.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_ZeroTimeToLive_ThrowsArgumentOutOfRangeException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, TimeSpan.Zero));
    }

    /// <summary>
    /// A send with a time-to-live carries the expiry as an absolute Unix-millisecond instant computed
    /// from now, in the header block, using the reserved expiry header key.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_WithTimeToLive_SendsExpiryHeader()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        byte[]? sentData = null;
        fixture.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => sentData = data.ToArray())
            .Returns(Task.CompletedTask);

        DateTimeOffset before = DateTimeOffset.UtcNow;
        await fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, TimeSpan.FromMinutes(5));
        DateTimeOffset after = DateTimeOffset.UtcNow;

        Assert.NotNull(sentData);
        Assert.Equal(0x11, sentData[0]); // SendMessageWithHeaders
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sentData.AsSpan(17, 2));
        MessageHeaders decoded = HeaderEnvelope.Read(sentData.AsSpan(19), headerLength);

        long expiresAt = long.Parse(
            decoded[MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds], CultureInfo.InvariantCulture);
        var expiry = DateTimeOffset.FromUnixTimeMilliseconds(expiresAt);

        // ToUnixTimeMilliseconds truncates rather than rounds, so the encoded value can legitimately
        // land up to a millisecond below "before" even though it was computed after it.
        Assert.InRange(
            expiry,
            before.Add(TimeSpan.FromMinutes(5)).AddMilliseconds(-1),
            after.Add(TimeSpan.FromMinutes(5)));
    }

    /// <summary>
    /// The expiry header key is reserved, mirroring the request/response and acknowledgement keys.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_HeadersContainReservedExpiryKey_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var headers = new MessageHeaders([new(MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds, "1")]);

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, headers));
    }

    /// <summary>
    /// A message whose expiry has already passed by the time it is received is discarded rather than
    /// raised through MessageReceived.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_ExpiredMessage_DoesNotRaiseMessageReceived()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();

        long alreadyExpired = DateTimeOffset.UtcNow.AddMinutes(-1).ToUnixTimeMilliseconds();
        var expiredHeaders = new MessageHeaders(
        [
            new(
                MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds,
                alreadyExpired.ToString(CultureInfo.InvariantCulture)),
        ]);
        byte[] expiredPayload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, expiredHeaders, new byte[] { 1 });

        // A non-expiring message that follows it, used as a barrier proving the loop processed (and
        // silently dropped) the expired frame immediately before it.
        var barrierHeaders = new MessageHeaders([new("marker", "barrier")]);
        byte[] barrierPayload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, barrierHeaders, new byte[] { 2 });

        fixture.SetupSuccessfulRegistration(expiredPayload, barrierPayload);

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(2, received.Data.Span[0]);
    }

    /// <summary>
    /// A message that has not yet expired is delivered exactly as an ordinary message would be.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_NonExpiredMessage_RaisesMessageReceived()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();

        long farInFuture = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds();
        var headers = new MessageHeaders(
        [
            new(
                MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds,
                farInFuture.ToString(CultureInfo.InvariantCulture)),
        ]);
        byte[] payload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, headers, new byte[] { 3 });
        fixture.SetupSuccessfulRegistration(payload);

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(3, received.Data.Span[0]);
    }

    /// <summary>
    /// A message with no expiry header at all behaves exactly as today: it is always delivered.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_MessageWithoutExpiry_RaisesMessageReceived()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        var headers = new MessageHeaders([new("marker", "no-expiry")]);
        byte[] payload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, headers, new byte[] { 4 });
        fixture.SetupSuccessfulRegistration(payload);

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(4, received.Data.Span[0]);
    }

    /// <summary>
    /// A malformed (non-numeric) expiry value is tolerated as "does not expire", the same as an absent
    /// one, rather than being treated as a delivery failure.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_MalformedExpiryValue_RaisesMessageReceived()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        var headers = new MessageHeaders([new(MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds, "not-a-number")]);
        byte[] payload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, headers, new byte[] { 5 });
        fixture.SetupSuccessfulRegistration(payload);

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(5, received.Data.Span[0]);
    }

    /// <summary>
    /// An expiry value that parses as a valid integer but falls outside the range DateTimeOffset can
    /// represent (for example long.MaxValue) is tolerated exactly like a non-numeric one — the receive
    /// loop must not crash over a hostile or malformed expiry header.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_OutOfRangeExpiryValue_RaisesMessageReceived()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();
        var headers = new MessageHeaders(
        [
            new(
                MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds,
                long.MaxValue.ToString(CultureInfo.InvariantCulture)),
        ]);
        byte[] payload = MeshClientFixture.CreateDeliverMessageWithHeadersPayload(
            senderId, headers, new byte[] { 6 });
        fixture.SetupSuccessfulRegistration(payload);

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(6, received.Data.Span[0]);
    }

    /// <summary>
    /// The expiry check applies to group messages too: an already-expired group message is discarded
    /// rather than raised through GroupMessageReceived, mirroring the direct-message case. Proved the
    /// same way — a non-expiring group message queued immediately afterwards is what actually arrives.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_ExpiredGroupMessage_DoesNotRaiseGroupMessageReceived()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();

        long alreadyExpired = DateTimeOffset.UtcNow.AddMinutes(-1).ToUnixTimeMilliseconds();
        var expiredHeaders = new MessageHeaders(
        [
            new(
                MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds,
                alreadyExpired.ToString(CultureInfo.InvariantCulture)),
        ]);
        byte[] expiredPayload = MeshClientFixture.CreateDeliverGroupMessageWithHeadersPayload(
            senderId, "team", expiredHeaders, new byte[] { 1 });

        var barrierHeaders = new MessageHeaders([new("marker", "barrier")]);
        byte[] barrierPayload = MeshClientFixture.CreateDeliverGroupMessageWithHeadersPayload(
            senderId, "team", barrierHeaders, new byte[] { 2 });

        fixture.SetupSuccessfulRegistration(expiredPayload, barrierPayload);

        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        fixture.Client.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(2, received.Data.Span[0]);
    }

    /// <summary>
    /// A group message that has not yet expired is delivered exactly as an ordinary group message would be.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ReceiveLoop_NonExpiredGroupMessage_RaisesGroupMessageReceived()
    {
        var fixture = new MeshClientFixture();
        var senderId = Guid.NewGuid();

        long farInFuture = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds();
        var headers = new MessageHeaders(
        [
            new(
                MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds,
                farInFuture.ToString(CultureInfo.InvariantCulture)),
        ]);
        byte[] payload = MeshClientFixture.CreateDeliverGroupMessageWithHeadersPayload(
            senderId, "team", headers, new byte[] { 3 });
        fixture.SetupSuccessfulRegistration(payload);

        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        fixture.Client.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(3, received.Data.Span[0]);
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
    /// A DeliverMessageWithHeaders frame whose header block is internally malformed — an outer length
    /// prefix the hub only ever forwards unchanged, never validates — is discarded rather than crashing
    /// the receive loop. The message it was queued behind (a plain DeliverMessage) still arrives.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveLoop_DeliverMessageWithHeaders_MalformedHeaderBlock_DiscardsFrameAndSurvives()
    {
        var fixture = new MeshClientFixture();

        // DeliverMessageWithHeaders: [type][senderId(16)][headerBlockLength(2)=1][headerBlock=[5]][body].
        // A declared header-block length of 1 byte whose sole byte claims a 5-byte key is internally
        // malformed: the key runs straight past the end of the block.
        var senderId = Guid.NewGuid();
        var malformedFrame = new byte[1 + 16 + 2 + 1 + 3];
        malformedFrame[0] = 0x12; // DeliverMessageWithHeaders
        senderId.TryWriteBytes(malformedFrame.AsSpan(1, 16));
        BinaryPrimitives.WriteUInt16BigEndian(malformedFrame.AsSpan(17, 2), 1);
        malformedFrame[19] = 5; // claims a 5-byte key within a 1-byte block
        new byte[] { 9, 9, 9 }.CopyTo(malformedFrame, 20);

        byte[] validFrame = fixture.CreateDeliverMessagePayload(Guid.NewGuid(), [1, 2, 3]);
        fixture.SetupSuccessfulRegistration(malformedFrame, validFrame);

        var receivedEvents = new List<MessageReceivedEventArgs>();
        fixture.Client.MessageReceived += (_, e) => receivedEvents.Add(e);

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.Single(receivedEvents);
        Assert.Equal(new byte[] { 1, 2, 3 }, receivedEvents[0].Data.ToArray());
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
    [Fact(Timeout = TestTimeouts.Harness)]
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
    [Fact(Timeout = TestTimeouts.Harness)]
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
    [Fact(Timeout = TestTimeouts.Harness)]
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
    [Fact(Timeout = TestTimeouts.Harness)]
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
    [Fact(Timeout = TestTimeouts.Harness)]
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

        // The teardown clears Name in the same locked block in which it decides whether to raise,
        // so waiting for that proves it reached the decision instead of stalling short of it —
        // without which the assertion below could pass for the wrong reason. Read as a reference,
        // which is atomic, and bounded generously because this only waits on a continuation hop.
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (fixture.Client.Name.Length != 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10));
        }

        Assert.Empty(fixture.Client.Name);

        // Only the few instructions between that lock being released and the event being invoked
        // remain, so a short settle is ample to catch a raise that should not happen.
        await Task.WhenAny(disconnectedRaised.Task, Task.Delay(TimeSpan.FromMilliseconds(250)));

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
        var response = new byte[18];
        response[0] = 0x01; // RegistrationComplete
        id.TryWriteBytes(response.AsSpan(1, 16));
        response[17] = Protocol.MaxSupportedVersion;
        return response;
    }
}
