using System.Buffers.Binary;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Compression;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Fixtures;

internal sealed class MeshClientFixture
{
    public Mock<ITransport> Transport { get; } = new();

    /// <summary>
    /// Writes further frames into the receive loop after setup, for a test that has to answer something
    /// the client sends rather than only script what it reads.
    /// </summary>
    public ChannelWriter<byte[]?>? Inbound { get; private set; }
    public MeshClient Client { get; }
    public Guid AssignedId { get; } = Guid.NewGuid();

    public MeshClientFixture(
        TimeSpan? idleTimeout = null,
        ICompressionStrategyRegistry? compressionStrategies = null,
        int? maxDecompressedBytes = null,
        TimeProvider? timeProvider = null,
        int? maxReassemblyBytes = null,
        TimeSpan? chunkTransferTimeout = null)
    {
        var logger = new Mock<ILogger<MeshClient>>();
        Client = new MeshClient(
            logger.Object,
            idleTimeout,
            maxReassemblyBytes: maxReassemblyBytes,
            chunkTransferTimeout: chunkTransferTimeout,
            timeProvider: timeProvider,
            compressionStrategies: compressionStrategies,
            maxDecompressedBytes: maxDecompressedBytes);
        Transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
    }

    /// <param name="negotiatedVersion">The protocol version the hub echoes back.</param>
    /// <param name="sessionToken">
    /// The resumption token a hub with session resumption enabled appends. Omit it for the 18-byte form
    /// every hub below protocol version 6 produces.
    /// </param>
    public byte[] CreateRegistrationResponse(
        byte negotiatedVersion = Protocol.MaxSupportedVersion, byte[]? sessionToken = null)
    {
        // RegistrationComplete frame: [type][clientId (16)][negotiated version], plus, from version 6,
        // [tokenLength (2, big-endian)][token].
        var response = new byte[sessionToken is null ? 18 : 20 + sessionToken.Length];
        response[0] = 0x01; // RegistrationComplete
        AssignedId.TryWriteBytes(response.AsSpan(1, 16));
        response[17] = negotiatedVersion;

        if (sessionToken is not null)
        {
            BinaryPrimitives.WriteUInt16BigEndian(response.AsSpan(18, 2), (ushort)sessionToken.Length);
            sessionToken.CopyTo(response, 20);
        }

        return response;
    }

    /// <summary>
    /// Builds a SessionResumed frame: [type][resumed client id (16)][tokenLength (2)][renewed token].
    /// </summary>
    public static byte[] CreateSessionResumedFrame(Guid resumedId, byte[] renewedToken)
    {
        var payload = new byte[1 + 16 + 2 + renewedToken.Length];
        payload[0] = 0x17; // SessionResumed
        resumedId.TryWriteBytes(payload.AsSpan(1, 16));
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(17, 2), (ushort)renewedToken.Length);
        renewedToken.CopyTo(payload, 19);
        return payload;
    }

    /// <summary>
    /// Builds a SessionResumeRefused frame: [type], no payload.
    /// </summary>
    public static byte[] CreateSessionResumeRefusedFrame() => [0x18];

    public byte[] CreateDeliverMessagePayload(Guid senderId, byte[] messageContent)
    {
        var payload = new byte[1 + 16 + messageContent.Length];
        payload[0] = 0x03; // DeliverMessage
        senderId.TryWriteBytes(payload.AsSpan(1));
        messageContent.CopyTo(payload, 17);
        return payload;
    }

    /// <summary>
    /// Builds a DeliverMessageWithHeaders frame:
    /// [type][senderId(16)][headerBlockLength(2)][headerBlock][message].
    /// </summary>
    public static byte[] CreateDeliverMessageWithHeadersPayload(
        Guid senderId, MessageHeaders headers, byte[] messageContent)
    {
        int headerLength = HeaderEnvelope.GetEncodedLength(headers);
        var payload = new byte[1 + 16 + 2 + headerLength + messageContent.Length];
        payload[0] = 0x12; // DeliverMessageWithHeaders
        senderId.TryWriteBytes(payload.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(17, 2), (ushort)headerLength);
        HeaderEnvelope.Write(headers, payload.AsSpan(19, headerLength));
        messageContent.CopyTo(payload, 19 + headerLength);
        return payload;
    }

    /// <summary>
    /// Builds a DeliverGroupMessageWithHeaders frame: [type][senderId(16)][groupNameLength(2)]
    /// [groupName][headerBlockLength(2)][headerBlock][message].
    /// </summary>
    public static byte[] CreateDeliverGroupMessageWithHeadersPayload(
        Guid senderId, string groupName, MessageHeaders headers, byte[] messageContent)
    {
        byte[] nameBytes = System.Text.Encoding.UTF8.GetBytes(groupName);
        int headerLength = HeaderEnvelope.GetEncodedLength(headers);
        int headerLengthOffset = 19 + nameBytes.Length;
        var payload = new byte[headerLengthOffset + 2 + headerLength + messageContent.Length];
        payload[0] = 0x14; // DeliverGroupMessageWithHeaders
        senderId.TryWriteBytes(payload.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(17, 2), (ushort)nameBytes.Length);
        nameBytes.CopyTo(payload, 19);
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(headerLengthOffset, 2), (ushort)headerLength);
        HeaderEnvelope.Write(headers, payload.AsSpan(headerLengthOffset + 2, headerLength));
        messageContent.CopyTo(payload, headerLengthOffset + 2 + headerLength);
        return payload;
    }

    public void SetupSuccessfulRegistration(params byte[][] receiveLoopMessages)
    {
        SetupSuccessfulRegistrationWithNegotiatedVersion(Protocol.MaxSupportedVersion, receiveLoopMessages);
    }

    /// <summary>
    /// As <see cref="SetupSuccessfulRegistration"/>, but lets the test control the protocol version the
    /// hub echoes back, so a negotiated-down version can be modelled.
    /// </summary>
    public void SetupSuccessfulRegistrationWithNegotiatedVersion(
        byte negotiatedVersion, params byte[][] receiveLoopMessages)
    {
        Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Model a live connection: yield the registration response then the scripted frames,
        // and afterwards block on an empty (uncompleted) channel so the receive loop stays
        // alive — exactly like a real transport awaiting more data. The blocking read honours
        // the cancellation token, so DisconnectAsync cancels it cleanly. The channel is left
        // uncompleted deliberately: returning null would now be interpreted as a lost
        // connection and trigger teardown.
        var channel = Channel.CreateUnbounded<byte[]?>();
        Inbound = channel.Writer;
        channel.Writer.TryWrite(CreateRegistrationResponse(negotiatedVersion));

        foreach (byte[] message in receiveLoopMessages)
        {
            channel.Writer.TryWrite(message);
        }

        Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await channel.Reader.ReadAsync(ct).ConfigureAwait(false));
    }

    /// <summary>
    /// Builds a GroupJoinRefused frame: [type][UTF-8 group name].
    /// </summary>
    public static byte[] CreateGroupJoinRefusal(string groupName)
    {
        byte[] nameBytes = System.Text.Encoding.UTF8.GetBytes(groupName);
        var payload = new byte[1 + nameBytes.Length];
        payload[0] = 0x10; // GroupJoinRefused
        nameBytes.CopyTo(payload, 1);
        return payload;
    }

    public static byte[] CreateLookupFoundResponse(Guid clientId, int correlationId = 0)
    {
        var response = new byte[22];
        response[0] = 0x07; // ClientLookupResponse
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(1, 4), correlationId);
        response[5] = 0x01; // found
        clientId.TryWriteBytes(response.AsSpan(6));
        return response;
    }

    public static byte[] CreateLookupNotFoundResponse(int correlationId = 0)
    {
        var response = new byte[6];
        response[0] = 0x07; // ClientLookupResponse
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(1, 4), correlationId);
        response[5] = 0x00; // not found
        return response;
    }

    public void SetupWithLookupResponse(byte[] lookupResponse)
    {
        var lookupTcs = new TaskCompletionSource<byte[]?>();

        // Triggered by the lookup's own opcode rather than by a send count. Connecting is not a fixed
        // number of frames — it sends a compression capability advertisement of its own — and an ordinal
        // here answers whichever send happens to land second, which is a race rather than a choice.
        Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
            {
                if ((MessageType)data.Span[0] == MessageType.ClientLookupRequest)
                {
                    lookupTcs.TrySetResult(lookupResponse);
                }
            })
            .Returns(Task.CompletedTask);

        Transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateRegistrationResponse())
            .Returns(lookupTcs.Task)
            .ReturnsAsync((byte[]?)null);
    }

    public async Task ConnectAsync(string clientName = "TestClient")
    {
        SetupSuccessfulRegistration();
        await Client.ConnectAsync(Transport.Object, clientName).ConfigureAwait(false);
    }
}
