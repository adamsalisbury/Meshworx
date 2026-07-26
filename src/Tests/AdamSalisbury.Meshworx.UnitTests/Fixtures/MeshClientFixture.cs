using System.Buffers.Binary;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Fixtures;

internal sealed class MeshClientFixture
{
    public Mock<ITransport> Transport { get; } = new();
    public MeshClient Client { get; }
    public Guid AssignedId { get; } = Guid.NewGuid();

    public MeshClientFixture(TimeSpan? idleTimeout = null)
    {
        var logger = new Mock<ILogger<MeshClient>>();
        Client = new MeshClient(logger.Object, idleTimeout);
        Transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
    }

    public byte[] CreateRegistrationResponse(byte negotiatedVersion = Protocol.MaxSupportedVersion)
    {
        // RegistrationComplete frame: [type][clientId (16)][negotiated version].
        var response = new byte[18];
        response[0] = 0x01; // RegistrationComplete
        AssignedId.TryWriteBytes(response.AsSpan(1, 16));
        response[17] = negotiatedVersion;
        return response;
    }

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
        int sendCount = 0;

        Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((_, _) =>
            {
                if (Interlocked.Increment(ref sendCount) == 2)
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
