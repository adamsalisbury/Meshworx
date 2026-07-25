using System.Threading.Channels;
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

    public byte[] CreateRegistrationResponse()
    {
        var response = new byte[17];
        response[0] = 0x01; // RegistrationComplete
        AssignedId.TryWriteBytes(response.AsSpan(1));
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

    public void SetupSuccessfulRegistration(params byte[][] receiveLoopMessages)
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
        channel.Writer.TryWrite(CreateRegistrationResponse());

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
