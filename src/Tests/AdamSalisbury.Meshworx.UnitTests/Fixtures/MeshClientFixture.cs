using System.Text;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Fixtures;

internal sealed class MeshClientFixture
{
    public Mock<ITransport> Transport { get; } = new();
    public MeshClient Client { get; }
    public Guid AssignedId { get; } = Guid.NewGuid();

    public MeshClientFixture()
    {
        var logger = new Mock<ILogger<MeshClient>>();
        Client = new MeshClient(logger.Object);
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

        var sequence = Transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateRegistrationResponse());

        foreach (byte[] message in receiveLoopMessages)
        {
            sequence = sequence.ReturnsAsync(message);
        }

        sequence.ReturnsAsync((byte[]?)null);
    }

    public static byte[] CreateLookupFoundResponse(Guid clientId)
    {
        var response = new byte[18];
        response[0] = 0x07; // ClientLookupResponse
        response[1] = 0x01; // found
        clientId.TryWriteBytes(response.AsSpan(2));
        return response;
    }

    public static byte[] CreateLookupNotFoundResponse()
    {
        return [0x07, 0x00]; // ClientLookupResponse, not found
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
