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

    public async Task ConnectAsync(string clientName = "TestClient")
    {
        SetupSuccessfulRegistration();
        await Client.ConnectAsync(Transport.Object, clientName).ConfigureAwait(false);
    }
}
