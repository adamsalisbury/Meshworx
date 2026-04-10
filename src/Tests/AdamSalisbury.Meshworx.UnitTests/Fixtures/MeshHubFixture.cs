using System.Text;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Fixtures;

internal sealed class MeshHubFixture
{
    private readonly Channel<ITransport> _pendingAccepts = Channel.CreateUnbounded<ITransport>();

    public Mock<ITransportListener> Listener { get; } = new();
    public MeshHub Hub { get; }

    public MeshHubFixture()
    {
        var logger = new Mock<ILogger<MeshHub>>();
        Listener.Setup(l => l.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        Listener.Setup(l => l.DisposeAsync()).Returns(ValueTask.CompletedTask);
        Listener.Setup(l => l.AcceptAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await _pendingAccepts.Reader.ReadAsync(ct).ConfigureAwait(false));
        Hub = new MeshHub(logger.Object, Listener.Object);
    }

    public static byte[] CreateRegistrationRequest(string name = "TestClient")
    {
        byte[] nameBytes = Encoding.UTF8.GetBytes(name);
        var payload = new byte[1 + nameBytes.Length];
        payload[0] = 0x04; // RegistrationRequest
        nameBytes.CopyTo(payload, 1);
        return payload;
    }

    public static Mock<ITransport> CreateMockTransport()
    {
        var transport = new Mock<ITransport>();
        transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return transport;
    }

    public void EnqueueClient(ITransport transport)
    {
        _pendingAccepts.Writer.TryWrite(transport);
    }

    public async Task<RegisteredClient> RegisterClientAsync(string name = "TestClient")
    {
        var transport = CreateMockTransport();
        var registrationCompleteTcs = new TaskCompletionSource<byte[]>();
        var disconnectTcs = new TaskCompletionSource<byte[]?>();

        transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateRegistrationRequest(name))
            .Returns(disconnectTcs.Task);

        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => registrationCompleteTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        EnqueueClient(transport.Object);

        byte[] responseData = await registrationCompleteTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        var clientId = new Guid(responseData.AsSpan(1, 16));

        return new RegisteredClient(clientId, transport, disconnectTcs, responseData);
    }
}

internal sealed class RegisteredClient(
    Guid id,
    Mock<ITransport> transport,
    TaskCompletionSource<byte[]?> disconnectTcs,
    byte[] registrationResponse)
{
    public Guid Id { get; } = id;
    public Mock<ITransport> Transport { get; } = transport;
    public TaskCompletionSource<byte[]?> DisconnectTcs { get; } = disconnectTcs;
    public byte[] RegistrationResponse { get; } = registrationResponse;

    public void Disconnect() => DisconnectTcs.TrySetResult(null);
}
