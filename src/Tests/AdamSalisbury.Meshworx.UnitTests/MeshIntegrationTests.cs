using System.Net;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// End-to-end tests that exercise the real wire protocol — framing, registration handshake,
/// name lookup correlation, message routing, and graceful disconnect — over loopback TCP using
/// the actual hub, clients, and transports rather than mocks.
/// </summary>
public sealed class MeshIntegrationTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private static MeshHub CreateHub(TcpTransportListener listener)
    {
        return new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
    }

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    /// <summary>
    /// Two clients connect to a hub over TCP; one looks the other up by name and sends it a message,
    /// which is delivered intact with the correct sender identifier.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_MessageRoutedBetweenTwoClientsOverTcp()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        await using var alice = CreateClient();
        await using var bob = CreateClient();

        await bob.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Bob");
        await alice.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Alice");

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        bob.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        Guid? bobId = await alice.GetClientIdByNameAsync("Bob");
        Assert.Equal(bob.Id, bobId);

        byte[] payload = Encoding.UTF8.GetBytes("hello bob");
        await alice.SendAsync(bobId!.Value, payload);

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(alice.Id, received.SenderId);
        Assert.Equal(payload, received.Data.ToArray());

        await hub.StopAsync();
    }

    /// <summary>
    /// A lookup for a name that is not registered returns null over the real protocol.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_LookupForUnknownName_ReturnsNull()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        await using var client = CreateClient();
        await client.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Solo");

        Guid? result = await client.GetClientIdByNameAsync("Nobody");

        Assert.Null(result);

        await hub.StopAsync();
    }

    /// <summary>
    /// When the hub stops, connected clients receive the disconnect notification and raise
    /// Disconnected with the RemoteDisconnect reason.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_HubStop_RaisesRemoteDisconnectOnClient()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        await using var client = CreateClient();
        await client.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Leaver");

        var reasonTcs = new TaskCompletionSource<DisconnectReason>();
        client.Disconnected += (_, e) => reasonTcs.TrySetResult(e.Reason);

        await hub.StopAsync();

        DisconnectReason reason = await reasonTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(DisconnectReason.RemoteDisconnect, reason);
    }
}
