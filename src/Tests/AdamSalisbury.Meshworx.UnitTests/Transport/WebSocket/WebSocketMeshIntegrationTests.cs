using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport.WebSocket;
using AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.WebSocket;

/// <summary>
/// End-to-end tests that exercise the real wire protocol — registration, direct send, broadcast, and
/// group messaging — over a WebSocket connection secured with TLS (<c>wss://</c>), using the actual
/// hub, clients, and transports rather than mocks.
/// </summary>
public sealed class WebSocketMeshIntegrationTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private static MeshHub CreateHub(WebSocketTransportListener listener)
    {
        return new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
    }

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    /// <summary>
    /// Three clients register, direct-send, broadcast, and exchange a group message over a hub reached
    /// entirely through <c>wss://</c> — the interoperability surface is identical to the TCP transport
    /// because nothing about the wire protocol changes.
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task EndToEnd_RegisterSendBroadcastAndGroupMessage_OverSecureWebSocket()
    {
        using X509Certificate2 hubCertificate = TestCertificates.CreateSelfSigned("localhost");

        var listener = new WebSocketTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            tlsOptions: new SslServerAuthenticationOptions { ServerCertificate = hubCertificate });

        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        Task<WebSocketTransport> Connect() => WebSocketTransport.ConnectAsync(
            new Uri($"wss://localhost:{port}/"),
            options => options.RemoteCertificateValidationCallback = TestCertificates.PinnedTo(hubCertificate));

        await using var alice = CreateClient();
        await using var bob = CreateClient();
        await using var carol = CreateClient();

        WebSocketTransport bobTransport = await Connect();
        WebSocketTransport aliceTransport = await Connect();
        WebSocketTransport carolTransport = await Connect();

        Assert.True(bobTransport.IsEncrypted);
        Assert.True(aliceTransport.IsEncrypted);
        Assert.True(carolTransport.IsEncrypted);

        await bob.ConnectAsync(bobTransport, "Bob");
        await alice.ConnectAsync(aliceTransport, "Alice");
        await carol.ConnectAsync(carolTransport, "Carol");

        // Registration and lookup.
        Guid? bobId = await alice.GetClientIdByNameAsync("Bob");
        Assert.Equal(bob.Id, bobId);

        // Direct send.
        var directTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        bob.MessageReceived += (_, e) => directTcs.TrySetResult(e);

        byte[] directPayload = Encoding.UTF8.GetBytes("hello bob");
        await alice.SendAsync(bobId!.Value, directPayload);

        MessageReceivedEventArgs directReceived = await directTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(alice.Id, directReceived.SenderId);
        Assert.Equal(directPayload, directReceived.Data.ToArray());

        // Broadcast.
        var bobBroadcastTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        bob.MessageReceived += (_, e) => bobBroadcastTcs.TrySetResult(e);
        var carolBroadcastTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        carol.MessageReceived += (_, e) => carolBroadcastTcs.TrySetResult(e);

        byte[] broadcastPayload = Encoding.UTF8.GetBytes("everyone");
        await alice.BroadcastAsync(broadcastPayload);

        MessageReceivedEventArgs bobBroadcastReceived = await bobBroadcastTcs.Task.WaitAsync(WaitTimeout);
        MessageReceivedEventArgs carolBroadcastReceived = await carolBroadcastTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(broadcastPayload, bobBroadcastReceived.Data.ToArray());
        Assert.Equal(broadcastPayload, carolBroadcastReceived.Data.ToArray());

        // Group.
        var groupTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        carol.GroupMessageReceived += (_, e) => groupTcs.TrySetResult(e);

        await carol.JoinGroupAsync("team");
        await carol.GetClientIdByNameAsync("Alice"); // barrier: the join is applied
        await alice.JoinGroupAsync("team");

        byte[] groupPayload = Encoding.UTF8.GetBytes("team update");
        await alice.SendToGroupAsync("team", groupPayload);

        GroupMessageReceivedEventArgs groupReceived = await groupTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(alice.Id, groupReceived.SenderId);
        Assert.Equal("team", groupReceived.GroupName);
        Assert.Equal(groupPayload, groupReceived.Data.ToArray());

        await hub.StopAsync();
    }
}
