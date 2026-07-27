using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport.Quic;
using AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Quic;

/// <summary>
/// End-to-end tests that exercise the real wire protocol — registration, direct send, broadcast, and
/// group messaging — over a QUIC connection, using the actual hub, clients, and transports rather than
/// mocks. Skipped as a no-op wherever <see cref="QuicListener.IsSupported"/>/<see cref="QuicConnection.IsSupported"/>
/// is <see langword="false"/>.
/// </summary>
public sealed class QuicMeshIntegrationTests
{
    private static readonly TimeSpan WaitTimeout = TestTimeouts.Wait;

    private static MeshHub CreateHub(QuicTransportListener listener)
    {
        return new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
    }

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    /// <summary>
    /// Three clients register, direct-send, broadcast, and exchange a group message over a hub reached
    /// entirely through QUIC — the interoperability surface is identical to the TCP transport because
    /// nothing about the wire protocol changes.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task EndToEnd_RegisterSendBroadcastAndGroupMessage_OverQuic()
    {
        if (!QuicListener.IsSupported || !QuicConnection.IsSupported)
        {
            return;
        }

        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();

        var listener = new QuicTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = certificate });

        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        Task<QuicTransport> Connect() => QuicTransport.ConnectAsync(
            "127.0.0.1",
            port,
            new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = TestCertificates.PinnedTo(certificate),
            });

        await using var alice = CreateClient();
        await using var bob = CreateClient();
        await using var carol = CreateClient();

        // MeshClient.ConnectAsync sends the registration frame immediately once handed a transport,
        // which is also what makes the QUIC stream visible to the hub's own AcceptAsync in the first
        // place — see QuicTransport.ConnectAsync's remarks for why connecting and registering cannot be
        // separated here the way the loopback tests separate connect from accept for other transports.
        await bob.ConnectAsync(await Connect(), "Bob");
        await alice.ConnectAsync(await Connect(), "Alice");
        await carol.ConnectAsync(await Connect(), "Carol");

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
