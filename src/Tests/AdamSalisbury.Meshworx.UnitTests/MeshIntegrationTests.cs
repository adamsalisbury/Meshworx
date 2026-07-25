using System.Buffers.Binary;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport.InMemory;
using AdamSalisbury.Meshworx.Transport.Tcp;
using AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;
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
    /// A broadcast from one client is delivered to every other connected client over the real protocol.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_BroadcastReachesAllOtherClients()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        await using var sender = CreateClient();
        await using var first = CreateClient();
        await using var second = CreateClient();

        await sender.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Sender");
        await first.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "First");
        await second.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Second");

        var firstTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        first.MessageReceived += (_, e) => firstTcs.TrySetResult(e);
        var secondTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        second.MessageReceived += (_, e) => secondTcs.TrySetResult(e);

        byte[] payload = Encoding.UTF8.GetBytes("everyone");
        await sender.BroadcastAsync(payload);

        MessageReceivedEventArgs firstReceived = await firstTcs.Task.WaitAsync(WaitTimeout);
        MessageReceivedEventArgs secondReceived = await secondTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal(sender.Id, firstReceived.SenderId);
        Assert.Equal(payload, firstReceived.Data.ToArray());
        Assert.Equal(sender.Id, secondReceived.SenderId);
        Assert.Equal(payload, secondReceived.Data.ToArray());

        await hub.StopAsync();
    }

    /// <summary>
    /// A message sent to a group is delivered to its members over the real protocol.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_GroupMessageReachesGroupMembers()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        await using var sender = CreateClient();
        await using var member = CreateClient();

        await sender.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Sender");
        await member.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Member");

        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        member.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await member.JoinGroupAsync("team");
        // A lookup round-trip on the member's connection is a barrier: because the hub processes a
        // client's frames in order, the join is guaranteed applied by the time this returns.
        await member.GetClientIdByNameAsync("Sender");

        byte[] payload = Encoding.UTF8.GetBytes("team update");
        await sender.SendToGroupAsync("team", payload);

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(sender.Id, received.SenderId);
        Assert.Equal("team", received.GroupName);
        Assert.Equal(payload, received.Data.ToArray());

        await hub.StopAsync();
    }

    /// <summary>
    /// After a client leaves a group, group messages are no longer delivered to it over the real protocol.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_LeftGroupNoLongerReceivesGroupMessages()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        await using var sender = CreateClient();
        await using var member = CreateClient();

        await sender.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Sender");
        await member.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Member");

        var directTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        member.MessageReceived += (_, e) => directTcs.TrySetResult(e);
        var groupTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        member.GroupMessageReceived += (_, e) => groupTcs.TrySetResult(e);

        await member.JoinGroupAsync("team");
        await member.LeaveGroupAsync("team");
        await member.GetClientIdByNameAsync("Sender"); // barrier: join and leave both applied

        // Send to the now-empty group, then a direct message. The hub processes the sender's frames
        // in order, so any (incorrect) group delivery would be enqueued before the direct message.
        // Receiving the direct message therefore proves the group message was not delivered.
        await sender.SendToGroupAsync("team", Encoding.UTF8.GetBytes("group"));
        await sender.SendAsync(member.Id, Encoding.UTF8.GetBytes("direct"));

        MessageReceivedEventArgs received = await directTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("direct", Encoding.UTF8.GetString(received.Data.Span));
        Assert.False(groupTcs.Task.IsCompleted, "Group message was delivered to a client that left the group.");

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
    /// With heartbeats enabled, a responsive client (one that replies to pings) stays connected across
    /// several heartbeat intervals, exercised end-to-end over the in-memory transport.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_ResponsiveClient_SurvivesHeartbeats()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(
            new Mock<ILogger<MeshHub>>().Object,
            listener,
            heartbeatInterval: TimeSpan.FromMilliseconds(50),
            maxMissedHeartbeats: 3);
        await hub.StartAsync();

        await using var client = CreateClient();
        await client.ConnectAsync(listener.Connect(), "Responsive");

        // Wait well past the eviction window (3 silent intervals ≈ 150 ms). A real client replies to
        // each ping with a pong, so it must remain connected and registered.
        await Task.Delay(TimeSpan.FromMilliseconds(400));

        Assert.True(client.IsConnected);
        Assert.True(hub.IsClientRegistered(client.Id));

        await hub.StopAsync();
    }

    /// <summary>
    /// With heartbeats enabled, a client that registers but never replies to pings is evicted by the
    /// hub, exercised end-to-end over the in-memory transport with a raw, unresponsive endpoint.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_SilentClient_IsEvictedByHeartbeat()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(
            new Mock<ILogger<MeshHub>>().Object,
            listener,
            heartbeatInterval: TimeSpan.FromMilliseconds(50),
            maxMissedHeartbeats: 2);
        await hub.StartAsync();

        // A raw endpoint that completes registration by hand, then goes silent (never replies to pings).
        // Frame: [type][version][name length (2, big-endian)][name][credential].
        var raw = listener.Connect();
        byte[] nameBytes = Encoding.UTF8.GetBytes("Silent");
        var registration = new byte[4 + nameBytes.Length];
        registration[0] = 0x04; // RegistrationRequest
        registration[1] = 0x03; // protocol version
        BinaryPrimitives.WriteUInt16BigEndian(registration.AsSpan(2, 2), (ushort)nameBytes.Length);
        nameBytes.CopyTo(registration, 4);
        await raw.SendAsync(registration);

        byte[]? response = await raw.ReceiveAsync().WaitAsync(WaitTimeout);
        Assert.NotNull(response);
        Assert.Equal(0x01, response[0]); // RegistrationComplete
        var clientId = new Guid(response.AsSpan(1, 16));
        Assert.True(hub.IsClientRegistered(clientId));

        // The endpoint never sends another frame, so the hub evicts it after the missed intervals.
        while (hub.IsClientRegistered(clientId))
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10));
        }

        Assert.False(hub.IsClientRegistered(clientId));

        await raw.DisposeAsync();
        await hub.StopAsync();
    }

    /// <summary>
    /// The full hub and client stack works over the in-memory transport, confirming the transport
    /// abstraction is pluggable: a message is routed between two clients without any sockets.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task EndToEnd_MessageRoutedOverInMemoryTransport()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var alice = CreateClient();
        await using var bob = CreateClient();

        await bob.ConnectAsync(listener.Connect(), "Bob");
        await alice.ConnectAsync(listener.Connect(), "Alice");

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        bob.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        Guid? bobId = await alice.GetClientIdByNameAsync("Bob");
        Assert.Equal(bob.Id, bobId);

        byte[] payload = Encoding.UTF8.GetBytes("over memory");
        await alice.SendAsync(bobId!.Value, payload);

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(alice.Id, received.SenderId);
        Assert.Equal(payload, received.Data.ToArray());

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

    /// <summary>
    /// The whole stack — registration with a credential, name lookup, and routed delivery — runs over a
    /// mutually authenticated TLS connection, with both ends confirming the byte stream is encrypted.
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task EndToEnd_MessageRoutedBetweenTwoClientsOverMutualTls()
    {
        using X509Certificate2 hubCertificate = TestCertificates.CreateSelfSigned("localhost");
        using X509Certificate2 clientCertificate = TestCertificates.CreateSelfSigned("mesh-client");

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions
            {
                ServerCertificate = hubCertificate,
                ClientCertificateRequired = true,
                RemoteCertificateValidationCallback = TestCertificates.PinnedTo(clientCertificate),
            });

        await using var hub = CreateHub(listener);
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        var clientTlsOptions = new SslClientAuthenticationOptions
        {
            ClientCertificates = [clientCertificate],
            RemoteCertificateValidationCallback = TestCertificates.PinnedTo(hubCertificate),
        };

        await using var alice = CreateClient();
        await using var bob = CreateClient();

        TcpTransport bobTransport = await TcpTransport.ConnectAsync("localhost", port, clientTlsOptions);
        TcpTransport aliceTransport = await TcpTransport.ConnectAsync("localhost", port, clientTlsOptions);

        Assert.True(bobTransport.IsEncrypted);
        Assert.True(aliceTransport.IsEncrypted);

        await bob.ConnectAsync(bobTransport, "Bob");
        await alice.ConnectAsync(aliceTransport, "Alice");

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        bob.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        Guid? bobId = await alice.GetClientIdByNameAsync("Bob");
        Assert.Equal(bob.Id, bobId);

        byte[] payload = Encoding.UTF8.GetBytes("hello over tls");
        await alice.SendAsync(bobId!.Value, payload);

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(alice.Id, received.SenderId);
        Assert.Equal(payload, received.Data.ToArray());
    }
}
