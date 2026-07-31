using System.Net;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// End-to-end tests for hub-to-hub federation (issue #40), exercised over real loopback TCP for both
/// the client-hub and the hub-to-hub legs.
/// </summary>
public sealed class MeshFederationTests
{
    private static readonly TimeSpan WaitTimeout = TestTimeouts.Wait;

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    /// <summary>
    /// Links two hubs and waits for the link to be visible on both sides — a barrier, since
    /// LinkPeerAsync's own documented return point (handshake plus the first route exchange) says
    /// nothing about when the accepting side has finished the same steps.
    /// </summary>
    private static async Task<(MeshHub HubA, int PortA, MeshHub HubB, int PortB)> CreateLinkedHubPairAsync()
    {
        var listenerA = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        var hubA = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listenerA, allowIncomingPeerLinks: true);
        await hubA.StartAsync();
        int portA = ((IPEndPoint)listenerA.LocalEndPoint!).Port;

        var listenerB = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        var hubB = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listenerB, allowIncomingPeerLinks: true);
        await hubB.StartAsync();
        int portB = ((IPEndPoint)listenerB.LocalEndPoint!).Port;

        await hubA.LinkPeerAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB));

        // LinkPeerAsync returns once hub A's own side is registered; wait for hub B to have accepted and
        // registered its own side of the same link before either test proceeds to rely on it.
        await WaitUntilAsync(() => hubB.LinkedPeerCount == 1);
        await WaitUntilAsync(() => hubA.LinkedPeerCount == 1);

        return (hubA, portA, hubB, portB);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(WaitTimeout);
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(10, cts.Token);
        }
    }

    /// <summary>
    /// A client on hub A can send a direct message to a client on hub B once the two hubs are linked,
    /// resolving the recipient's id via the federated ClientLookupRequest fallback.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task EndToEnd_DirectMessageRoutedAcrossFederatedHubs()
    {
        (MeshHub hubA, int portA, MeshHub hubB, int portB) = await CreateLinkedHubPairAsync();
        await using var disposeA = hubA;
        await using var disposeB = hubB;

        await using var sender = CreateClient();
        await using var recipient = CreateClient();

        await sender.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Sender");
        await recipient.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB), "Recipient");

        // Barrier: hub A's routing table only learns of "Recipient" once hub B's own registration has
        // propagated across the peer link — poll the lookup until it resolves rather than assuming a
        // fixed delay is enough.
        Guid? recipientId = null;
        await WaitUntilAsync(() =>
        {
            recipientId = sender.GetClientIdByNameAsync("Recipient").GetAwaiter().GetResult();
            return recipientId is not null;
        });

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        recipient.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        byte[] payload = Encoding.UTF8.GetBytes("hello across the federation");
        await sender.SendAsync(recipientId!.Value, payload);

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(sender.Id, received.SenderId);
        Assert.Equal(payload, received.Data.ToArray());

        await hubA.StopAsync();
        await hubB.StopAsync();
    }

    /// <summary>
    /// A group send from a member on hub A reaches a member of the same group on hub B.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task EndToEnd_GroupMessageRoutedAcrossFederatedHubs()
    {
        (MeshHub hubA, int portA, MeshHub hubB, int portB) = await CreateLinkedHubPairAsync();
        await using var disposeA = hubA;
        await using var disposeB = hubB;

        await using var sender = CreateClient();
        await using var member = CreateClient();

        await sender.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Sender");
        await member.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB), "Member");

        await sender.JoinGroupAsync("federated-team");
        await member.JoinGroupAsync("federated-team");

        // Barrier: both joins have to be applied on their own hubs before the send below; a round trip
        // on each connection guarantees that.
        await sender.GetClientIdByNameAsync("Sender");
        await member.GetClientIdByNameAsync("Member");

        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        member.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        byte[] payload = Encoding.UTF8.GetBytes("team update, federated");
        await sender.SendToGroupAsync("federated-team", payload);

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(sender.Id, received.SenderId);
        Assert.Equal("federated-team", received.GroupName);
        Assert.Equal(payload, received.Data.ToArray());

        await hubA.StopAsync();
        await hubB.StopAsync();
    }

    /// <summary>
    /// A topic publish from hub A reaches a subscriber on hub B.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task EndToEnd_TopicMessageRoutedAcrossFederatedHubs()
    {
        (MeshHub hubA, int portA, MeshHub hubB, int portB) = await CreateLinkedHubPairAsync();
        await using var disposeA = hubA;
        await using var disposeB = hubB;

        await using var publisher = CreateClient();
        await using var subscriber = CreateClient();

        await publisher.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Publisher");
        await subscriber.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB), "Subscriber");

        await subscriber.SubscribeAsync("orders.#");
        await subscriber.GetClientIdByNameAsync("Subscriber"); // barrier: subscription applied

        var receivedTcs = new TaskCompletionSource<TopicMessageReceivedEventArgs>();
        subscriber.TopicMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        byte[] payload = Encoding.UTF8.GetBytes("order 42, federated");
        await publisher.PublishAsync("orders.eu.created", payload);

        TopicMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(publisher.Id, received.SenderId);
        Assert.Equal("orders.eu.created", received.Topic);
        Assert.Equal(payload, received.Data.ToArray());

        await hubA.StopAsync();
        await hubB.StopAsync();
    }

    /// <summary>
    /// When a peer link is lost, the routes it advertised are withdrawn — a name that used to resolve
    /// across the federation stops resolving, rather than pointing at a peer that is no longer there.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task EndToEnd_PeerLossWithdrawsRoutes()
    {
        (MeshHub hubA, int portA, MeshHub hubB, int portB) = await CreateLinkedHubPairAsync();
        await using var disposeA = hubA;

        await using var recipient = CreateClient();
        await recipient.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB), "Recipient");

        await using var sender = CreateClient();
        await sender.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Sender");

        await WaitUntilAsync(() => hubA.LinkedPeerCount == 1);
        Guid? beforeLoss = null;
        await WaitUntilAsync(() =>
        {
            beforeLoss = sender.GetClientIdByNameAsync("Recipient").GetAwaiter().GetResult();
            return beforeLoss is not null;
        });
        Assert.NotNull(beforeLoss);

        // Peer loss: hub B stops entirely, taking the peer link down from hub A's point of view.
        await hubB.StopAsync();
        await hubB.DisposeAsync();

        await WaitUntilAsync(() => hubA.LinkedPeerCount == 0);

        Guid? afterLoss = await sender.GetClientIdByNameAsync("Recipient");
        Assert.Null(afterLoss);

        await hubA.StopAsync();
    }

    /// <summary>
    /// A local client's name always wins over a same-named client advertised by a peer — the conflict
    /// policy documented for federation. Each hub resolves the name it already had locally, not
    /// whichever hub happened to advertise first.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task EndToEnd_DuplicateNameAcrossFederatedHubs_LocalNameWins()
    {
        (MeshHub hubA, int portA, MeshHub hubB, int portB) = await CreateLinkedHubPairAsync();
        await using var disposeA = hubA;
        await using var disposeB = hubB;

        await using var aliceOnA = CreateClient();
        await aliceOnA.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Alice");

        await using var aliceOnB = CreateClient();
        await aliceOnB.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB), "Alice");

        await using var seekerOnA = CreateClient();
        await seekerOnA.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Seeker");

        // Barrier: give the (refused) cross-hub advertisement time to have been processed either way.
        await seekerOnA.GetClientIdByNameAsync("Seeker");

        Guid? resolved = await seekerOnA.GetClientIdByNameAsync("Alice");

        // Hub A's own local Alice must win — a query on hub A for "Alice" can never resolve to hub B's
        // Alice while hub A has one of its own.
        Assert.Equal(aliceOnA.Id, resolved);

        await hubA.StopAsync();
        await hubB.StopAsync();
    }

    /// <summary>
    /// An incoming connection that sends PeerHello is refused outright when the accepting hub was not
    /// constructed with allowIncomingPeerLinks — no link is established.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task LinkPeerAsync_TargetHubDoesNotAllowIncomingLinks_LinkIsRefused()
    {
        var listenerA = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hubA = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listenerA, allowIncomingPeerLinks: true);
        await hubA.StartAsync();
        int portA = ((IPEndPoint)listenerA.LocalEndPoint!).Port;

        var listenerB = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hubB = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listenerB); // allowIncomingPeerLinks defaults to false
        await hubB.StartAsync();
        int portB = ((IPEndPoint)listenerB.LocalEndPoint!).Port;

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => hubA.LinkPeerAsync(TcpTransport.ConnectAsync("127.0.0.1", portB).GetAwaiter().GetResult()));

        Assert.Equal(0, hubA.LinkedPeerCount);
        Assert.Equal(0, hubB.LinkedPeerCount);

        await hubA.StopAsync();
        await hubB.StopAsync();
    }

    /// <summary>
    /// A configured peerAuthenticator that refuses every peer prevents a link from being established,
    /// even when allowIncomingPeerLinks is set.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task LinkPeerAsync_PeerAuthenticatorRefuses_LinkIsRefused()
    {
        var listenerA = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hubA = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listenerA, allowIncomingPeerLinks: true);
        await hubA.StartAsync();
        int portA = ((IPEndPoint)listenerA.LocalEndPoint!).Port;

        var listenerB = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hubB = new MeshHub(
            new Mock<ILogger<MeshHub>>().Object,
            listenerB,
            allowIncomingPeerLinks: true,
            peerAuthenticator: (_, _) => ValueTask.FromResult(false));
        await hubB.StartAsync();
        int portB = ((IPEndPoint)listenerB.LocalEndPoint!).Port;

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => hubA.LinkPeerAsync(TcpTransport.ConnectAsync("127.0.0.1", portB).GetAwaiter().GetResult()));

        Assert.Equal(0, hubB.LinkedPeerCount);

        await hubA.StopAsync();
        await hubB.StopAsync();
    }
}
