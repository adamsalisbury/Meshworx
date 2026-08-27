using AdamSalisbury.Meshworx.Transport.InMemory;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// End-to-end tests for the hub administration surface (issue #46): <see cref="IMeshHub.GetClients"/>,
/// <see cref="IMeshHub.GetGroups"/>, <see cref="IMeshHub.GetTopics"/> and
/// <see cref="IMeshHub.DisconnectClient"/>.
/// </summary>
public sealed class HubAdminApiTests
{
    private static readonly TimeSpan WaitTimeout = TestTimeouts.Wait;

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    /// <summary>
    /// A connected client's snapshot reflects its id, name, current group membership and connection
    /// time; an unrelated client with no groups reports an empty group list rather than null or the
    /// first client's groups.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task GetClients_ReflectsRealStateForEveryConnectedClient()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        DateTimeOffset before = DateTimeOffset.UtcNow;

        await using var alice = CreateClient();
        await alice.ConnectAsync(listener.Connect(), "Alice");
        await alice.JoinGroupAsync("team");
        await alice.GetClientIdByNameAsync("Alice"); // barrier: join applied

        await using var bob = CreateClient();
        await bob.ConnectAsync(listener.Connect(), "Bob");
        await bob.GetClientIdByNameAsync("Alice"); // barrier: both connections fully registered

        DateTimeOffset after = DateTimeOffset.UtcNow;

        IReadOnlyList<ConnectedClientInfo> clients = hub.GetClients();

        ConnectedClientInfo aliceInfo = Assert.Single(clients, c => c.Id == alice.Id);
        Assert.Equal("Alice", aliceInfo.Name);
        Assert.Equal(["team"], aliceInfo.Groups);
        Assert.InRange(aliceInfo.ConnectedAt, before, after);

        ConnectedClientInfo bobInfo = Assert.Single(clients, c => c.Id == bob.Id);
        Assert.Equal("Bob", bobInfo.Name);
        Assert.Empty(bobInfo.Groups);

        await hub.StopAsync();
    }

    /// <summary>
    /// A client's queued-but-undelivered frames are reflected in its outbound queue depth, proving the
    /// snapshot reads real, live state rather than a fixed placeholder. Driven via
    /// <see cref="MeshHub.TryQueueRawFrameForTesting(Guid, byte[])"/> with the recipient's send loop
    /// blocked, mirroring the technique <c>HandleClient_ParkedAwaitingCapacity_IsNotEvictedForLookingIdle</c>
    /// in <c>MeshHubTests</c> already uses to drive a connection's outbound queue to a specific depth
    /// deterministically.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task GetClients_QueueDepthReflectsUndeliveredFrames()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var recipient = await fixture.RegisterClientAsync("Recipient");

        var sendCalledTcs = new TaskCompletionSource();
        var blockedSendTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => sendCalledTcs.TrySetResult())
            .Returns(blockedSendTcs.Task);

        // The first frame is dequeued by the send loop immediately — releasing its capacity slot as soon
        // as it is read, per PriorityOutboundQueue's own documented semantics, before the (now blocked)
        // send of it even completes — so it no longer counts as queued once sendCalledTcs resolves. The
        // four enqueued afterwards have nowhere to go while the loop is stuck sending the first, so they
        // are what OutboundQueueDepth reports: frames still waiting their turn, not one already in flight.
        Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        await sendCalledTcs.Task.WaitAsync(WaitTimeout);

        for (int i = 0; i < 4; i++)
        {
            Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        }

        ConnectedClientInfo recipientInfo = Assert.Single(fixture.Hub.GetClients(), c => c.Id == recipient.Id);
        Assert.Equal(4, recipientInfo.OutboundQueueDepth);

        blockedSendTcs.TrySetResult();
        recipient.Disconnect();

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// Only groups with at least one member are reported; a group that empties out (every member leaves)
    /// is no longer present in the snapshot.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task GetGroups_ReflectsMembershipAndOmitsEmptiedGroups()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var alice = CreateClient();
        await alice.ConnectAsync(listener.Connect(), "Alice");
        await using var bob = CreateClient();
        await bob.ConnectAsync(listener.Connect(), "Bob");

        await alice.JoinGroupAsync("team");
        await bob.JoinGroupAsync("team");
        await bob.JoinGroupAsync("solo");
        await bob.GetClientIdByNameAsync("Alice"); // barrier: every join applied

        GroupInfo team = Assert.Single(hub.GetGroups(), g => g.Name == "team");
        Assert.Equal([alice.Id, bob.Id], team.MemberIds.OrderBy(id => id == alice.Id ? 0 : 1));

        GroupInfo solo = Assert.Single(hub.GetGroups(), g => g.Name == "solo");
        Assert.Equal([bob.Id], solo.MemberIds);

        await bob.LeaveGroupAsync("solo");
        await bob.GetClientIdByNameAsync("Alice"); // barrier: leave applied

        Assert.DoesNotContain(hub.GetGroups(), g => g.Name == "solo");
        Assert.Single(hub.GetGroups(), g => g.Name == "team");

        await hub.StopAsync();
    }

    /// <summary>
    /// Every distinct subscription pattern currently held is reported, alongside exactly the clients
    /// subscribed to it — including a wildcard pattern, and excluding one that has been unsubscribed.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task GetTopics_ReflectsSubscriptionsAndOmitsUnsubscribedPatterns()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var alice = CreateClient();
        await alice.ConnectAsync(listener.Connect(), "Alice");
        await using var bob = CreateClient();
        await bob.ConnectAsync(listener.Connect(), "Bob");

        await alice.SubscribeAsync("orders.+.created");
        await bob.SubscribeAsync("orders.+.created");
        await bob.SubscribeAsync("invoices.#");
        await bob.GetClientIdByNameAsync("Alice"); // barrier: every subscribe applied

        TopicSubscriptionInfo shared = Assert.Single(hub.GetTopics(), t => t.Pattern == "orders.+.created");
        Assert.Equal([alice.Id, bob.Id], shared.SubscriberIds.OrderBy(id => id == alice.Id ? 0 : 1));

        TopicSubscriptionInfo wildcard = Assert.Single(hub.GetTopics(), t => t.Pattern == "invoices.#");
        Assert.Equal([bob.Id], wildcard.SubscriberIds);

        await bob.UnsubscribeAsync("invoices.#");
        await bob.GetClientIdByNameAsync("Alice"); // barrier: unsubscribe applied

        Assert.DoesNotContain(hub.GetTopics(), t => t.Pattern == "invoices.#");
        Assert.Single(hub.GetTopics(), t => t.Pattern == "orders.+.created");

        await hub.StopAsync();
    }

    /// <summary>
    /// Disconnecting a connected client closes its connection cleanly, fires
    /// <see cref="IMeshHub.ClientDisconnected"/> carrying the given reason, and removes it from the group
    /// it held membership in — the acceptance criteria the issue names explicitly.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task DisconnectClient_ConnectedClient_DisconnectsFiresEventWithReasonAndClearsGroupMembership()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var target = CreateClient();
        await target.ConnectAsync(listener.Connect(), "Target");
        await target.JoinGroupAsync("team");

        await using var observer = CreateClient();
        await observer.ConnectAsync(listener.Connect(), "Observer");
        await observer.JoinGroupAsync("team");
        await observer.GetClientIdByNameAsync("Target"); // barrier: both joins applied

        // Captured before disconnecting: MeshClient.Id resets once the client notices its own connection
        // has closed, and that can race the hub-side ClientDisconnected event this test also waits on.
        Guid targetId = target.Id;

        var disconnectedTcs = new TaskCompletionSource<ClientConnectionEventArgs>();
        hub.ClientDisconnected += (_, e) =>
        {
            if (e.ClientId == targetId)
            {
                disconnectedTcs.TrySetResult(e);
            }
        };

        bool requested = hub.DisconnectClient(targetId, "misbehaving client");
        Assert.True(requested);

        ClientConnectionEventArgs disconnected = await disconnectedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("Target", disconnected.ClientName);
        Assert.Equal("misbehaving client", disconnected.Reason);

        Assert.False(hub.IsClientRegistered(targetId));
        Assert.DoesNotContain(hub.GetGroups(), g => g.Name == "team" && g.MemberIds.Contains(targetId));

        // The group survives with its remaining member — a kick tears down only the kicked connection.
        GroupInfo team = Assert.Single(hub.GetGroups(), g => g.Name == "team");
        Assert.Equal([observer.Id], team.MemberIds);

        await hub.StopAsync();
    }

    /// <summary>
    /// Disconnecting an id that names no connected client is reported as such rather than throwing or
    /// silently succeeding.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task DisconnectClient_UnknownClientId_ReturnsFalse()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        Assert.False(hub.DisconnectClient(Guid.NewGuid()));

        await hub.StopAsync();
    }

    /// <summary>
    /// An ordinary disconnect, not initiated by <see cref="IMeshHub.DisconnectClient"/>, carries no
    /// reason — the field is specific to an administrative kick, not a general-purpose annotation.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ClientDisconnected_OrdinaryDisconnect_HasNoReason()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        var disconnectedTcs = new TaskCompletionSource<ClientConnectionEventArgs>();
        hub.ClientDisconnected += (_, e) => disconnectedTcs.TrySetResult(e);

        var client = CreateClient();
        await client.ConnectAsync(listener.Connect(), "Client");
        await client.DisposeAsync();

        ClientConnectionEventArgs disconnected = await disconnectedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Null(disconnected.Reason);

        await hub.StopAsync();
    }
}
