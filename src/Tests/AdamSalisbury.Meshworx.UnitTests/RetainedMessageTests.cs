using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport.InMemory;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// End-to-end tests for retained (last-value) group and topic messages (issue #42): a client that joins
/// a group, or subscribes to a topic, after a retained send receives it immediately, exactly as though it
/// had arrived the moment membership or subscription took effect.
/// </summary>
public sealed class RetainedMessageTests
{
    private static readonly TimeSpan WaitTimeout = TestTimeouts.Wait;

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    /// <summary>
    /// A group member sends a retained message before anyone else has joined; a client that joins the
    /// group afterwards receives it immediately, without any further send.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_RetainedGroupMessage_IsReplayedToLateJoiner()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var sender = CreateClient();
        await sender.ConnectAsync(listener.Connect(), "Sender");
        await sender.JoinGroupAsync("team");

        byte[] payload = Encoding.UTF8.GetBytes("last known status");
        await sender.SendToGroupAsync("team", payload, retain: true);

        // Barrier: the hub processes the sender's own frames in order, so a lookup round trip on the
        // same connection proves the retained send has already been applied before the joiner connects.
        await sender.GetClientIdByNameAsync("Sender");

        await using var joiner = CreateClient();
        await joiner.ConnectAsync(listener.Connect(), "Joiner");

        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        joiner.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await joiner.JoinGroupAsync("team");

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(sender.Id, received.SenderId);
        Assert.Equal("team", received.GroupName);
        Assert.Equal(payload, received.Data.ToArray());
        Assert.Equal("1", received.Headers[RetainHeaderKeys.Retain]);

        await hub.StopAsync();
    }

    /// <summary>
    /// A retained group send with an empty body clears the group's retained value: a client joining
    /// afterwards receives nothing replayed.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_EmptyRetainedGroupMessage_ClearsRetainedValue()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var sender = CreateClient();
        await sender.ConnectAsync(listener.Connect(), "Sender");
        await sender.JoinGroupAsync("team");

        await sender.SendToGroupAsync("team", Encoding.UTF8.GetBytes("status"), retain: true);
        await sender.SendToGroupAsync("team", ReadOnlyMemory<byte>.Empty, retain: true);
        await sender.GetClientIdByNameAsync("Sender"); // barrier

        await using var joiner = CreateClient();
        await joiner.ConnectAsync(listener.Connect(), "Joiner");

        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        joiner.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await joiner.JoinGroupAsync("team");

        // Prove the group is still healthy and nothing was replayed: a fresh live send still arrives.
        await sender.SendToGroupAsync("team", Encoding.UTF8.GetBytes("live"), retain: false);
        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("live", Encoding.UTF8.GetString(received.Data.Span));

        await hub.StopAsync();
    }

    /// <summary>
    /// A later retained group send replaces the earlier retained value rather than accumulating: a
    /// joiner sees only the most recent one.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_LaterRetainedGroupMessage_ReplacesEarlierOne()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var sender = CreateClient();
        await sender.ConnectAsync(listener.Connect(), "Sender");
        await sender.JoinGroupAsync("team");

        await sender.SendToGroupAsync("team", Encoding.UTF8.GetBytes("first"), retain: true);
        await sender.SendToGroupAsync("team", Encoding.UTF8.GetBytes("second"), retain: true);
        await sender.GetClientIdByNameAsync("Sender"); // barrier

        await using var joiner = CreateClient();
        await joiner.ConnectAsync(listener.Connect(), "Joiner");

        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        joiner.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await joiner.JoinGroupAsync("team");

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("second", Encoding.UTF8.GetString(received.Data.Span));

        await hub.StopAsync();
    }

    /// <summary>
    /// A publish retained on a topic before anyone has subscribed is replayed to a client that
    /// subscribes to a matching wildcard pattern afterwards.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_RetainedTopicMessage_IsReplayedToLateSubscriberViaWildcard()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var publisher = CreateClient();
        await publisher.ConnectAsync(listener.Connect(), "Publisher");

        byte[] payload = Encoding.UTF8.GetBytes("order 42");
        await publisher.PublishAsync("orders.eu.created", payload, retain: true);
        await publisher.GetClientIdByNameAsync("Publisher"); // barrier

        await using var subscriber = CreateClient();
        await subscriber.ConnectAsync(listener.Connect(), "Subscriber");

        var receivedTcs = new TaskCompletionSource<TopicMessageReceivedEventArgs>();
        subscriber.TopicMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await subscriber.SubscribeAsync("orders.+.created");

        TopicMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(publisher.Id, received.SenderId);
        Assert.Equal("orders.eu.created", received.Topic);
        Assert.Equal(payload, received.Data.ToArray());
        Assert.Equal("1", received.Headers[RetainHeaderKeys.Retain]);

        await hub.StopAsync();
    }

    /// <summary>
    /// Subscribing to a pattern replays every retained topic it matches, not just one — a single
    /// subscription can pick up several independently retained topics at once.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_SubscribePattern_ReplaysEveryMatchingRetainedTopic()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var publisher = CreateClient();
        await publisher.ConnectAsync(listener.Connect(), "Publisher");

        await publisher.PublishAsync("orders.eu.created", Encoding.UTF8.GetBytes("eu"), retain: true);
        await publisher.PublishAsync("orders.us.created", Encoding.UTF8.GetBytes("us"), retain: true);
        await publisher.GetClientIdByNameAsync("Publisher"); // barrier

        await using var subscriber = CreateClient();
        await subscriber.ConnectAsync(listener.Connect(), "Subscriber");

        var received = new List<string>();
        var bothReceivedTcs = new TaskCompletionSource();
        subscriber.TopicMessageReceived += (_, e) =>
        {
            lock (received)
            {
                received.Add(Encoding.UTF8.GetString(e.Data.Span));
                if (received.Count == 2)
                {
                    bothReceivedTcs.TrySetResult();
                }
            }
        };

        await subscriber.SubscribeAsync("orders.#");

        await bothReceivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Contains("eu", received);
        Assert.Contains("us", received);

        await hub.StopAsync();
    }

    /// <summary>
    /// A retained publish with an empty body clears the topic's retained value: a client subscribing
    /// afterwards receives nothing replayed for it.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_EmptyRetainedTopicMessage_ClearsRetainedValue()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var publisher = CreateClient();
        await publisher.ConnectAsync(listener.Connect(), "Publisher");

        await publisher.PublishAsync("orders.eu.created", Encoding.UTF8.GetBytes("eu"), retain: true);
        await publisher.PublishAsync("orders.eu.created", ReadOnlyMemory<byte>.Empty, retain: true);
        await publisher.GetClientIdByNameAsync("Publisher"); // barrier

        await using var subscriber = CreateClient();
        await subscriber.ConnectAsync(listener.Connect(), "Subscriber");

        var receivedTcs = new TaskCompletionSource<TopicMessageReceivedEventArgs>();
        subscriber.TopicMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        await subscriber.SubscribeAsync("orders.eu.created");

        // Prove nothing was replayed and the subscription is still healthy: a fresh live publish arrives.
        await publisher.PublishAsync("orders.eu.created", Encoding.UTF8.GetBytes("live"));
        TopicMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("live", Encoding.UTF8.GetString(received.Data.Span));

        await hub.StopAsync();
    }

    /// <summary>
    /// A retained body over <see cref="Protocol.MaxRetainedMessageBytes"/> is refused rather than stored,
    /// but the live fan-out to already-connected members still happens — only the retention itself is
    /// dropped.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_OversizeRetainedGroupMessage_IsNotStoredButStillFansOutLive()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();

        await using var sender = CreateClient();
        await using var member = CreateClient();

        await sender.ConnectAsync(listener.Connect(), "Sender");
        await member.ConnectAsync(listener.Connect(), "Member");

        await member.JoinGroupAsync("team");
        await member.GetClientIdByNameAsync("Sender"); // barrier
        await sender.JoinGroupAsync("team");

        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        member.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        byte[] oversizePayload = new byte[Protocol.MaxRetainedMessageBytes + 1];
        await sender.SendToGroupAsync("team", oversizePayload, retain: true);

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(oversizePayload.Length, received.Data.Length);

        // Confirm nothing was retained: a late joiner receives no replay, proven by a live send arriving
        // as the very next group message it sees.
        await using var joiner = CreateClient();
        await joiner.ConnectAsync(listener.Connect(), "Joiner");

        var joinerReceivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        joiner.GroupMessageReceived += (_, e) => joinerReceivedTcs.TrySetResult(e);

        await joiner.JoinGroupAsync("team");
        await joiner.GetClientIdByNameAsync("Sender"); // barrier: join applied, no replay arrived

        Assert.False(joinerReceivedTcs.Task.IsCompleted, "An oversize retained message was replayed.");

        await sender.SendToGroupAsync("team", Encoding.UTF8.GetBytes("live"));
        GroupMessageReceivedEventArgs joinerReceived = await joinerReceivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("live", Encoding.UTF8.GetString(joinerReceived.Data.Span));

        await hub.StopAsync();
    }

    /// <summary>
    /// A single subscribe matching several retained topics at once is bounded by the same fan-out
    /// delivery-volume budget an equivalent live publish would be — a wildcard pattern cannot force the
    /// hub into unlimited replay just because it originates from one small inbound frame.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_SubscribeMatchingManyRetainedTopics_IsBoundedByDeliveryVolumeBudget()
    {
        var listener = new InMemoryTransportListener();
        await using var hub = new MeshHub(
            new Mock<ILogger<MeshHub>>().Object,
            listener,
            maxFanOutMessagesPerSecond: 100,
            maxFanOutDeliveriesPerSecond: 3);
        await hub.StartAsync();

        await using var publisher = CreateClient();
        await publisher.ConnectAsync(listener.Connect(), "Publisher");

        // Five retained topics, all matching the wildcard pattern the subscriber below uses. Published
        // before anyone subscribes, so none of this spends the publisher's own delivery-volume budget —
        // that budget is only charged for an actual recipient, and there are none yet.
        for (int i = 0; i < 5; i++)
        {
            await publisher.PublishAsync(
                $"orders.region{i}.created", Encoding.UTF8.GetBytes($"order {i}"), retain: true);
        }

        await publisher.GetClientIdByNameAsync("Publisher"); // barrier: all five retained

        await using var subscriber = CreateClient();
        await subscriber.ConnectAsync(listener.Connect(), "Subscriber");

        var received = new List<string>();
        subscriber.TopicMessageReceived += (_, e) =>
        {
            lock (received)
            {
                received.Add(e.Topic);
            }
        };

        await subscriber.SubscribeAsync("orders.#");
        // Barrier: replay runs synchronously within the hub's own handling of the subscribe frame, on
        // the subscriber's own connection, so a round trip on that same connection guarantees replay —
        // whatever the budget allowed of it — has already happened by the time this returns.
        await subscriber.GetClientIdByNameAsync("Publisher");

        lock (received)
        {
            // A budget of 3 admits exactly three of the five matching retained topics; the rest are
            // dropped rather than built and enqueued regardless.
            Assert.Equal(3, received.Count);
        }

        await hub.StopAsync();
    }
}
