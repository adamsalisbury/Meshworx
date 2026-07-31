using System.Net;
using System.Text;
using AdamSalisbury.Meshworx.Backplane;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// End-to-end tests for the scale-out backplane (issue #41): two independent <see cref="MeshHub"/>
/// instances sharing one <see cref="InMemoryHubBackplane"/>, exercised over real loopback TCP for the
/// client-hub leg, exactly as the acceptance criteria describe ("can use an in-memory fake backplane").
/// </summary>
public sealed class MeshBackplaneTests
{
    private static readonly TimeSpan WaitTimeout = TestTimeouts.Wait;

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    private static async Task<(MeshHub HubA, int PortA, MeshHub HubB, int PortB)> CreateSharedBackplaneHubPairAsync()
    {
        var backplane = new InMemoryHubBackplane();

        var listenerA = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        var hubA = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listenerA, backplane: backplane);
        await hubA.StartAsync();
        int portA = ((IPEndPoint)listenerA.LocalEndPoint!).Port;

        var listenerB = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        var hubB = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listenerB, backplane: backplane);
        await hubB.StartAsync();
        int portB = ((IPEndPoint)listenerB.LocalEndPoint!).Port;

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
    /// A client on hub A can send a direct message to a client on hub B, two independent instances that
    /// share nothing but an in-memory backplane — the recipient's id resolves via the backplane's shared
    /// directory, not a live connection between the two hubs.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task EndToEnd_DirectMessageRoutedAcrossBackplane()
    {
        (MeshHub hubA, int portA, MeshHub hubB, int portB) = await CreateSharedBackplaneHubPairAsync();
        await using var disposeA = hubA;
        await using var disposeB = hubB;

        await using var sender = CreateClient();
        await using var recipient = CreateClient();

        await sender.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Sender");
        await recipient.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB), "Recipient");

        Guid? recipientId = null;
        await WaitUntilAsync(() =>
        {
            recipientId = sender.GetClientIdByNameAsync("Recipient").GetAwaiter().GetResult();
            return recipientId is not null;
        });

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        recipient.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        byte[] payload = Encoding.UTF8.GetBytes("hello across the backplane");
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
    public async Task EndToEnd_GroupMessageRoutedAcrossBackplane()
    {
        (MeshHub hubA, int portA, MeshHub hubB, int portB) = await CreateSharedBackplaneHubPairAsync();
        await using var disposeA = hubA;
        await using var disposeB = hubB;

        await using var sender = CreateClient();
        await using var member = CreateClient();

        await sender.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Sender");
        await member.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB), "Member");

        await sender.JoinGroupAsync("scaled-team");
        await member.JoinGroupAsync("scaled-team");
        await sender.GetClientIdByNameAsync("Sender");
        await member.GetClientIdByNameAsync("Member");

        var receivedTcs = new TaskCompletionSource<GroupMessageReceivedEventArgs>();
        member.GroupMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        byte[] payload = Encoding.UTF8.GetBytes("team update, scaled out");
        await sender.SendToGroupAsync("scaled-team", payload);

        GroupMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(sender.Id, received.SenderId);
        Assert.Equal("scaled-team", received.GroupName);
        Assert.Equal(payload, received.Data.ToArray());

        await hubA.StopAsync();
        await hubB.StopAsync();
    }

    /// <summary>
    /// A topic publish from hub A reaches a subscriber on hub B.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task EndToEnd_TopicMessageRoutedAcrossBackplane()
    {
        (MeshHub hubA, int portA, MeshHub hubB, int portB) = await CreateSharedBackplaneHubPairAsync();
        await using var disposeA = hubA;
        await using var disposeB = hubB;

        await using var publisher = CreateClient();
        await using var subscriber = CreateClient();

        await publisher.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Publisher");
        await subscriber.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB), "Subscriber");

        await subscriber.SubscribeAsync("orders.#");
        await subscriber.GetClientIdByNameAsync("Subscriber");

        var receivedTcs = new TaskCompletionSource<TopicMessageReceivedEventArgs>();
        subscriber.TopicMessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        byte[] payload = Encoding.UTF8.GetBytes("order 42, scaled out");
        await publisher.PublishAsync("orders.eu.created", payload);

        TopicMessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(publisher.Id, received.SenderId);
        Assert.Equal("orders.eu.created", received.Topic);
        Assert.Equal(payload, received.Data.ToArray());

        await hubA.StopAsync();
        await hubB.StopAsync();
    }

    /// <summary>
    /// The shared directory stays consistent as a client disconnects: a name that resolved across
    /// instances before stops resolving afterwards.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task EndToEnd_DirectoryRemovesEntryOnDisconnect()
    {
        (MeshHub hubA, int portA, MeshHub hubB, int portB) = await CreateSharedBackplaneHubPairAsync();
        await using var disposeA = hubA;
        await using var disposeB = hubB;

        await using var seeker = CreateClient();
        await seeker.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portA), "Seeker");

        var recipient = CreateClient();
        await recipient.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", portB), "Recipient");

        Guid? beforeDisconnect = null;
        await WaitUntilAsync(() =>
        {
            beforeDisconnect = seeker.GetClientIdByNameAsync("Recipient").GetAwaiter().GetResult();
            return beforeDisconnect is not null;
        });
        Assert.NotNull(beforeDisconnect);

        await recipient.DisconnectAsync();

        Guid? afterDisconnect = null;
        await WaitUntilAsync(() =>
        {
            afterDisconnect = seeker.GetClientIdByNameAsync("Recipient").GetAwaiter().GetResult();
            return afterDisconnect is null;
        });
        Assert.Null(afterDisconnect);

        await hubA.StopAsync();
        await hubB.StopAsync();
    }

    /// <summary>
    /// A hub with no backplane configured routes exactly as it always has — the single-instance path is
    /// unaffected by the feature existing.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task EndToEnd_HubWithoutBackplane_RoutesLocallyAsBefore()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await using var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener); // no backplane
        await hub.StartAsync();
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        await using var alice = CreateClient();
        await using var bob = CreateClient();
        await bob.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Bob");
        await alice.ConnectAsync(await TcpTransport.ConnectAsync("127.0.0.1", port), "Alice");

        var receivedTcs = new TaskCompletionSource<MessageReceivedEventArgs>();
        bob.MessageReceived += (_, e) => receivedTcs.TrySetResult(e);

        Guid? bobId = await alice.GetClientIdByNameAsync("Bob");
        byte[] payload = Encoding.UTF8.GetBytes("still works");
        await alice.SendAsync(bobId!.Value, payload);

        MessageReceivedEventArgs received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(payload, received.Data.ToArray());

        await hub.StopAsync();
    }
}
