using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport.Unix;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Unix;

/// <summary>
/// End-to-end tests that exercise the real wire protocol — registration, direct send, broadcast, and
/// group messaging — over a Unix domain socket, using the actual hub, clients, and transports rather
/// than mocks.
/// </summary>
public sealed class UnixSocketMeshIntegrationTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private static MeshHub CreateHub(UnixSocketTransportListener listener)
    {
        return new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
    }

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    /// <summary>
    /// Three clients register, direct-send, broadcast, and exchange a group message over a hub reached
    /// entirely through a Unix domain socket — the interoperability surface is identical to the TCP
    /// transport because nothing about the wire protocol changes.
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task EndToEnd_RegisterSendBroadcastAndGroupMessage_OverUnixSocket()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);

        await using var hub = CreateHub(listener);
        await hub.StartAsync();

        await using var alice = CreateClient();
        await using var bob = CreateClient();
        await using var carol = CreateClient();

        await bob.ConnectAsync(await UnixSocketTransport.ConnectAsync(path), "Bob");
        await alice.ConnectAsync(await UnixSocketTransport.ConnectAsync(path), "Alice");
        await carol.ConnectAsync(await UnixSocketTransport.ConnectAsync(path), "Carol");

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
