using System.Buffers.Binary;
using System.Diagnostics;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

public sealed class MeshHubMetricsTests
{
    private static readonly TimeSpan WaitTimeout = TestTimeouts.Wait;

    // A short, fixed grace period used only to prove a background effect has *not* happened yet before
    // deliberately triggering the condition that makes it happen — never used as a positive wait for
    // something that must occur, which WaitUntilAsync/WaitTimeout cover instead.
    private static readonly TimeSpan ShortGrace = TimeSpan.FromMilliseconds(200);

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();
        while (!condition())
        {
            if (stopwatch.Elapsed > timeout)
            {
                throw new TimeoutException("The expected condition was not met within the timeout.");
            }

            await Task.Delay(10);
        }
    }

    private static string? TagValue(KeyValuePair<string, object?>[] tags, string key)
    {
        foreach (KeyValuePair<string, object?> tag in tags)
        {
            if (tag.Key == key)
            {
                return tag.Value as string;
            }
        }

        return null;
    }

    // meshworx.hub.clients.connected

    /// <summary>
    /// When a client completes registration, the hub's connected-clients up/down counter is incremented.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ClientsConnectedCounter_ClientRegisters_IsIncremented()
    {
        var fixture = new MeshHubFixture();
        using var capture = new MetricsCapture<int>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.clients.connected");

        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        Assert.Contains(1, capture.Values);

        // A registered client's receive loop awaits a fixed, never-completing Task rather than one linked
        // to the hub's cancellation token — see RegisterClientAsync — so it must be disconnected before
        // StopAsync is called, or the hub's shutdown would wait on that handler task forever.
        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        client.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a registered client disconnects, the hub's connected-clients up/down counter is decremented.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ClientsConnectedCounter_ClientDisconnects_IsDecremented()
    {
        var fixture = new MeshHubFixture();
        using var capture = new MetricsCapture<int>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.clients.connected");

        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        client.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.Contains(-1, capture.Values);

        await fixture.Hub.StopAsync();
    }

    // RouteMessage (direct)

    /// <summary>
    /// When a message is routed directly to a registered recipient, the routed-messages and routed-bytes
    /// counters are both incremented, tagged with direction "direct".
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task RouteMessage_RecipientExists_IncrementsRoutedCountersTaggedDirect()
    {
        var fixture = new MeshHubFixture();
        using var routedCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.messages.routed");
        using var bytesCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.bytes.routed");

        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterClientAsync("Sender");
        var recipient = await fixture.RegisterClientAsync("Recipient");

        var deliveredTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => deliveredTcs.TrySetResult())
            .Returns(Task.CompletedTask);

        byte[] messageContent = [10, 20, 30];
        sender.DisconnectTcs.SetResult(MeshHubFixture.CreateDirectMessage(recipient.Id, messageContent));

        await deliveredTcs.Task.WaitAsync(WaitTimeout);

        Assert.Contains(1L, routedCapture.Values);
        int routedIndex = routedCapture.Values.ToList().IndexOf(1L);
        Assert.Equal("direct", TagValue(routedCapture.Tags[routedIndex], "direction"));

        Assert.Contains((long)messageContent.Length, bytesCapture.Values);
        int bytesIndex = bytesCapture.Values.ToList().IndexOf(messageContent.Length);
        Assert.Equal("direct", TagValue(bytesCapture.Tags[bytesIndex], "direction"));

        recipient.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a message is sent to a Guid that does not match any registered client, the dropped-messages
    /// counter is incremented with reason "unknown-recipient".
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task RouteMessage_RecipientDoesNotExist_IncrementsDroppedCounterTaggedUnknownRecipient()
    {
        var fixture = new MeshHubFixture();
        using var droppedCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.messages.dropped");

        await fixture.Hub.StartAsync();
        var client = await fixture.RegisterClientAsync();

        var disposedTcs = new TaskCompletionSource();
        client.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        client.DisconnectTcs.SetResult(MeshHubFixture.CreateDirectMessage(Guid.NewGuid(), [1, 2, 3]));
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        Assert.Contains(1L, droppedCapture.Values);
        int droppedIndex = droppedCapture.Values.ToList().IndexOf(1L);
        Assert.Equal("unknown-recipient", TagValue(droppedCapture.Tags[droppedIndex], "reason"));

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When a recipient's outbound queue is already full, a further message routed to it increments the
    /// dropped-messages counter with reason "queue-full" instead of the routed counters.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task RouteMessage_RecipientQueueFull_IncrementsDroppedCounterTaggedQueueFull()
    {
        var fixture = new MeshHubFixture();
        using var droppedCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.messages.dropped");

        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var recipient = await fixture.RegisterClientAsync("Recipient");

        // Racing a real flood of messages against the recipient's background send loop to fill its queue
        // is not reliably reproducible — it depends on thread-pool scheduling. Instead, prove the send
        // loop is genuinely blocked (via sendCalledTcs), then fill the now-idle queue directly and
        // deterministically through the internal test hook, and only the message that tips it over
        // capacity goes through the real wire protocol and RouteMessage.
        var sendCalledTcs = new TaskCompletionSource();
        var blockedSendTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => sendCalledTcs.TrySetResult())
            .Returns(blockedSendTcs.Task);

        Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        await sendCalledTcs.Task.WaitAsync(WaitTimeout);

        // The send loop has now dequeued that one frame and is stuck delivering it, so nothing else will
        // read from the queue until blockedSendTcs is released below. Fill it to capacity.
        for (int i = 0; i < MeshHub.OutboundQueueCapacityForTesting; i++)
        {
            Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        }

        Assert.False(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));

        // The message that tips the queue over capacity goes through the real routing path, so this
        // proves RouteMessage's own queue-full branch — not just the queue's own bound — records the drop.
        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(recipient.Id, [1]));

        await WaitUntilAsync(() => droppedCapture.Values.Count > 0, WaitTimeout);

        int droppedIndex = droppedCapture.Values.ToList().IndexOf(1L);
        Assert.Equal("queue-full", TagValue(droppedCapture.Tags[droppedIndex], "reason"));

        // Release the recipient's blocked send first, so its send loop can finish draining rather than
        // hanging when its handler is torn down below.
        blockedSendTcs.TrySetResult();

        // The recipient's receive loop is separately awaiting a fixed, never-completing Task — see
        // RegisterClientAsync — so it must be disconnected explicitly before StopAsync, or the hub's
        // shutdown would wait on that handler task forever.
        var disposedTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        recipient.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        await fixture.Hub.StopAsync();
    }

    // Backpressure signalling (#30)

    /// <summary>
    /// A message dropped because the recipient's outbound queue is full always raises
    /// <see cref="IMeshHub.QueueSaturated"/>, regardless of whether the hub was configured to also notify
    /// the sender over the wire — the acceptance criterion that saturation must be observable, not
    /// swallowed.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task RouteMessage_RecipientQueueFull_RaisesQueueSaturatedEvent()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var recipient = await fixture.RegisterClientAsync("Recipient");

        var saturatedTcs = new TaskCompletionSource<QueueSaturatedEventArgs>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Hub.QueueSaturated += (_, e) => saturatedTcs.TrySetResult(e);

        var sendCalledTcs = new TaskCompletionSource();
        var blockedSendTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => sendCalledTcs.TrySetResult())
            .Returns(blockedSendTcs.Task);

        Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        await sendCalledTcs.Task.WaitAsync(WaitTimeout);

        for (int i = 0; i < MeshHub.OutboundQueueCapacityForTesting; i++)
        {
            Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        }

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(recipient.Id, [1]));

        QueueSaturatedEventArgs saturated = await saturatedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(sender.Id, saturated.SenderId);
        Assert.Equal(recipient.Id, saturated.RecipientId);

        blockedSendTcs.TrySetResult();

        var disposedTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        recipient.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// By default (<c>notifyOnQueueSaturation</c> left at its default of <see langword="false"/>), a
    /// sender whose message was dropped for queue-full receives nothing over the wire beyond what it
    /// would have received before this feature existed — the fire-and-forget default is unchanged.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task RouteMessage_RecipientQueueFull_DefaultDoesNotNotifySenderOverTheWire()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var senderFrames = new FrameRecorder(sender.Transport);
        var recipient = await fixture.RegisterClientAsync("Recipient");

        var sendCalledTcs = new TaskCompletionSource();
        var blockedSendTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => sendCalledTcs.TrySetResult())
            .Returns(blockedSendTcs.Task);

        Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        await sendCalledTcs.Task.WaitAsync(WaitTimeout);

        for (int i = 0; i < MeshHub.OutboundQueueCapacityForTesting; i++)
        {
            Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        }

        using var droppedCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.messages.dropped");

        // A second message sent afterwards, on the same connection, that the hub certainly does route —
        // its arrival at a fresh recipient proves the queue-full path above ran and completed first, so
        // senderFrames has already seen everything the drop could possibly have produced. Waiting on the
        // absence of a frame is never deterministic on its own (see FrameRecorder's own remarks); pairing
        // it with a frame the hub certainly will send afterwards on the same connection is what makes it
        // provable.
        var second = await fixture.RegisterClientAsync("Second");
        var secondFrames = new FrameRecorder(second.Transport);

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(recipient.Id, [1]));
        await WaitUntilAsync(() => droppedCapture.Values.Count > 0, WaitTimeout);

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(second.Id, [9]));
        await secondFrames.WaitForAsync(f => f.Length > 0 && f[0] == 0x03).WaitAsync(WaitTimeout); // DeliverMessage

        int droppedIndex = droppedCapture.Values.ToList().IndexOf(1L);
        Assert.Equal("queue-full", TagValue(droppedCapture.Tags[droppedIndex], "reason"));
        Assert.DoesNotContain(senderFrames.Frames, f => f.Length > 0 && f[0] == 0x15); // QueueSaturated

        blockedSendTcs.TrySetResult();

        var disposedTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        recipient.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);
        second.Disconnect();

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// With <c>notifyOnQueueSaturation</c> enabled, the sender is sent a
    /// <see cref="MessageType.QueueSaturated"/> control frame naming the saturated recipient.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task RouteMessage_RecipientQueueFull_NotifyEnabled_SendsQueueSaturatedFrameToSender()
    {
        var fixture = new MeshHubFixture(notifyOnQueueSaturation: true);
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var recipient = await fixture.RegisterClientAsync("Recipient");
        var senderFrames = new FrameRecorder(sender.Transport);

        var sendCalledTcs = new TaskCompletionSource();
        var blockedSendTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => sendCalledTcs.TrySetResult())
            .Returns(blockedSendTcs.Task);

        Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        await sendCalledTcs.Task.WaitAsync(WaitTimeout);

        for (int i = 0; i < MeshHub.OutboundQueueCapacityForTesting; i++)
        {
            Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        }

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(recipient.Id, [1]));

        byte[] nack = await senderFrames.WaitForAsync(f => f.Length > 0 && f[0] == 0x15) // QueueSaturated
            .WaitAsync(WaitTimeout);

        Assert.Equal(recipient.Id, new Guid(nack.AsSpan(1, 16)));

        blockedSendTcs.TrySetResult();

        var disposedTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        recipient.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A broadcast dropped for a full queue raises the in-process event but never sends the sender a
    /// QueueSaturated frame, even with <c>notifyOnQueueSaturation</c> enabled. The sender never named
    /// that recipient — the hub found it in its own registry — so echoing its id back would let any
    /// client enumerate every connected client's id simply by broadcasting until a queue filled.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task BroadcastMessage_RecipientQueueFull_NotifyEnabled_RaisesEventButSendsNoFrame()
    {
        var fixture = new MeshHubFixture(notifyOnQueueSaturation: true);
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var senderFrames = new FrameRecorder(sender.Transport);
        var recipient = await fixture.RegisterClientAsync("Recipient");

        var saturatedTcs = new TaskCompletionSource<QueueSaturatedEventArgs>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Hub.QueueSaturated += (_, e) => saturatedTcs.TrySetResult(e);

        var sendCalledTcs = new TaskCompletionSource();
        var blockedSendTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => sendCalledTcs.TrySetResult())
            .Returns(blockedSendTcs.Task);

        Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        await sendCalledTcs.Task.WaitAsync(WaitTimeout);

        for (int i = 0; i < MeshHub.OutboundQueueCapacityForTesting; i++)
        {
            Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        }

        var broadcastFrame = new byte[2];
        broadcastFrame[0] = 0x0B; // BroadcastMessage
        broadcastFrame[1] = 1;
        sender.EnqueueMessage(broadcastFrame);

        // The hub-side event still fires, naming the saturated recipient — the operator is trusted with
        // that; only the wire notification is withheld.
        QueueSaturatedEventArgs saturated = await saturatedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(recipient.Id, saturated.RecipientId);

        // A directly addressed send to a third client afterwards, on the same connection, is delivered —
        // proving the broadcast drop above has been fully processed, so anything it was going to send
        // back to the sender has already been recorded.
        var third = await fixture.RegisterClientAsync("Third");
        var thirdFrames = new FrameRecorder(third.Transport);
        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(third.Id, [9]));
        await thirdFrames.WaitForAsync(f => f.Length > 0 && f[0] == 0x03).WaitAsync(WaitTimeout);

        Assert.DoesNotContain(senderFrames.Frames, f => f.Length > 0 && f[0] == 0x15); // QueueSaturated

        blockedSendTcs.TrySetResult();

        var disposedTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        recipient.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);
        third.Disconnect();

        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A direct send that opted into <see cref="DeliveryOptions.AwaitCapacity"/> (carried as the
    /// <c>mesh.await-capacity</c> header) is not dropped when the recipient's queue is momentarily full:
    /// the hub awaits capacity, and once the slow consumer drains a slot the message is delivered —
    /// proving no loss under a slow consumer, the acceptance criterion for the opt-in blocking mode.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task RouteMessageWithHeaders_AwaitCapacityRequested_DeliversOnceSlowConsumerDrainsASlot()
    {
        var fixture = new MeshHubFixture(backpressureAwaitTimeout: TimeSpan.FromSeconds(30));
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var recipient = await fixture.RegisterClientAsync("Recipient", versionMin: 5, versionMax: 5);

        using var droppedCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.messages.dropped");

        // Simulate a slow consumer: the send loop dequeues one frame and gets stuck delivering it, so
        // nothing else drains until releaseTcs is completed below. The same mock also records the
        // eventually delivered frame — set up once, rather than layering a FrameRecorder on top, since a
        // later Moq Setup on the same method replaces an earlier one rather than composing with it.
        var sendCalledTcs = new TaskCompletionSource();
        var releaseTcs = new TaskCompletionSource();
        var deliveredTcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns<ReadOnlyMemory<byte>, CancellationToken>(async (data, _) =>
            {
                byte[] frame = data.ToArray();
                sendCalledTcs.TrySetResult();
                await releaseTcs.Task.ConfigureAwait(false);

                if (frame.Length > 0 && frame[0] == 0x12) // DeliverMessageWithHeaders
                {
                    deliveredTcs.TrySetResult(frame);
                }
            });

        Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        await sendCalledTcs.Task.WaitAsync(WaitTimeout);

        for (int i = 0; i < MeshHub.OutboundQueueCapacityForTesting; i++)
        {
            Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        }

        var headers = new MessageHeaders([new(BackpressureHeaderKeys.AwaitCapacity, "1")]);
        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessageWithHeaders(recipient.Id, headers, [42]));

        // Give the awaited write every opportunity to (wrongly) resolve as a drop before capacity frees.
        await Task.Delay(ShortGrace);
        Assert.Empty(droppedCapture.Values);

        // Free exactly one slot; the awaited write should claim it and the message should be delivered.
        releaseTcs.TrySetResult();

        byte[] delivered = await deliveredTcs.Task.WaitAsync(WaitTimeout);

        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(delivered.AsSpan(17, 2));
        Assert.Equal(new byte[] { 42 }, delivered[(19 + headerLength)..]);
        Assert.Empty(droppedCapture.Values);

        recipient.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// When capacity does not free up before <c>backpressureAwaitTimeout</c> elapses, an await-capacity
    /// send falls back to the ordinary drop-on-full behaviour rather than blocking the sender's receive
    /// loop indefinitely.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task RouteMessageWithHeaders_AwaitCapacityRequested_FallsBackToDropAfterTimeout()
    {
        var fixture = new MeshHubFixture(backpressureAwaitTimeout: ShortGrace);
        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var recipient = await fixture.RegisterClientAsync("Recipient", versionMin: 5, versionMax: 5);

        using var droppedCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.messages.dropped");

        var sendCalledTcs = new TaskCompletionSource();
        var blockedSendTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => sendCalledTcs.TrySetResult())
            .Returns(blockedSendTcs.Task);

        Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        await sendCalledTcs.Task.WaitAsync(WaitTimeout);

        for (int i = 0; i < MeshHub.OutboundQueueCapacityForTesting; i++)
        {
            Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        }

        var headers = new MessageHeaders([new(BackpressureHeaderKeys.AwaitCapacity, "1")]);
        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessageWithHeaders(recipient.Id, headers, [7]));

        await WaitUntilAsync(() => droppedCapture.Values.Count > 0, WaitTimeout);
        int droppedIndex = droppedCapture.Values.ToList().IndexOf(1L);
        Assert.Equal("queue-full", TagValue(droppedCapture.Tags[droppedIndex], "reason"));

        blockedSendTcs.TrySetResult();

        var disposedTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        recipient.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        await fixture.Hub.StopAsync();
    }

    // BroadcastMessage

    /// <summary>
    /// A broadcast to several recipients increments the routed-messages counter once, tagged "broadcast" —
    /// not once per recipient — since it counts the message the hub routed rather than the deliveries it
    /// fanned out to.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task BroadcastMessage_MultipleRecipients_IncrementsRoutedCounterOnceTaggedBroadcast()
    {
        var fixture = new MeshHubFixture();
        using var routedCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.messages.routed");

        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var first = await fixture.RegisterClientAsync("First");
        var second = await fixture.RegisterClientAsync("Second");

        var secondTcs = new TaskCompletionSource();
        second.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => secondTcs.TrySetResult())
            .Returns(Task.CompletedTask);

        byte[] messageContent = [1, 2, 3];
        var broadcastFrame = new byte[1 + messageContent.Length];
        broadcastFrame[0] = 0x0B; // BroadcastMessage
        messageContent.CopyTo(broadcastFrame, 1);
        sender.EnqueueMessage(broadcastFrame);

        await secondTcs.Task.WaitAsync(WaitTimeout);

        var broadcastMeasurements = routedCapture.Tags
            .Select((tags, index) => (Value: routedCapture.Values[index], Tags: tags))
            .Where(m => TagValue(m.Tags, "direction") == "broadcast")
            .ToList();

        Assert.Single(broadcastMeasurements);
        Assert.Equal(1L, broadcastMeasurements[0].Value);

        sender.Disconnect();
        first.Disconnect();
        second.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A broadcast from a hub's only connected client has nobody to deliver to, so it does not increment
    /// the routed-messages counter at all — mirroring SendToGroup, which likewise records nothing when
    /// the sender is the group's only member.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task BroadcastMessage_SenderIsOnlyClient_DoesNotIncrementRoutedCounter()
    {
        var fixture = new MeshHubFixture();
        using var routedCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.messages.routed");

        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var senderFrames = new FrameRecorder(sender.Transport);

        byte[] messageContent = [1, 2, 3];
        var broadcastFrame = new byte[1 + messageContent.Length];
        broadcastFrame[0] = 0x0B; // BroadcastMessage
        messageContent.CopyTo(broadcastFrame, 1);
        sender.EnqueueMessage(broadcastFrame);

        // Barrier: a lookup processed after the broadcast on the same connection proves the broadcast
        // itself has already been handled, since the hub processes one client's frames in order.
        sender.EnqueueMessage(MeshHubFixture.CreateLookupRequest(0, "Sender"));
        await senderFrames.WaitForAsync(f => f[0] == 0x07).WaitAsync(WaitTimeout); // ClientLookupResponse

        Assert.DoesNotContain(routedCapture.Tags, tags => TagValue(tags, "direction") == "broadcast");

        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // SendToGroup

    /// <summary>
    /// Enqueues a lookup on a client's connection and waits for its response. The hub processes one
    /// client's frames in order, so this proves every frame that client enqueued beforehand has been
    /// applied — a barrier that needs no sleeping. Mirrors the identically named helper in
    /// <see cref="MeshHubTests"/>.
    /// </summary>
    private static async Task ApplyPendingFramesAsync(
        MultiMessageRegisteredClient client, FrameRecorder recorder, string lookupName)
    {
        client.EnqueueMessage(MeshHubFixture.CreateLookupRequest(0, lookupName));
        await recorder.WaitForAsync(f => f[0] == 0x07).WaitAsync(WaitTimeout); // ClientLookupResponse
    }

    /// <summary>
    /// A message sent to a group increments the routed-messages and routed-bytes counters, tagged
    /// "group".
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task SendToGroup_MemberSends_IncrementsRoutedCountersTaggedGroup()
    {
        var fixture = new MeshHubFixture();
        using var routedCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.messages.routed");
        using var bytesCapture = new MetricsCapture<long>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.bytes.routed");

        await fixture.Hub.StartAsync();
        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        var member = await fixture.RegisterMultiMessageClientAsync("Member");
        var senderFrames = new FrameRecorder(sender.Transport);
        var memberFrames = new FrameRecorder(member.Transport);

        // Both the sender and the member must join: a group message from a client that is the group's
        // only member has nothing to deliver, and the hub records no routed message for it.
        member.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("room"));
        await ApplyPendingFramesAsync(member, memberFrames, "Sender");
        sender.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("room"));
        await ApplyPendingFramesAsync(sender, senderFrames, "Member");

        byte[] messageContent = [7, 8, 9];
        sender.EnqueueMessage(MeshHubFixture.CreateGroupMessage("room", messageContent));

        await memberFrames.WaitForAsync(f => f[0] == 0x0F).WaitAsync(WaitTimeout); // DeliverGroupMessage

        Assert.Contains(1L, routedCapture.Values);
        int routedIndex = routedCapture.Values.ToList().IndexOf(1L);
        Assert.Equal("group", TagValue(routedCapture.Tags[routedIndex], "direction"));

        Assert.Contains((long)messageContent.Length, bytesCapture.Values);
        int bytesIndex = bytesCapture.Values.ToList().IndexOf(messageContent.Length);
        Assert.Equal("group", TagValue(bytesCapture.Tags[bytesIndex], "direction"));

        member.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    // Observable gauge

    /// <summary>
    /// While a recipient's outbound queue holds undelivered frames, the observable outbound-queue-depth
    /// gauge reports a total no smaller than the number queued.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task OutboundQueueDepth_MessagesQueued_ReportsPositiveAggregateDepth()
    {
        var fixture = new MeshHubFixture();
        using var depthCapture = new MetricsCapture<int>(
            fixture.Hub.GetMeterForTesting(), "meshworx.hub.outbound_queue.depth");

        await fixture.Hub.StartAsync();
        var recipient = await fixture.RegisterClientAsync("Recipient");

        // Racing queued writes against the recipient's background send loop to keep some of them sitting
        // in the queue when observed is not reliably reproducible — it depends on thread-pool scheduling.
        // Instead, prove the send loop is genuinely blocked (via sendCalledTcs) before queuing the frames
        // the gauge is expected to report, through the internal test hook.
        var sendCalledTcs = new TaskCompletionSource();
        var blockedSendTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback(() => sendCalledTcs.TrySetResult())
            .Returns(blockedSendTcs.Task);

        Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        await sendCalledTcs.Task.WaitAsync(WaitTimeout);

        for (int i = 0; i < 10; i++)
        {
            Assert.True(fixture.Hub.TryQueueRawFrameForTesting(recipient.Id, [0xFF]));
        }

        depthCapture.RecordObservableInstruments();
        Assert.Contains(depthCapture.Values, v => v == 10);

        // Release the recipient's blocked send first, so its send loop can finish draining rather than
        // hanging when its handler is torn down below.
        blockedSendTcs.TrySetResult();

        // The recipient's receive loop is separately awaiting a fixed, never-completing Task — see
        // RegisterClientAsync — so it must be disconnected explicitly before StopAsync, or the hub's
        // shutdown would wait on that handler task forever.
        var disposedTcs = new TaskCompletionSource();
        recipient.Transport.Setup(t => t.DisposeAsync())
            .Callback(() => disposedTcs.TrySetResult())
            .Returns(ValueTask.CompletedTask);

        recipient.Disconnect();
        await disposedTcs.Task.WaitAsync(WaitTimeout);

        await fixture.Hub.StopAsync();
    }
}
