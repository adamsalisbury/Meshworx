using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Unit-level tests for <see cref="PriorityOutboundQueue"/> in isolation, direct against the queue rather
/// than through the hub or a transport — these are the fastest, most deterministic place to pin the two
/// acceptance criteria issue #31 asks for: a high-priority frame overtaking a normal-priority backlog,
/// and low-priority traffic never being starved indefinitely.
/// </summary>
public sealed class PriorityOutboundQueueTests
{
    /// <summary>
    /// A high-priority frame enqueued after a whole backlog of normal-priority frames is still the first
    /// frame <see cref="PriorityOutboundQueue.ReadAllAsync"/> yields, because the high lane is drained
    /// ahead of the normal lane.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReadAllAsync_HighPriorityFrameQueuedAfterNormalBacklog_IsYieldedFirst()
    {
        var queue = new PriorityOutboundQueue(capacity: 100);

        for (int i = 0; i < 10; i++)
        {
            Assert.True(queue.TryEnqueue(MessagePriority.Normal, [(byte)i]));
        }

        Assert.True(queue.TryEnqueue(MessagePriority.High, [0xFF]));

        using var cts = new CancellationTokenSource();
        await using IAsyncEnumerator<byte[]> enumerator = queue.ReadAllAsync(cts.Token).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal([0xFF], enumerator.Current);
    }

    /// <summary>
    /// With no priority ever set — every frame queued at the default <see cref="MessagePriority.Normal"/> —
    /// draining still yields frames in the same strict arrival order the single pre-lane queue gave,
    /// so a caller that never opts into priority sees no change in behaviour.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReadAllAsync_AllNormalPriority_PreservesArrivalOrder()
    {
        var queue = new PriorityOutboundQueue(capacity: 100);

        for (byte i = 0; i < 5; i++)
        {
            Assert.True(queue.TryEnqueue(MessagePriority.Normal, [i]));
        }

        using var cts = new CancellationTokenSource();
        await using IAsyncEnumerator<byte[]> enumerator = queue.ReadAllAsync(cts.Token).GetAsyncEnumerator();

        for (byte i = 0; i < 5; i++)
        {
            Assert.True(await enumerator.MoveNextAsync());
            Assert.Equal([i], enumerator.Current);
        }
    }

    /// <summary>
    /// A sustained flood of high- and normal-priority traffic still lets a low-priority frame make
    /// progress: the anti-starvation policy guarantees the low lane one frame every full cycle, so it is
    /// serviced long before the flood itself is exhausted rather than only once every other lane empties.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReadAllAsync_LowPriorityFrame_IsServicedWellBeforeTheFloodDrains()
    {
        var queue = new PriorityOutboundQueue(capacity: 1000);

        const int FloodSize = 40;
        for (int i = 0; i < FloodSize; i++)
        {
            Assert.True(queue.TryEnqueue(MessagePriority.High, [1]));
        }

        for (int i = 0; i < FloodSize; i++)
        {
            Assert.True(queue.TryEnqueue(MessagePriority.Normal, [2]));
        }

        Assert.True(queue.TryEnqueue(MessagePriority.Low, [3]));

        using var cts = new CancellationTokenSource();
        await using IAsyncEnumerator<byte[]> enumerator = queue.ReadAllAsync(cts.Token).GetAsyncEnumerator();

        bool sawLow = false;

        // Comfortably short of the 2 * FloodSize frames a starved low lane would have to wait out —
        // proves the low frame arrives on its own guaranteed cycle rather than only once the flood empties.
        for (int i = 0; i < FloodSize; i++)
        {
            Assert.True(await enumerator.MoveNextAsync());
            if (enumerator.Current is [3])
            {
                sawLow = true;
                break;
            }
        }

        Assert.True(sawLow, "The low-priority frame was not serviced within one flood-sized window.");
    }

    /// <summary>
    /// Capacity is a single gate shared across all three lanes, not one bound per lane, so replacing the
    /// previous single <see cref="System.Threading.Channels.Channel{T}"/> with priority lanes does not
    /// raise the worst-case memory a saturated client can hold.
    /// </summary>
    [Fact]
    public void TryEnqueue_CapacitySharedAcrossMixedLanes_RefusesOnceExhausted()
    {
        var queue = new PriorityOutboundQueue(capacity: 4);

        Assert.True(queue.TryEnqueue(MessagePriority.High, [1]));
        Assert.True(queue.TryEnqueue(MessagePriority.Normal, [2]));
        Assert.True(queue.TryEnqueue(MessagePriority.Low, [3]));
        Assert.True(queue.TryEnqueue(MessagePriority.Normal, [4]));

        Assert.False(queue.TryEnqueue(MessagePriority.High, [5]));
        Assert.Equal(4, queue.Count);
    }

    /// <summary>
    /// Regression test: once every lane is empty, <see cref="PriorityOutboundQueue.ReadAllAsync"/> must
    /// propagate a cancelled token as <see cref="OperationCanceledException"/> rather than spinning —
    /// each lane's <c>WaitToReadAsync</c> resolves (cancelled) instantly once the token fires, and without
    /// an explicit check the loop re-entered its wait branch continuously without ever awaiting real
    /// asynchronous time, pinning a thread-pool worker instead of returning.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task ReadAllAsync_TokenCancelledWhileIdle_ThrowsRatherThanSpinning()
    {
        var queue = new PriorityOutboundQueue(capacity: 10);
        using var cts = new CancellationTokenSource();

        IAsyncEnumerator<byte[]> enumerator = queue.ReadAllAsync(cts.Token).GetAsyncEnumerator();
        ValueTask<bool> moveNextTask = enumerator.MoveNextAsync();

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await moveNextTask.AsTask().WaitAsync(TimeSpan.FromSeconds(1)));
    }

    /// <summary>
    /// Completing the queue (mirroring a client disconnecting) unblocks a pending
    /// <see cref="PriorityOutboundQueue.TryEnqueueAsync"/> wait rather than leaving it parked for its full
    /// timeout with no capacity that will ever free up again.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task TryEnqueueAsync_QueueCompletedWhileWaiting_ReturnsFalseRatherThanWaitingOutTheTimeout()
    {
        var queue = new PriorityOutboundQueue(capacity: 1);
        Assert.True(queue.TryEnqueue(MessagePriority.Normal, [1]));

        Task<bool> waitTask = queue.TryEnqueueAsync(
            MessagePriority.Normal, [2], TimeSpan.FromMinutes(1), CancellationToken.None);

        queue.Complete();

        bool queued = await waitTask.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.False(queued);
    }
}
