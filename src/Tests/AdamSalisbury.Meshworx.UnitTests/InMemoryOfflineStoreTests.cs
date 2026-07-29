namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Unit-level tests for <see cref="InMemoryOfflineStore"/> in isolation, direct against the store rather
/// than through the hub — the fastest, most deterministic place to pin the bounds issue #28 asks for, and
/// the only place the retention window can be exercised without waiting real time for it (a test supplies
/// the message's <see cref="OfflineMessage.QueuedAt"/> itself).
/// </summary>
public sealed class InMemoryOfflineStoreTests
{
    private static OfflineMessage Message(byte body, DateTimeOffset? queuedAt = null)
    {
        return new OfflineMessage(
            Guid.NewGuid(), ReadOnlyMemory<byte>.Empty, new byte[] { body }, queuedAt ?? DateTimeOffset.UtcNow);
    }

    [Fact]
    public async Task TakeAllAsync_ReturnsMessagesInArrivalOrder()
    {
        var store = new InMemoryOfflineStore();

        for (byte i = 0; i < 5; i++)
        {
            Assert.True(await store.TryEnqueueAsync("worker", Message(i)));
        }

        IReadOnlyList<OfflineMessage> taken = await store.TakeAllAsync("worker");

        Assert.Equal(5, taken.Count);
        for (byte i = 0; i < 5; i++)
        {
            Assert.Equal(i, taken[i].Body.Span[0]);
        }
    }

    [Fact]
    public async Task TakeAllAsync_RemovesWhatItReturns()
    {
        var store = new InMemoryOfflineStore();
        Assert.True(await store.TryEnqueueAsync("worker", Message(1)));

        Assert.Single(await store.TakeAllAsync("worker"));
        Assert.Empty(await store.TakeAllAsync("worker"));
    }

    [Fact]
    public async Task TakeAllAsync_NameHoldingNothing_ReturnsEmpty()
    {
        var store = new InMemoryOfflineStore();

        Assert.Empty(await store.TakeAllAsync("never-seen"));
    }

    [Fact]
    public async Task TryEnqueueAsync_MessageCountBoundReached_RefusesTheNewMessage()
    {
        var store = new InMemoryOfflineStore(maxMessagesPerClient: 3);

        Assert.True(await store.TryEnqueueAsync("worker", Message(1)));
        Assert.True(await store.TryEnqueueAsync("worker", Message(2)));
        Assert.True(await store.TryEnqueueAsync("worker", Message(3)));
        Assert.False(await store.TryEnqueueAsync("worker", Message(4)));

        // Refusing rather than evicting is the documented policy: what was already accepted survives.
        IReadOnlyList<OfflineMessage> taken = await store.TakeAllAsync("worker");
        Assert.Equal([1, 2, 3], taken.Select(m => m.Body.Span[0]));
    }

    [Fact]
    public async Task TryEnqueueAsync_ByteBoundReached_RefusesTheNewMessage()
    {
        var store = new InMemoryOfflineStore(maxBytesPerClient: 10);
        var large = new OfflineMessage(
            Guid.NewGuid(), ReadOnlyMemory<byte>.Empty, new byte[8], DateTimeOffset.UtcNow);
        var overflowing = new OfflineMessage(
            Guid.NewGuid(), ReadOnlyMemory<byte>.Empty, new byte[8], DateTimeOffset.UtcNow);

        Assert.True(await store.TryEnqueueAsync("worker", large));
        Assert.False(await store.TryEnqueueAsync("worker", overflowing));
    }

    [Fact]
    public async Task TryEnqueueAsync_ByteBoundCountsHeadersAsWellAsBody()
    {
        var store = new InMemoryOfflineStore(maxBytesPerClient: 10);
        var withHeaders = new OfflineMessage(
            Guid.NewGuid(), new byte[6], new byte[6], DateTimeOffset.UtcNow);

        Assert.False(await store.TryEnqueueAsync("worker", withHeaders));
    }

    [Fact]
    public async Task TryEnqueueAsync_BoundsAreIndependentPerName()
    {
        var store = new InMemoryOfflineStore(maxMessagesPerClient: 1);

        Assert.True(await store.TryEnqueueAsync("worker-a", Message(1)));
        Assert.False(await store.TryEnqueueAsync("worker-a", Message(2)));
        Assert.True(await store.TryEnqueueAsync("worker-b", Message(3)));
    }

    [Fact]
    public async Task TryEnqueueAsync_NameCountBoundReached_RefusesAnUnseenName()
    {
        var store = new InMemoryOfflineStore(maxClients: 2);

        Assert.True(await store.TryEnqueueAsync("worker-a", Message(1)));
        Assert.True(await store.TryEnqueueAsync("worker-b", Message(2)));
        Assert.False(await store.TryEnqueueAsync("worker-c", Message(3)));

        // The cap bounds how many names hold anything, not how much each of them may hold — a name
        // already in the store is unaffected by it.
        Assert.True(await store.TryEnqueueAsync("worker-a", Message(4)));
    }

    [Fact]
    public async Task TakeAllAsync_DrainingAName_FreesItsSlotUnderTheNameCap()
    {
        var store = new InMemoryOfflineStore(maxClients: 1);
        Assert.True(await store.TryEnqueueAsync("worker-a", Message(1)));
        Assert.False(await store.TryEnqueueAsync("worker-b", Message(2)));

        await store.TakeAllAsync("worker-a");

        Assert.True(await store.TryEnqueueAsync("worker-b", Message(3)));
    }

    /// <summary>
    /// A message older than the retention window is discarded rather than delivered late. Pinned by
    /// supplying the queued-at instant directly instead of waiting for real time to pass, so the test is
    /// deterministic and instant.
    /// </summary>
    [Fact]
    public async Task TakeAllAsync_MessagePastItsRetentionWindow_IsDiscarded()
    {
        var store = new InMemoryOfflineStore(timeToLive: TimeSpan.FromMinutes(5));
        DateTimeOffset stale = DateTimeOffset.UtcNow.AddMinutes(-6);

        Assert.True(await store.TryEnqueueAsync("worker", Message(1, stale)));
        Assert.True(await store.TryEnqueueAsync("worker", Message(2)));

        IReadOnlyList<OfflineMessage> taken = await store.TakeAllAsync("worker");

        Assert.Equal([2], taken.Select(m => m.Body.Span[0]));
    }

    /// <summary>
    /// A queue that is only nominally full — every message in it having aged out — accepts a new message
    /// rather than refusing it, because expiry is purged before the bounds are tested.
    /// </summary>
    [Fact]
    public async Task TryEnqueueAsync_FullOfExpiredMessages_AcceptsTheNewOne()
    {
        var store = new InMemoryOfflineStore(maxMessagesPerClient: 2, timeToLive: TimeSpan.FromMinutes(5));
        DateTimeOffset stale = DateTimeOffset.UtcNow.AddMinutes(-6);

        Assert.True(await store.TryEnqueueAsync("worker", Message(1, stale)));
        Assert.True(await store.TryEnqueueAsync("worker", Message(2, stale)));
        Assert.True(await store.TryEnqueueAsync("worker", Message(3)));

        Assert.Equal([3], (await store.TakeAllAsync("worker")).Select(m => m.Body.Span[0]));
    }

    [Fact]
    public async Task TryEnqueueAsync_ConcurrentWritersToOneName_LoseNothing()
    {
        var store = new InMemoryOfflineStore(maxMessagesPerClient: 200);

        await Task.WhenAll(Enumerable.Range(0, 100).Select(async i =>
        {
            Assert.True(await store.TryEnqueueAsync("worker", Message((byte)i)));
        }));

        Assert.Equal(100, (await store.TakeAllAsync("worker")).Count);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_NonPositiveMessageBound_ThrowsArgumentOutOfRangeException(int maxMessages)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new InMemoryOfflineStore(maxMessagesPerClient: maxMessages));
    }

    [Fact]
    public void Constructor_NonPositiveByteBound_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InMemoryOfflineStore(maxBytesPerClient: 0));
    }

    [Fact]
    public void Constructor_NonPositiveRetentionWindow_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InMemoryOfflineStore(timeToLive: TimeSpan.Zero));
    }

    [Fact]
    public void Constructor_NonPositiveNameBound_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InMemoryOfflineStore(maxClients: 0));
    }

    [Fact]
    public async Task TryEnqueueAsync_EmptyClientName_ThrowsArgumentException()
    {
        var store = new InMemoryOfflineStore();

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await store.TryEnqueueAsync(string.Empty, Message(1)));
    }
}
