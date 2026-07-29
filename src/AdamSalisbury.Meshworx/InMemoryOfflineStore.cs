using System.Collections.Concurrent;

namespace AdamSalisbury.Meshworx;

/// <summary>
/// The default <see cref="IOfflineStore"/>: a bounded, process-local queue per client name. Everything
/// it holds is lost when the process ends — swap it for a durable implementation if messages must
/// survive a hub restart.
/// </summary>
/// <remarks>
/// <para>
/// Three independent bounds apply, all per client name: a message count, a total byte count, and a
/// retention window. A fourth bounds how many distinct names may hold anything at once, so a hub that
/// sees a long tail of short-lived names cannot accumulate one queue per name it has ever routed to.
/// </para>
/// <para>
/// <strong>A full queue refuses the new message rather than evicting the oldest.</strong> Both are
/// defensible; refusing is the one that keeps "accepted means it will be delivered, unless it expires
/// first" true, and it makes the loss visible at the moment and place it happens — the hub counts the
/// refusal on its dropped-message counter — instead of silently discarding a message that was already
/// accepted. A caller that wants the opposite policy can implement it in a few lines against
/// <see cref="IOfflineStore"/>.
/// </para>
/// <para>
/// Expired messages are purged lazily, on the next call touching that name, rather than by a timer: a
/// store with nothing being sent to it does no work at all, and a queue whose messages have all aged out
/// accepts new ones again rather than staying full until someone reconnects to drain it.
/// </para>
/// </remarks>
public sealed class InMemoryOfflineStore : IOfflineStore
{
    /// <summary>The default per-name message-count bound.</summary>
    public const int DefaultMaxMessagesPerClient = 100;

    /// <summary>The default per-name byte bound, 1 MiB.</summary>
    public const int DefaultMaxBytesPerClient = 1024 * 1024;

    /// <summary>The default number of distinct client names that may hold messages at once.</summary>
    public const int DefaultMaxClients = 1000;

    /// <summary>The default retention window, five minutes.</summary>
    public static readonly TimeSpan DefaultTimeToLive = TimeSpan.FromMinutes(5);

    private readonly int _maxMessagesPerClient;
    private readonly int _maxBytesPerClient;
    private readonly int _maxClients;
    private readonly TimeSpan _timeToLive;

    // One queue per name, each guarded by its own lock so distinct names never contend — the same shape
    // the hub uses for groups, including the Removed flag that closes the drain/enqueue race when a
    // queue empties and is taken out of the dictionary.
    private readonly ConcurrentDictionary<string, ClientQueue> _queues = new(StringComparer.Ordinal);

    /// <summary>
    /// Creates a store with the given per-name bounds.
    /// </summary>
    /// <param name="maxMessagesPerClient">
    /// How many messages one name may hold. Defaults to <see cref="DefaultMaxMessagesPerClient"/>.
    /// </param>
    /// <param name="maxBytesPerClient">
    /// How many bytes of message body and headers one name may hold. Defaults to
    /// <see cref="DefaultMaxBytesPerClient"/>.
    /// </param>
    /// <param name="timeToLive">
    /// How long a message is retained before it is discarded undelivered. Defaults to
    /// <see cref="DefaultTimeToLive"/>.
    /// </param>
    /// <param name="maxClients">
    /// How many distinct names may hold messages at once. Once reached, a name that holds nothing yet is
    /// refused rather than admitted. Defaults to <see cref="DefaultMaxClients"/>.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">Any bound is not positive.</exception>
    public InMemoryOfflineStore(
        int? maxMessagesPerClient = null,
        int? maxBytesPerClient = null,
        TimeSpan? timeToLive = null,
        int? maxClients = null)
    {
        if (maxMessagesPerClient is { } maxMessages && maxMessages <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxMessagesPerClient), "The maximum message count must be positive.");
        }

        if (maxBytesPerClient is { } maxBytes && maxBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxBytesPerClient), "The maximum byte count must be positive.");
        }

        if (timeToLive is { } retention && retention <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeToLive), "The retention window must be positive.");
        }

        if (maxClients is { } maxNames && maxNames <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxClients), "The maximum client count must be positive.");
        }

        _maxMessagesPerClient = maxMessagesPerClient ?? DefaultMaxMessagesPerClient;
        _maxBytesPerClient = maxBytesPerClient ?? DefaultMaxBytesPerClient;
        _timeToLive = timeToLive ?? DefaultTimeToLive;
        _maxClients = maxClients ?? DefaultMaxClients;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Purges anything already past the retention window for this name before testing the bounds, so a
    /// queue that is only nominally full — every message in it having aged out — accepts the new message
    /// rather than refusing it.
    /// </remarks>
    public ValueTask<bool> TryEnqueueAsync(
        string clientName, OfflineMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientName);
        ArgumentNullException.ThrowIfNull(message);

        cancellationToken.ThrowIfCancellationRequested();

        DateTimeOffset now = DateTimeOffset.UtcNow;

        while (true)
        {
            if (!_queues.TryGetValue(clientName, out ClientQueue? queue))
            {
                // Count the dictionary only when a genuinely new name is about to be added: an existing
                // name enqueueing its second message must never be refused by the name cap.
                if (_queues.Count >= _maxClients)
                {
                    return ValueTask.FromResult(false);
                }

                queue = _queues.GetOrAdd(clientName, static _ => new ClientQueue());
            }

            lock (queue.Lock)
            {
                if (queue.Removed)
                {
                    // The queue drained to empty and was taken out of the dictionary between the lookup
                    // above and this lock. Go round again against whatever is mapped now.
                    continue;
                }

                queue.PurgeExpired(now - _timeToLive);

                if (queue.Messages.Count >= _maxMessagesPerClient
                    || queue.ByteCount + message.ByteCount > _maxBytesPerClient)
                {
                    return ValueTask.FromResult(false);
                }

                queue.Messages.Enqueue(message);
                queue.ByteCount += message.ByteCount;
                return ValueTask.FromResult(true);
            }
        }
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<OfflineMessage>> TakeAllAsync(
        string clientName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientName);

        cancellationToken.ThrowIfCancellationRequested();

        if (!_queues.TryGetValue(clientName, out ClientQueue? queue))
        {
            return ValueTask.FromResult<IReadOnlyList<OfflineMessage>>([]);
        }

        OfflineMessage[] taken;

        lock (queue.Lock)
        {
            queue.PurgeExpired(DateTimeOffset.UtcNow - _timeToLive);
            taken = [.. queue.Messages];
            queue.Messages.Clear();
            queue.ByteCount = 0;

            // Mark removed and drop it from the dictionary under the same lock, and only if this exact
            // instance is still mapped — mirroring how the hub retires an emptied group. Removing inside
            // the lock is what makes the retry in TryEnqueueAsync terminate: a caller that acquires this
            // lock and sees Removed is guaranteed to find the entry already gone (or replaced by a live
            // one) when it goes back round, rather than fetching the same dead queue again.
            queue.Removed = true;
            _queues.TryRemove(new KeyValuePair<string, ClientQueue>(clientName, queue));
        }

        return ValueTask.FromResult<IReadOnlyList<OfflineMessage>>(taken);
    }

    private sealed class ClientQueue
    {
        public Lock Lock { get; } = new();

        public Queue<OfflineMessage> Messages { get; } = new();

        public int ByteCount { get; set; }

        public bool Removed { get; set; }

        /// <summary>
        /// Discards every message queued before <paramref name="cutoff"/>. The queue is in arrival
        /// order, so the expired ones are always a prefix and dequeueing stops at the first survivor.
        /// </summary>
        public void PurgeExpired(DateTimeOffset cutoff)
        {
            while (Messages.TryPeek(out OfflineMessage? oldest) && oldest.QueuedAt < cutoff)
            {
                Messages.Dequeue();
                ByteCount -= oldest.ByteCount;
            }
        }
    }
}
