namespace AdamSalisbury.Meshworx;

/// <summary>
/// Tracks every client's topic-pattern subscriptions in a segment trie and matches a published topic
/// against them without scanning the whole subscriber population.
/// </summary>
/// <remarks>
/// <para>
/// A topic is a dot-separated hierarchy — <c>orders.eu.created</c> — and a subscription pattern is the
/// same shape with two reserved wildcard segments borrowed from MQTT: <c>+</c> matches exactly one
/// segment, and <c>#</c> matches the rest of the hierarchy from that point on and may only appear as the
/// final segment of a pattern. <c>orders.+.created</c> matches <c>orders.eu.created</c> but not
/// <c>orders.eu.region.created</c>; <c>orders.#</c> matches <c>orders.eu.created</c>,
/// <c>orders.eu.region.created</c>, and — mirroring MQTT's own treatment of <c>#</c> — the parent topic
/// <c>orders</c> itself. Neither wildcard character may appear in a concrete topic passed to
/// <see cref="Match"/> — only in a pattern passed to <see cref="Subscribe"/>.
/// </para>
/// <para>
/// Matching walks one trie node per topic segment, so its cost is proportional to the topic's depth and
/// the branching factor actually subscribed at each level — never to the total number of subscribers the
/// trie holds, which is what keeps it sub-linear in subscriber count for a typical, shallow tree. A
/// <see cref="ReaderWriterLockSlim"/> guards the tree rather than a plain mutual-exclusion lock: matching
/// is on the publish hot path and is read-only with respect to the tree's shape, so any number of
/// publishes can traverse it at once, while <see cref="Subscribe"/> and <see cref="Unsubscribe"/> — far
/// rarer, and the only operations that mutate it — take the exclusive write lock. Neither lock is ever
/// held while a message is actually being delivered; only the traversal itself is covered.
/// </para>
/// </remarks>
internal sealed class TopicSubscriptionTrie : IDisposable
{
    /// <summary>The wildcard segment that matches exactly one topic segment.</summary>
    internal const string SingleSegmentWildcard = "+";

    /// <summary>The wildcard segment that matches the remainder of the topic hierarchy.</summary>
    internal const string MultiSegmentWildcard = "#";

    private readonly Node _root = new();
    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private int _disposed;

    /// <summary>
    /// Registers <paramref name="clientId"/> as a subscriber of <paramref name="pattern"/>.
    /// </summary>
    /// <param name="pattern">
    /// The topic pattern to subscribe to. May contain <see cref="SingleSegmentWildcard"/> segments
    /// anywhere and a single trailing <see cref="MultiSegmentWildcard"/> segment.
    /// </param>
    /// <param name="clientId">The subscribing client's identifier.</param>
    /// <exception cref="ArgumentException"><paramref name="pattern"/> is not a valid pattern.</exception>
    public void Subscribe(string pattern, Guid clientId)
    {
        string[] segments = SplitAndValidate(pattern, isPattern: true);

        _lock.EnterWriteLock();
        try
        {
            Node node = _root;
            foreach (string segment in segments)
            {
                node = segment switch
                {
                    SingleSegmentWildcard => node.Plus ??= new Node(),
                    MultiSegmentWildcard => node.Hash ??= new Node(),
                    _ => GetOrAddLiteralChild(node, segment),
                };
            }

            (node.Subscribers ??= new HashSet<Guid>()).Add(clientId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes <paramref name="clientId"/>'s subscription to <paramref name="pattern"/>, if it holds one,
    /// pruning any trie nodes the removal leaves empty.
    /// </summary>
    /// <param name="pattern">The topic pattern to unsubscribe from.</param>
    /// <param name="clientId">The unsubscribing client's identifier.</param>
    /// <returns><see langword="true"/> if a subscription was removed; otherwise <see langword="false"/>.</returns>
    public bool Unsubscribe(string pattern, Guid clientId)
    {
        string[] segments;
        try
        {
            segments = SplitAndValidate(pattern, isPattern: true);
        }
        catch (ArgumentException)
        {
            // An invalid pattern was never accepted by Subscribe, so there is nothing to remove.
            return false;
        }

        _lock.EnterWriteLock();
        try
        {
            return RemovePath(_root, segments, 0, clientId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Finds every client subscribed to a pattern that matches <paramref name="topic"/>.
    /// </summary>
    /// <param name="topic">The concrete topic a message was published to.</param>
    /// <returns>
    /// The distinct set of matching subscribers, or an empty array if none match. Never
    /// <see langword="null"/>.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="topic"/> is not a valid concrete topic.</exception>
    public IReadOnlyList<Guid> Match(string topic)
    {
        string[] segments = SplitAndValidate(topic, isPattern: false);
        var results = new HashSet<Guid>();

        _lock.EnterReadLock();
        try
        {
            Collect(_root, segments, 0, results);
        }
        finally
        {
            _lock.ExitReadLock();
        }

        return results.Count == 0 ? [] : [.. results];
    }

    /// <summary>
    /// Releases the underlying <see cref="ReaderWriterLockSlim"/>. Safe to call more than once.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _lock.Dispose();
        }
    }

    private static void Collect(Node node, string[] segments, int index, HashSet<Guid> results)
    {
        // A '#' child swallows every remaining segment — including zero of them — so its subscribers
        // match regardless of how much of the topic is left to consume.
        if (node.Hash is { Subscribers.Count: > 0 } hashChild)
        {
            results.UnionWith(hashChild.Subscribers!);
        }

        if (index == segments.Length)
        {
            if (node.Subscribers is { Count: > 0 })
            {
                results.UnionWith(node.Subscribers);
            }

            return;
        }

        string segment = segments[index];

        if (node.Literal is not null && node.Literal.TryGetValue(segment, out Node? literalChild))
        {
            Collect(literalChild, segments, index + 1, results);
        }

        if (node.Plus is not null)
        {
            Collect(node.Plus, segments, index + 1, results);
        }
    }

    private static bool RemovePath(Node node, string[] segments, int index, Guid clientId)
    {
        if (index == segments.Length)
        {
            return node.Subscribers?.Remove(clientId) ?? false;
        }

        string segment = segments[index];
        Node? child = segment switch
        {
            SingleSegmentWildcard => node.Plus,
            MultiSegmentWildcard => node.Hash,
            _ => node.Literal is not null && node.Literal.TryGetValue(segment, out Node? literalChild)
                ? literalChild
                : null,
        };

        if (child is null)
        {
            return false;
        }

        bool removed = RemovePath(child, segments, index + 1, clientId);

        if (child.IsEmpty)
        {
            switch (segment)
            {
                case SingleSegmentWildcard:
                    node.Plus = null;
                    break;
                case MultiSegmentWildcard:
                    node.Hash = null;
                    break;
                default:
                    node.Literal?.Remove(segment);
                    if (node.Literal is { Count: 0 })
                    {
                        node.Literal = null;
                    }

                    break;
            }
        }

        return removed;
    }

    private static Node GetOrAddLiteralChild(Node node, string segment)
    {
        node.Literal ??= new Dictionary<string, Node>(StringComparer.Ordinal);

        if (!node.Literal.TryGetValue(segment, out Node? child))
        {
            child = new Node();
            node.Literal[segment] = child;
        }

        return child;
    }

    // Collect and RemovePath recurse once per segment, so this is also the effective cap on their
    // recursion depth. Neither a topic nor a pattern is otherwise length-bounded — a client could
    // otherwise pack tens of thousands of single-character segments into one still well-under-1-MiB
    // frame — and a StackOverflowException cannot be caught in .NET, so an uncapped depth would let a
    // single malformed subscribe or publish take the whole hub process down rather than just that one
    // frame. 128 is generous for any topic hierarchy a real deployment would use while keeping the worst
    // case nowhere near a default 1 MiB thread stack.
    private const int MaxSegmentCount = 128;

    /// <summary>
    /// Splits a topic or pattern into its dot-separated segments, validating it is well formed for the
    /// role it is being used in.
    /// </summary>
    private static string[] SplitAndValidate(string value, bool isPattern)
    {
        ArgumentException.ThrowIfNullOrEmpty(value);

        string[] segments = value.Split('.');

        if (segments.Length > MaxSegmentCount)
        {
            throw new ArgumentException(
                $"A topic or pattern cannot have more than {MaxSegmentCount} dot-separated segments.",
                nameof(value));
        }

        for (int i = 0; i < segments.Length; i++)
        {
            string segment = segments[i];

            if (segment.Length == 0)
            {
                throw new ArgumentException(
                    "A topic segment cannot be empty; check for a leading, trailing or repeated '.'.",
                    nameof(value));
            }

            if (!isPattern && (segment == SingleSegmentWildcard || segment == MultiSegmentWildcard))
            {
                throw new ArgumentException(
                    $"A concrete topic cannot contain the '{segment}' wildcard segment.", nameof(value));
            }

            if (segment == MultiSegmentWildcard && i != segments.Length - 1)
            {
                throw new ArgumentException(
                    "The '#' wildcard segment may only appear as the final segment of a pattern.",
                    nameof(value));
            }
        }

        return segments;
    }

    /// <summary>
    /// One segment's worth of the trie. Absent children and an absent subscriber set are represented as
    /// <see langword="null"/> rather than empty collections, so a topic hierarchy nobody has subscribed
    /// deeply into costs no more than the segments actually in use.
    /// </summary>
    private sealed class Node
    {
        public Dictionary<string, Node>? Literal { get; set; }

        public Node? Plus { get; set; }

        public Node? Hash { get; set; }

        public HashSet<Guid>? Subscribers { get; set; }

        public bool IsEmpty =>
            (Subscribers is null || Subscribers.Count == 0)
            && Plus is null
            && Hash is null
            && (Literal is null || Literal.Count == 0);
    }
}
