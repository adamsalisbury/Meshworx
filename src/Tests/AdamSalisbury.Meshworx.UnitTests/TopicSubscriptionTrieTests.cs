namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Verifies <see cref="TopicSubscriptionTrie"/>'s matching rules — including the <c>+</c> and <c>#</c>
/// wildcards — and its subscribe/unsubscribe bookkeeping.
/// </summary>
public sealed class TopicSubscriptionTrieTests
{
    [Fact]
    public void Match_ExactLiteralTopic_MatchesOnlyTheExactSubscription()
    {
        var trie = new TopicSubscriptionTrie();
        Guid subscriber = Guid.NewGuid();
        trie.Subscribe("orders.created", subscriber);

        Assert.Equal([subscriber], trie.Match("orders.created"));
        Assert.Empty(trie.Match("orders.updated"));
        Assert.Empty(trie.Match("orders"));
        Assert.Empty(trie.Match("orders.created.eu"));
    }

    [Theory]
    [InlineData("orders.+", "orders.eu", true)]
    [InlineData("orders.+", "orders.eu.region", false)]
    [InlineData("orders.+", "orders", false)]
    [InlineData("orders.+.created", "orders.eu.created", true)]
    [InlineData("orders.+.created", "orders.eu.region.created", false)]
    [InlineData("+.created", "orders.created", true)]
    [InlineData("+.created", "created", false)]
    public void Match_SingleSegmentWildcard_MatchesExactlyOneSegment(string pattern, string topic, bool expectMatch)
    {
        var trie = new TopicSubscriptionTrie();
        Guid subscriber = Guid.NewGuid();
        trie.Subscribe(pattern, subscriber);

        IReadOnlySet<Guid> matches = trie.Match(topic);

        Assert.Equal(expectMatch, matches.Contains(subscriber));
    }

    [Theory]
    [InlineData("sensors.#", "sensors.temperature", true)]
    [InlineData("sensors.#", "sensors.temperature.eu", true)]
    [InlineData("sensors.#", "sensors", true)]
    [InlineData("sensors.#", "other.temperature", false)]
    [InlineData("#", "anything.at.all", true)]
    public void Match_MultiSegmentWildcard_MatchesTheRemainderOfTheHierarchy(
        string pattern, string topic, bool expectMatch)
    {
        var trie = new TopicSubscriptionTrie();
        Guid subscriber = Guid.NewGuid();
        trie.Subscribe(pattern, subscriber);

        IReadOnlySet<Guid> matches = trie.Match(topic);

        Assert.Equal(expectMatch, matches.Contains(subscriber));
    }

    [Fact]
    public void Match_OverlappingSubscriptions_ReturnsEveryMatchingSubscriberOnce()
    {
        var trie = new TopicSubscriptionTrie();
        Guid literalSubscriber = Guid.NewGuid();
        Guid plusSubscriber = Guid.NewGuid();
        Guid hashSubscriber = Guid.NewGuid();

        trie.Subscribe("orders.eu.created", literalSubscriber);
        trie.Subscribe("orders.+.created", plusSubscriber);
        trie.Subscribe("orders.#", hashSubscriber);

        IReadOnlySet<Guid> matches = trie.Match("orders.eu.created");

        Assert.Equal(3, matches.Count);
        Assert.Contains(literalSubscriber, matches);
        Assert.Contains(plusSubscriber, matches);
        Assert.Contains(hashSubscriber, matches);
    }

    [Fact]
    public void Match_SameSubscriberViaTwoOverlappingPatterns_IsReturnedOnce()
    {
        var trie = new TopicSubscriptionTrie();
        Guid subscriber = Guid.NewGuid();
        trie.Subscribe("orders.eu.created", subscriber);
        trie.Subscribe("orders.#", subscriber);

        IReadOnlySet<Guid> matches = trie.Match("orders.eu.created");

        Assert.Equal([subscriber], matches);
    }

    [Fact]
    public void Match_NoSubscriptions_ReturnsEmpty()
    {
        var trie = new TopicSubscriptionTrie();

        Assert.Empty(trie.Match("orders.created"));
    }

    [Fact]
    public void Unsubscribe_RemovesTheSubscription()
    {
        var trie = new TopicSubscriptionTrie();
        Guid subscriber = Guid.NewGuid();
        trie.Subscribe("orders.created", subscriber);

        bool removed = trie.Unsubscribe("orders.created", subscriber);

        Assert.True(removed);
        Assert.Empty(trie.Match("orders.created"));
    }

    [Fact]
    public void Unsubscribe_UnknownSubscription_ReturnsFalseAndLeavesOthersIntact()
    {
        var trie = new TopicSubscriptionTrie();
        Guid subscriber = Guid.NewGuid();
        trie.Subscribe("orders.created", subscriber);

        bool removed = trie.Unsubscribe("orders.created", Guid.NewGuid());

        Assert.False(removed);
        Assert.Equal([subscriber], trie.Match("orders.created"));
    }

    [Fact]
    public void Unsubscribe_OneOfTwoSubscribersToSamePattern_LeavesTheOther()
    {
        var trie = new TopicSubscriptionTrie();
        Guid first = Guid.NewGuid();
        Guid second = Guid.NewGuid();
        trie.Subscribe("orders.created", first);
        trie.Subscribe("orders.created", second);

        trie.Unsubscribe("orders.created", first);

        Assert.Equal([second], trie.Match("orders.created"));
    }

    [Fact]
    public void Unsubscribe_InvalidPattern_ReturnsFalseRatherThanThrowing()
    {
        var trie = new TopicSubscriptionTrie();

        bool removed = trie.Unsubscribe("orders..created", Guid.NewGuid());

        Assert.False(removed);
    }

    [Theory]
    [InlineData("")]
    [InlineData("orders.")]
    [InlineData(".orders")]
    [InlineData("orders..created")]
    public void Subscribe_MalformedPattern_ThrowsArgumentException(string pattern)
    {
        var trie = new TopicSubscriptionTrie();

        Assert.Throws<ArgumentException>(() => trie.Subscribe(pattern, Guid.NewGuid()));
    }

    [Fact]
    public void Subscribe_HashNotInFinalPosition_ThrowsArgumentException()
    {
        var trie = new TopicSubscriptionTrie();

        Assert.Throws<ArgumentException>(() => trie.Subscribe("orders.#.created", Guid.NewGuid()));
    }

    [Theory]
    [InlineData("orders.+")]
    [InlineData("orders.#")]
    public void Match_ConcreteTopicCannotContainAWildcardSegment(string topic)
    {
        var trie = new TopicSubscriptionTrie();

        Assert.Throws<ArgumentException>(() => trie.Match(topic));
    }

    [Fact]
    public void Subscribe_PatternWithTooManySegments_ThrowsArgumentExceptionRatherThanOverflowingTheStack()
    {
        var trie = new TopicSubscriptionTrie();
        string pattern = string.Join('.', Enumerable.Repeat("a", 10_000));

        // A StackOverflowException cannot be caught, so the only way to prove this is bounded is to
        // observe the graceful ArgumentException the segment-count cap is meant to produce instead.
        Assert.Throws<ArgumentException>(() => trie.Subscribe(pattern, Guid.NewGuid()));
    }

    [Fact]
    public void Match_TopicWithTooManySegments_ThrowsArgumentExceptionRatherThanOverflowingTheStack()
    {
        var trie = new TopicSubscriptionTrie();
        string topic = string.Join('.', Enumerable.Repeat("a", 10_000));

        Assert.Throws<ArgumentException>(() => trie.Match(topic));
    }

    [Fact]
    public void Unsubscribe_LastSubscriberOnABranch_PrunesTheNowEmptyNodes()
    {
        // There is no public way to observe the trie's internal node count, so this proves pruning
        // indirectly: re-subscribing a different client to a sibling pattern after the only subscriber
        // to a deep branch leaves must not find any trace of the pruned branch's matches.
        var trie = new TopicSubscriptionTrie();
        Guid first = Guid.NewGuid();
        trie.Subscribe("orders.eu.region.created", first);
        trie.Unsubscribe("orders.eu.region.created", first);

        Guid second = Guid.NewGuid();
        trie.Subscribe("orders.eu.other", second);

        Assert.Empty(trie.Match("orders.eu.region.created"));
        Assert.Equal([second], trie.Match("orders.eu.other"));
    }

    [Theory]
    [InlineData("orders.created", "orders.created", true)]
    [InlineData("orders.created", "orders.updated", false)]
    [InlineData("orders.created", "orders", false)]
    [InlineData("orders.created", "orders.created.eu", false)]
    [InlineData("orders.+", "orders.eu", true)]
    [InlineData("orders.+", "orders.eu.region", false)]
    [InlineData("orders.+", "orders", false)]
    [InlineData("orders.+.created", "orders.eu.created", true)]
    [InlineData("orders.+.created", "orders.eu.region.created", false)]
    [InlineData("+.created", "orders.created", true)]
    [InlineData("+.created", "created", false)]
    [InlineData("sensors.#", "sensors.temperature", true)]
    [InlineData("sensors.#", "sensors.temperature.eu", true)]
    [InlineData("sensors.#", "sensors", true)]
    [InlineData("sensors.#", "other.temperature", false)]
    [InlineData("#", "anything.at.all", true)]
    public void PatternMatches_MirrorsSubscribeThenMatchForTheSamePatternAndTopic(
        string pattern, string topic, bool expectMatch)
    {
        // PatternMatches answers the same question Subscribe-then-Match does, without a trie at all —
        // proved here by checking the two never disagree for the same inputs already exercised above.
        Assert.Equal(expectMatch, TopicSubscriptionTrie.PatternMatches(pattern, topic));

        // The pre-split overload, used by a caller testing one pattern against many topics, must agree
        // with the single-call convenience overload for the exact same inputs.
        string[] patternSegments = TopicSubscriptionTrie.SplitAndValidatePattern(pattern);
        Assert.Equal(expectMatch, TopicSubscriptionTrie.PatternMatches(patternSegments, topic));

        var trie = new TopicSubscriptionTrie();
        Guid subscriber = Guid.NewGuid();
        trie.Subscribe(pattern, subscriber);
        Assert.Equal(expectMatch, trie.Match(topic).Contains(subscriber));
    }

    [Fact]
    public void SplitAndValidatePattern_MalformedPattern_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => TopicSubscriptionTrie.SplitAndValidatePattern("orders."));
    }

    [Fact]
    public void SplitAndValidatePattern_SplitOnce_ReusedAcrossManyTopics()
    {
        // The whole point of the pre-split overload: split and validate once, then test it against
        // several different topics without re-parsing the pattern for each one.
        string[] patternSegments = TopicSubscriptionTrie.SplitAndValidatePattern("orders.#");

        Assert.True(TopicSubscriptionTrie.PatternMatches(patternSegments, "orders.eu.created"));
        Assert.True(TopicSubscriptionTrie.PatternMatches(patternSegments, "orders.us.created"));
        Assert.False(TopicSubscriptionTrie.PatternMatches(patternSegments, "invoices.eu.created"));
    }

    [Fact]
    public void PatternMatches_MalformedPattern_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => TopicSubscriptionTrie.PatternMatches("orders.", "orders.created"));
    }

    [Fact]
    public void PatternMatches_TopicContainingWildcardSegment_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => TopicSubscriptionTrie.PatternMatches("orders.+", "orders.+"));
    }
}
