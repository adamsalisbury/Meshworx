using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Covers the two client-side gaps a correctness review found after the initial topic pub/sub
/// implementation: a malformed pattern or topic was only ever rejected inside the hub's trie, never at
/// the client boundary the interface's own documentation promises; and the feature had no protocol
/// version gate, unlike every other incrementally added wire capability.
/// </summary>
public sealed class TopicPubSubClientTests
{
    [Theory]
    [InlineData("")]
    [InlineData("orders.")]
    [InlineData("orders..created")]
    public async Task SubscribeAsync_MalformedPattern_ThrowsArgumentException(string pattern)
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => fixture.Client.SubscribeAsync(pattern));
    }

    [Fact]
    public async Task SubscribeAsync_HashNotInFinalPosition_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => fixture.Client.SubscribeAsync("orders.#.created"));
    }

    [Fact]
    public async Task PublishAsync_TopicContainingWildcardSegment_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.PublishAsync("orders.+", new byte[1]));
    }

    [Fact]
    public async Task SubscribeAsync_MalformedPattern_NeverSendsAFrame()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => fixture.Client.SubscribeAsync("orders."));

        // Only the registration frame that ConnectAsync itself sent should ever have gone out; the
        // malformed subscribe must be rejected before anything else reaches the transport.
        fixture.Transport.Verify(
            t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SubscribeAsync_OnPreTopicPubSubPeer_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion((byte)(Protocol.TopicPubSubMinVersion - 1));
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<NotSupportedException>(() => fixture.Client.SubscribeAsync("orders.created"));
    }

    [Fact]
    public async Task UnsubscribeAsync_OnPreTopicPubSubPeer_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion((byte)(Protocol.TopicPubSubMinVersion - 1));
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<NotSupportedException>(() => fixture.Client.UnsubscribeAsync("orders.created"));
    }

    [Fact]
    public async Task PublishAsync_OnPreTopicPubSubPeer_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion((byte)(Protocol.TopicPubSubMinVersion - 1));
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<NotSupportedException>(
            () => fixture.Client.PublishAsync("orders.created", new byte[1]));
    }

    [Fact]
    public async Task SubscribeAsync_AtTopicPubSubMinVersion_Succeeds()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion(Protocol.TopicPubSubMinVersion);
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await fixture.Client.SubscribeAsync("orders.created");

        Assert.Contains("orders.created", fixture.Client.SubscribedTopics);
    }
}
