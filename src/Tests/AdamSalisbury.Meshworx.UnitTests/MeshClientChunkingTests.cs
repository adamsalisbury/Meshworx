using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Isolates the chunking tests from every other test class.
/// </summary>
/// <remarks>
/// These tests exist to move multi-megabyte payloads, so they allocate and copy far more than any
/// other class here. Run in parallel on a loaded two-core CI runner that pressure is enough to push a
/// test with a sub-second timeout — the WebSocket listener tests allow 1000 ms — past its budget for
/// reasons that have nothing to do with what it is testing. Serialising them keeps this class's cost
/// to itself.
/// </remarks>
[CollectionDefinition(ChunkingCollectionDefinition.Name, DisableParallelization = true)]
public sealed class ChunkingCollectionDefinition
{
    public const string Name = "Chunking";
}

[Collection(ChunkingCollectionDefinition.Name)]
public class MeshClientChunkingTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Sizes chosen to exercise multi-chunk paths at the least allocation that still does so.
    /// </summary>
    private static class MeshClientTestSizes
    {
        internal const int JustOverOneChunk = (1024 * 1024) + 1;
    }

    /// <summary>
    /// Acceptance criterion: a payload beyond the 1 MiB frame cap round-trips correctly. The sender
    /// splits it, each chunk is a legal frame, and the receiver raises exactly one message carrying the
    /// original bytes.
    /// </summary>
    [Fact(Timeout = 30000)]
    public async Task SendLargeAsync_PayloadBeyondFrameCap_RoundTripsThroughReassembly()
    {
        // 3 MiB, filled with a position-dependent pattern so a mis-ordered or truncated reassembly
        // fails the comparison rather than passing on length alone.
        var payload = new byte[3 * 1024 * 1024];
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i * 31 % 251);
        }

        var senderFixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        senderFixture.SetupSuccessfulRegistration();
        await senderFixture.ConnectAsync();

        senderFixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) => sentFrames.Add(frame.ToArray()))
            .Returns(Task.CompletedTask);

        var recipientId = Guid.NewGuid();
        await senderFixture.Client.SendLargeAsync(recipientId, payload);

        // More than one frame, and every one of them within the transport's cap.
        Assert.True(sentFrames.Count > 1, $"expected chunking, got {sentFrames.Count} frame(s)");
        Assert.All(sentFrames, f => Assert.True(f.Length <= 1024 * 1024, $"frame of {f.Length} bytes"));

        // Feed the sender's own frames to a receiving client, rewritten from SendMessageWithHeaders
        // into the DeliverMessageWithHeaders frames the hub would have produced.
        var receiverFixture = new MeshClientFixture();
        var received = new TaskCompletionSource<byte[]>();
        var raisedCount = 0;

        receiverFixture.SetupSuccessfulRegistration(
            [.. sentFrames.Select(f => ToDeliveryFrame(f, senderFixture.Client.Id))]);

        receiverFixture.Client.MessageReceived += (_, e) =>
        {
            Interlocked.Increment(ref raisedCount);
            received.TrySetResult(e.Data.ToArray());
        };

        await receiverFixture.Client.ConnectAsync(receiverFixture.Transport.Object, "Recipient");

        byte[] reassembled = await received.Task.WaitAsync(WaitTimeout);

        Assert.Equal(payload.Length, reassembled.Length);
        Assert.True(payload.AsSpan().SequenceEqual(reassembled), "reassembled payload differs");

        // Exactly one message, not one per chunk: a subscriber never sees a partial message.
        Assert.Equal(1, raisedCount);
    }

    /// <summary>
    /// A payload that fits in one frame still goes as a single chunk and arrives whole, so a caller
    /// need not decide which send method to use based on size.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task SendLargeAsync_SmallPayload_StillRoundTrips()
    {
        byte[] payload = [1, 2, 3, 4, 5];

        var senderFixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        senderFixture.SetupSuccessfulRegistration();
        await senderFixture.ConnectAsync();

        senderFixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) => sentFrames.Add(frame.ToArray()))
            .Returns(Task.CompletedTask);

        await senderFixture.Client.SendLargeAsync(Guid.NewGuid(), payload);

        Assert.Single(sentFrames);

        var receiverFixture = new MeshClientFixture();
        var received = new TaskCompletionSource<byte[]>();

        receiverFixture.SetupSuccessfulRegistration(
            ToDeliveryFrame(sentFrames[0], senderFixture.Client.Id));

        receiverFixture.Client.MessageReceived += (_, e) => received.TrySetResult(e.Data.ToArray());
        await receiverFixture.Client.ConnectAsync(receiverFixture.Transport.Object, "Recipient");

        Assert.Equal(payload, await received.Task.WaitAsync(WaitTimeout));
    }

    /// <summary>
    /// An empty payload is still one chunk. Sending none would complete no transfer at the far end, so
    /// the message would silently never arrive.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task SendLargeAsync_EmptyPayload_SendsOneChunk()
    {
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        fixture.SetupSuccessfulRegistration();
        await fixture.ConnectAsync();

        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) => sentFrames.Add(frame.ToArray()))
            .Returns(Task.CompletedTask);

        await fixture.Client.SendLargeAsync(Guid.NewGuid(), ReadOnlyMemory<byte>.Empty);

        Assert.Single(sentFrames);
    }

    /// <summary>
    /// Caller headers ride on every chunk and are delivered once, with the reassembled message.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task SendLargeAsync_WithCallerHeaders_DeliversThemWithTheWholeMessage()
    {
        var payload = new byte[MeshClientTestSizes.JustOverOneChunk];
        var senderFixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        senderFixture.SetupSuccessfulRegistration();
        await senderFixture.ConnectAsync();

        senderFixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) => sentFrames.Add(frame.ToArray()))
            .Returns(Task.CompletedTask);

        var headers = new MessageHeaders(new Dictionary<string, string> { ["kind"] = "report" });
        await senderFixture.Client.SendLargeAsync(Guid.NewGuid(), payload, headers);

        Assert.True(sentFrames.Count > 1);

        var receiverFixture = new MeshClientFixture();
        var received = new TaskCompletionSource<MessageHeaders>();

        receiverFixture.SetupSuccessfulRegistration(
            [.. sentFrames.Select(f => ToDeliveryFrame(f, senderFixture.Client.Id))]);

        receiverFixture.Client.MessageReceived += (_, e) => received.TrySetResult(e.Headers);
        await receiverFixture.Client.ConnectAsync(receiverFixture.Transport.Object, "Recipient");

        MessageHeaders delivered = await received.Task.WaitAsync(WaitTimeout);
        Assert.Equal("report", delivered["kind"]);
    }

    /// <summary>
    /// Chunking needs the header envelope to carry its reassembly metadata, so a peer that cannot
    /// receive headers cannot receive a chunked message. Unlike trace context — an optional extra that
    /// degrades silently — this is a failed send: the caller explicitly asked to send something that
    /// cannot go any other way.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task SendLargeAsync_OnPreHeaderEnvelopePeer_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion(
            (byte)(Protocol.HeaderEnvelopeMinVersion - 1));
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<NotSupportedException>(
            () => fixture.Client.SendLargeAsync(Guid.NewGuid(), new byte[10]));
    }

    [Fact(Timeout = 10000)]
    public async Task SendLargeAsync_WithReservedHeaderKey_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistration();
        await fixture.ConnectAsync();

        var headers = new MessageHeaders(
            new Dictionary<string, string> { [MessagePriorityHeaderKeys.Priority] = "2" });

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.SendLargeAsync(Guid.NewGuid(), new byte[10], headers));
    }

    /// <summary>
    /// Rewrites a SendMessageWithHeaders frame (client to hub) into the DeliverMessageWithHeaders frame
    /// (hub to client) the hub would forward for it. The hub copies the header block through untouched,
    /// replacing only the addressing, so the two differ by the message type and by carrying the sender's
    /// id rather than the recipient's.
    /// </summary>
    private static byte[] ToDeliveryFrame(byte[] sendFrame, Guid senderId)
    {
        Assert.Equal((byte)MessageType.SendMessageWithHeaders, sendFrame[0]);

        var delivery = new byte[sendFrame.Length];
        sendFrame.CopyTo(delivery, 0);
        delivery[0] = (byte)MessageType.DeliverMessageWithHeaders;
        senderId.TryWriteBytes(delivery.AsSpan(1, 16));
        return delivery;
    }
}
