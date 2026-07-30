using System.Text;
using AdamSalisbury.Meshworx.Messages;
using Moq;

namespace AdamSalisbury.Meshworx.Serialization.UnitTests;

public class MeshClientSerializationExtensionsTests
{
    private static readonly Guid RecipientId = Guid.Parse("11111111-1111-1111-1111-111111111111");

    /// <summary>
    /// A typed send reaches the byte-oriented method underneath with the serialized body and a content
    /// type describing it — the two halves that make the value decodable at the other end.
    /// </summary>
    [Fact]
    public async Task SendAsync_Typed_SendsSerializedBodyTaggedWithContentType()
    {
        var client = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> sentBody = default;
        MessageHeaders? sentHeaders = null;

        client
            .Setup(c => c.SendAsync(
                RecipientId,
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<Guid, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, body, headers, _) => (sentBody, sentHeaders) = (body, headers))
            .Returns(Task.CompletedTask);

        await client.Object.SendAsync(RecipientId, new Point(3, 4), JsonMessageSerializer.Default);

        Assert.Equal("""{"X":3,"Y":4}""", Encoding.UTF8.GetString(sentBody.Span));
        Assert.Equal("application/json", sentHeaders![SerializationHeaderKeys.ContentType]);
    }

    /// <summary>
    /// Caller-supplied headers survive alongside the content type rather than being replaced by it.
    /// </summary>
    [Fact]
    public async Task SendAsync_Typed_WithCallerHeaders_PreservesThemAndAddsContentType()
    {
        var client = new Mock<IMeshClient>();
        MessageHeaders? sentHeaders = null;

        client
            .Setup(c => c.SendAsync(
                It.IsAny<Guid>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<Guid, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, _, headers, _) => sentHeaders = headers)
            .Returns(Task.CompletedTask);

        var callerHeaders = new MessageHeaders(new Dictionary<string, string> { ["trace"] = "abc" });
        await client.Object.SendAsync(
            RecipientId, new Point(1, 2), JsonMessageSerializer.Default, callerHeaders);

        Assert.Equal("abc", sentHeaders!["trace"]);
        Assert.Equal("application/json", sentHeaders[SerializationHeaderKeys.ContentType]);
    }

    /// <summary>
    /// The caller's own <see cref="MessageHeaders"/> is never mutated: it is immutable by contract and
    /// routinely reused across sends, so adding the content type must produce a copy.
    /// </summary>
    [Fact]
    public async Task SendAsync_Typed_DoesNotMutateCallerHeaders()
    {
        var client = new Mock<IMeshClient>();
        client
            .Setup(c => c.SendAsync(
                It.IsAny<Guid>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var callerHeaders = new MessageHeaders(new Dictionary<string, string> { ["trace"] = "abc" });
        await client.Object.SendAsync(
            RecipientId, new Point(1, 2), JsonMessageSerializer.Default, callerHeaders);

        Assert.Single(callerHeaders);
        Assert.False(callerHeaders.ContainsKey(SerializationHeaderKeys.ContentType));
    }

    /// <summary>
    /// A content type the caller set explicitly is left alone. Overwriting it would make a header the
    /// caller can set a header the caller cannot set.
    /// </summary>
    [Fact]
    public async Task SendAsync_Typed_CallerSuppliedContentType_IsNotOverwritten()
    {
        var client = new Mock<IMeshClient>();
        MessageHeaders? sentHeaders = null;

        client
            .Setup(c => c.SendAsync(
                It.IsAny<Guid>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<Guid, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, _, headers, _) => sentHeaders = headers)
            .Returns(Task.CompletedTask);

        var callerHeaders = new MessageHeaders(
            new Dictionary<string, string> { [SerializationHeaderKeys.ContentType] = "application/vnd.custom" });

        await client.Object.SendAsync(
            RecipientId, new Point(1, 2), JsonMessageSerializer.Default, callerHeaders);

        Assert.Equal("application/vnd.custom", sentHeaders![SerializationHeaderKeys.ContentType]);
    }

    [Fact]
    public async Task SendToGroupAsync_Typed_SendsSerializedBodyTaggedWithContentType()
    {
        var client = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> sentBody = default;
        MessageHeaders? sentHeaders = null;

        client
            .Setup(c => c.SendToGroupAsync(
                "news",
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, body, headers, _) => (sentBody, sentHeaders) = (body, headers))
            .Returns(Task.CompletedTask);

        await client.Object.SendToGroupAsync("news", new Point(5, 6), JsonMessageSerializer.Default);

        Assert.Equal("""{"X":5,"Y":6}""", Encoding.UTF8.GetString(sentBody.Span));
        Assert.Equal("application/json", sentHeaders![SerializationHeaderKeys.ContentType]);
    }

    /// <summary>
    /// A typed request serializes the outbound value, tags it with the codec's content type, and
    /// deserializes the reply with the same codec.
    /// </summary>
    /// <remarks>
    /// The content-type assertion is not incidental. Without the header a receiver carrying more than
    /// one codec accepts the request for all of them, so the first in the chain claims it and decodes
    /// another codec's bytes — the exact failure the header exists to prevent.
    /// </remarks>
    [Fact]
    public async Task RequestAsync_Typed_SerializesRequestAndDeserializesReply()
    {
        var client = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> sentBody = default;
        MessageHeaders? sentHeaders = null;

        client
            .Setup(c => c.RequestAsync(
                RecipientId,
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<Guid, ReadOnlyMemory<byte>, TimeSpan, MessageHeaders, CancellationToken>(
                (_, body, _, headers, _) => (sentBody, sentHeaders) = (body, headers))
            .ReturnsAsync((ReadOnlyMemory<byte>)Encoding.UTF8.GetBytes("""{"X":9,"Y":8}"""));

        Point? reply = await client.Object.RequestAsync<Point, Point>(
            RecipientId, new Point(1, 2), JsonMessageSerializer.Default, TimeSpan.FromSeconds(5));

        Assert.Equal("""{"X":1,"Y":2}""", Encoding.UTF8.GetString(sentBody.Span));
        Assert.Equal("application/json", sentHeaders![SerializationHeaderKeys.ContentType]);
        Assert.Equal(new Point(9, 8), reply);
    }

    /// <summary>
    /// A typed reply is tagged with the codec's content type too, for the same reason a typed request
    /// is: the requester decodes it, and has the same several codecs to choose between.
    /// </summary>
    [Fact]
    public async Task ReplyAsync_Typed_SendsSerializedBodyUnderItsContentType()
    {
        var client = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> sentBody = default;
        MessageHeaders? sentHeaders = null;

        client
            .Setup(c => c.ReplyAsync(
                It.IsAny<MessageReceivedEventArgs>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<MessageReceivedEventArgs, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, body, headers, _) => (sentBody, sentHeaders) = (body, headers))
            .Returns(Task.CompletedTask);

        var request = new MessageReceivedEventArgs { SenderId = RecipientId, Data = default };
        await client.Object.ReplyAsync(request, new Point(7, 7), JsonMessageSerializer.Default);

        Assert.Equal("""{"X":7,"Y":7}""", Encoding.UTF8.GetString(sentBody.Span));
        Assert.Equal("application/json", sentHeaders![SerializationHeaderKeys.ContentType]);
    }

    [Fact]
    public async Task SendAsync_Typed_NullSerializer_Throws()
    {
        var client = new Mock<IMeshClient>();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => client.Object.SendAsync(RecipientId, new Point(1, 1), null!));
    }

    private sealed record Point(int X, int Y);
}
