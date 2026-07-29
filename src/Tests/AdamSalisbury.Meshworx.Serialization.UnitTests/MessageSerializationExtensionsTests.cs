using System.Text;
using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx.Serialization.UnitTests;

public class MessageSerializationExtensionsTests
{
    private static readonly Guid SenderId = Guid.Parse("22222222-2222-2222-2222-222222222222");

    [Fact]
    public void Deserialize_MatchingContentType_ReturnsValue()
    {
        MessageReceivedEventArgs message = DirectMessage("""{"X":3,"Y":4}""", "application/json");

        Assert.Equal(new Point(3, 4), message.Deserialize<Point>(JsonMessageSerializer.Default));
    }

    /// <summary>
    /// A body produced by a different codec is refused rather than decoded. Bytes alone carry no format,
    /// so without this check a codec would happily turn another's output into a plausible wrong value.
    /// </summary>
    [Fact]
    public void Deserialize_ForeignContentType_Throws()
    {
        MessageReceivedEventArgs message = DirectMessage("""{"X":3,"Y":4}""", "application/x-msgpack");

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => message.Deserialize<Point>(JsonMessageSerializer.Default));

        Assert.Contains("application/x-msgpack", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A message with no content type at all is accepted: the header is only written by this package's
    /// typed sends, so its absence means a byte-oriented sender, and the caller reaching for a codec is
    /// asserting it knows the format.
    /// </summary>
    [Fact]
    public void Deserialize_NoContentTypeHeader_DecodesAnyway()
    {
        MessageReceivedEventArgs message = DirectMessage("""{"X":1,"Y":1}""", contentType: null);

        Assert.Equal(new Point(1, 1), message.Deserialize<Point>(JsonMessageSerializer.Default));
    }

    [Fact]
    public void TryDeserialize_MatchingContentType_ReturnsTrueAndValue()
    {
        MessageReceivedEventArgs message = DirectMessage("""{"X":5,"Y":6}""", "application/json");

        Assert.True(message.TryDeserialize(JsonMessageSerializer.Default, out Point? value));
        Assert.Equal(new Point(5, 6), value);
    }

    [Fact]
    public void TryDeserialize_ForeignContentType_ReturnsFalse()
    {
        MessageReceivedEventArgs message = DirectMessage("""{"X":5,"Y":6}""", "application/x-msgpack");

        Assert.False(message.TryDeserialize(JsonMessageSerializer.Default, out Point? value));
        Assert.Null(value);
    }

    /// <summary>
    /// A malformed body from a remote peer is an ordinary runtime condition, not a programming error:
    /// the Try shape reports it as false rather than throwing into a receive loop.
    /// </summary>
    [Fact]
    public void TryDeserialize_MalformedBody_ReturnsFalseRatherThanThrowing()
    {
        MessageReceivedEventArgs message = DirectMessage("{ not json at all", "application/json");

        Assert.False(message.TryDeserialize(JsonMessageSerializer.Default, out Point? value));
        Assert.Null(value);
    }

    [Fact]
    public void TryDeserialize_GroupMessage_DecodesTheSameWay()
    {
        var message = new GroupMessageReceivedEventArgs
        {
            SenderId = SenderId,
            GroupName = "news",
            Data = Encoding.UTF8.GetBytes("""{"X":8,"Y":9}"""),
            Headers = new MessageHeaders(
                new Dictionary<string, string>
                {
                    [SerializationHeaderKeys.ContentType] = "application/json",
                }),
        };

        Assert.True(message.TryDeserialize(JsonMessageSerializer.Default, out Point? value));
        Assert.Equal(new Point(8, 9), value);
    }

    [Fact]
    public void Deserialize_GroupMessage_ForeignContentType_Throws()
    {
        var message = new GroupMessageReceivedEventArgs
        {
            SenderId = SenderId,
            GroupName = "news",
            Data = Encoding.UTF8.GetBytes("""{"X":8,"Y":9}"""),
            Headers = new MessageHeaders(
                new Dictionary<string, string>
                {
                    [SerializationHeaderKeys.ContentType] = "application/x-msgpack",
                }),
        };

        Assert.Throws<InvalidOperationException>(
            () => message.Deserialize<Point>(JsonMessageSerializer.Default));
    }

    /// <summary>
    /// The whole point of the abstraction: a second codec plugs in and round-trips through the same
    /// send and receive extensions with no change to the core library or to this package.
    /// </summary>
    [Fact]
    public void TryDeserialize_SecondCodec_WorksThroughTheSameExtensions()
    {
        var codec = new UpperCaseTextSerializer();
        MessageReceivedEventArgs message = DirectMessage("HELLO", codec.ContentType);

        Assert.True(message.TryDeserialize(codec, out string? value));
        Assert.Equal("HELLO", value);

        // And the JSON codec correctly declines the same message rather than decoding it.
        Assert.False(message.TryDeserialize(JsonMessageSerializer.Default, out string? _));
    }

    private static MessageReceivedEventArgs DirectMessage(string body, string? contentType)
    {
        MessageHeaders headers = contentType is null
            ? MessageHeaders.Empty
            : new MessageHeaders(
                new Dictionary<string, string> { [SerializationHeaderKeys.ContentType] = contentType });

        return new MessageReceivedEventArgs
        {
            SenderId = SenderId,
            Data = Encoding.UTF8.GetBytes(body),
            Headers = headers,
        };
    }

    private sealed record Point(int X, int Y);

    /// <summary>
    /// A deliberately trivial second codec, present to prove the abstraction holds: swapping it in
    /// requires no change to the core library, and it is distinguished from JSON purely by its content
    /// type.
    /// </summary>
    private sealed class UpperCaseTextSerializer : IMessageSerializer
    {
        public string ContentType => "text/plain; case=upper";

        public ReadOnlyMemory<byte> Serialize<TValue>(TValue value)
        {
            return Encoding.UTF8.GetBytes(value?.ToString()?.ToUpperInvariant() ?? string.Empty);
        }

        public TValue? Deserialize<TValue>(ReadOnlySpan<byte> data)
        {
            return (TValue)(object)Encoding.UTF8.GetString(data);
        }
    }
}
