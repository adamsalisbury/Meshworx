using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx.UnitTests.Messages;

public sealed class MessageHeadersTests
{
    [Fact]
    public void Empty_HasNoEntries()
    {
        Assert.Empty(MessageHeaders.Empty);
    }

    [Fact]
    public void Constructor_NullValues_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new MessageHeaders(null!));
    }

    [Fact]
    public void Constructor_CopiesSuppliedPairs()
    {
        var source = new Dictionary<string, string> { ["correlationId"] = "abc123" };
        var headers = new MessageHeaders(source);

        source["correlationId"] = "mutated";

        Assert.Equal("abc123", headers["correlationId"]);
    }

    [Fact]
    public void Indexer_KnownKey_ReturnsValue()
    {
        var headers = new MessageHeaders([new("contentType", "application/json")]);

        Assert.Equal("application/json", headers["contentType"]);
    }

    [Fact]
    public void TryGetValue_UnknownKey_ReturnsFalse()
    {
        var headers = new MessageHeaders([new("contentType", "application/json")]);

        Assert.False(headers.TryGetValue("missing", out string? value));
        Assert.Null(value);
    }

    [Fact]
    public void GetEnumerator_YieldsEveryPair()
    {
        var headers = new MessageHeaders(
        [
            new("a", "1"),
            new("b", "2"),
        ]);

        var pairs = headers.ToDictionary(pair => pair.Key, pair => pair.Value);

        Assert.Equal(2, pairs.Count);
        Assert.Equal("1", pairs["a"]);
        Assert.Equal("2", pairs["b"]);
    }
}
