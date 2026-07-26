using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx.UnitTests.Messages;

public sealed class HeaderEnvelopeTests
{
    [Fact]
    public void GetEncodedLength_EmptyHeaders_IsZero()
    {
        Assert.Equal(0, HeaderEnvelope.GetEncodedLength(MessageHeaders.Empty));
    }

    [Fact]
    public void Read_ZeroLength_ReturnsEmpty()
    {
        MessageHeaders headers = HeaderEnvelope.Read(ReadOnlySpan<byte>.Empty, 0);

        Assert.Same(MessageHeaders.Empty, headers);
    }

    [Fact]
    public void WriteThenRead_SingleHeader_RoundTrips()
    {
        var original = new MessageHeaders([new("correlationId", "abc-123")]);
        int length = HeaderEnvelope.GetEncodedLength(original);
        var buffer = new byte[length];

        HeaderEnvelope.Write(original, buffer);
        MessageHeaders decoded = HeaderEnvelope.Read(buffer, length);

        Assert.Single(decoded);
        Assert.Equal("abc-123", decoded["correlationId"]);
    }

    [Fact]
    public void WriteThenRead_MultipleHeaders_RoundTrips()
    {
        var original = new MessageHeaders(
        [
            new("correlationId", "abc-123"),
            new("contentType", "application/json"),
            new("priority", "high"),
        ]);
        int length = HeaderEnvelope.GetEncodedLength(original);
        var buffer = new byte[length];

        HeaderEnvelope.Write(original, buffer);
        MessageHeaders decoded = HeaderEnvelope.Read(buffer, length);

        Assert.Equal(3, decoded.Count);
        Assert.Equal("abc-123", decoded["correlationId"]);
        Assert.Equal("application/json", decoded["contentType"]);
        Assert.Equal("high", decoded["priority"]);
    }

    [Fact]
    public void WriteThenRead_NonAsciiValue_RoundTrips()
    {
        var original = new MessageHeaders([new("greeting", "héllo wörld 你好")]);
        int length = HeaderEnvelope.GetEncodedLength(original);
        var buffer = new byte[length];

        HeaderEnvelope.Write(original, buffer);
        MessageHeaders decoded = HeaderEnvelope.Read(buffer, length);

        Assert.Equal("héllo wörld 你好", decoded["greeting"]);
    }

    [Fact]
    public void WriteThenRead_EmptyValue_RoundTrips()
    {
        var original = new MessageHeaders([new("flag", string.Empty)]);
        int length = HeaderEnvelope.GetEncodedLength(original);
        var buffer = new byte[length];

        HeaderEnvelope.Write(original, buffer);
        MessageHeaders decoded = HeaderEnvelope.Read(buffer, length);

        Assert.Equal(string.Empty, decoded["flag"]);
    }

    [Fact]
    public void Write_KeyTooLong_ThrowsArgumentException()
    {
        string longKey = new('k', 256);
        var headers = new MessageHeaders([new(longKey, "value")]);
        int length = HeaderEnvelope.GetEncodedLength(headers);
        var buffer = new byte[length];

        Assert.Throws<ArgumentException>(() => HeaderEnvelope.Write(headers, buffer));
    }

    [Fact]
    public void Write_ValueTooLong_ThrowsArgumentException()
    {
        string longValue = new('v', 65536);
        var headers = new MessageHeaders([new("key", longValue)]);
        int length = HeaderEnvelope.GetEncodedLength(headers);
        var buffer = new byte[length];

        Assert.Throws<ArgumentException>(() => HeaderEnvelope.Write(headers, buffer));
    }
}
