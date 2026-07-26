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

    /// <summary>
    /// A single value this long already exceeds the maximum block-length prefix too, so whichever check
    /// runs first — Write's own per-value check, or GetEncodedLength's aggregate check when the caller
    /// sizes its buffer the usual way — the whole pipeline must still reject it as an ArgumentException.
    /// </summary>
    [Fact]
    public void Write_ValueTooLong_ThrowsArgumentException()
    {
        string longValue = new('v', 65536);
        var headers = new MessageHeaders([new("key", longValue)]);

        Assert.Throws<ArgumentException>(() =>
        {
            int length = HeaderEnvelope.GetEncodedLength(headers);
            var buffer = new byte[length];
            HeaderEnvelope.Write(headers, buffer);
        });
    }

    /// <summary>
    /// Every individual key and value here is within its own per-entry limit, but their combined
    /// encoded length exceeds what the wire format's 2-byte block-length prefix can represent. This
    /// must be rejected rather than silently truncated when narrowed to a ushort by the caller.
    /// </summary>
    [Fact]
    public void GetEncodedLength_AggregateExceedsBlockLengthLimit_ThrowsArgumentException()
    {
        string largeValue = new('v', 65000);
        var headers = new MessageHeaders(
        [
            new("first", largeValue),
            new("second", largeValue),
        ]);

        Assert.Throws<ArgumentException>(() => HeaderEnvelope.GetEncodedLength(headers));
    }

    [Fact]
    public void Read_KeyRunsPastBlockLength_ThrowsFormatException()
    {
        // keyLength byte declares 5, but no bytes follow within the 1-byte block.
        byte[] block = [5];

        Assert.Throws<FormatException>(() => HeaderEnvelope.Read(block, block.Length));
    }

    [Fact]
    public void Read_TruncatedBeforeValueLengthField_ThrowsFormatException()
    {
        // keyLength(1)=1, key="a", then nothing — the 2-byte value-length field is missing entirely.
        byte[] block = [1, (byte)'a'];

        Assert.Throws<FormatException>(() => HeaderEnvelope.Read(block, block.Length));
    }

    [Fact]
    public void Read_ValueRunsPastBlockLength_ThrowsFormatException()
    {
        // keyLength(1)=1, key="a", valueLength(2, BE)=10, but no value bytes follow.
        byte[] block = [1, (byte)'a', 0, 10];

        Assert.Throws<FormatException>(() => HeaderEnvelope.Read(block, block.Length));
    }
}
