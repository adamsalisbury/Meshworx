using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx.UnitTests.Messages;

public sealed class MessageExpiryHeaderKeysTests
{
    [Fact]
    public void TryParseExpiry_ValidValue_ReturnsTrueAndExpiry()
    {
        var expected = DateTimeOffset.UtcNow;
        string value = expected.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture);

        bool parsed = MessageExpiryHeaderKeys.TryParseExpiry(value, out DateTimeOffset expiry);

        Assert.True(parsed);
        Assert.Equal(expected.ToUnixTimeMilliseconds(), expiry.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void TryParseExpiry_Null_ReturnsFalse()
    {
        bool parsed = MessageExpiryHeaderKeys.TryParseExpiry(null, out DateTimeOffset expiry);

        Assert.False(parsed);
        Assert.Equal(default, expiry);
    }

    [Fact]
    public void TryParseExpiry_NonNumeric_ReturnsFalse()
    {
        bool parsed = MessageExpiryHeaderKeys.TryParseExpiry("not-a-number", out DateTimeOffset expiry);

        Assert.False(parsed);
    }

    /// <summary>
    /// A value that parses as a perfectly good long but falls outside the range DateTimeOffset can
    /// represent must be tolerated as "does not expire", not thrown from — this is the regression a
    /// crafted or merely malformed header must never be able to trigger, since it comes straight from
    /// sender-controlled bytes and this method must never crash either the hub's send loop or the
    /// recipient's receive loop over it.
    /// </summary>
    [Theory]
    [InlineData("9223372036854775807")] // long.MaxValue
    [InlineData("-9223372036854775808")] // long.MinValue
    public void TryParseExpiry_OutOfRangeValue_ReturnsFalseWithoutThrowing(string value)
    {
        bool parsed = MessageExpiryHeaderKeys.TryParseExpiry(value, out DateTimeOffset expiry);

        Assert.False(parsed);
        Assert.Equal(default, expiry);
    }
}
