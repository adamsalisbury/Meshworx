namespace AdamSalisbury.Meshworx.UnitTests;

public sealed class DeliveryOptionsTests
{
    [Fact]
    public void None_RequiresNoAcknowledgement()
    {
        Assert.False(DeliveryOptions.None.RequireAcknowledgement);
        Assert.Null(DeliveryOptions.None.AcknowledgementTimeout);
    }

    [Fact]
    public void Default_IsEquivalentToNone()
    {
        Assert.Equal(DeliveryOptions.None, default);
    }

    [Fact]
    public void RequireAck_PositiveTimeout_SetsProperties()
    {
        DeliveryOptions options = DeliveryOptions.RequireAck(TimeSpan.FromSeconds(5));

        Assert.True(options.RequireAcknowledgement);
        Assert.Equal(TimeSpan.FromSeconds(5), options.AcknowledgementTimeout);
    }

    [Fact]
    public void RequireAck_ZeroTimeout_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => DeliveryOptions.RequireAck(TimeSpan.Zero));
    }

    [Fact]
    public void RequireAck_NegativeTimeout_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => DeliveryOptions.RequireAck(TimeSpan.FromSeconds(-1)));
    }

    [Fact]
    public void Equality_SameValues_AreEqual()
    {
        DeliveryOptions first = DeliveryOptions.RequireAck(TimeSpan.FromSeconds(1));
        DeliveryOptions second = DeliveryOptions.RequireAck(TimeSpan.FromSeconds(1));

        Assert.Equal(first, second);
        Assert.True(first == second);
        Assert.False(first != second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
    }

    [Fact]
    public void Equality_DifferentTimeouts_AreNotEqual()
    {
        DeliveryOptions first = DeliveryOptions.RequireAck(TimeSpan.FromSeconds(1));
        DeliveryOptions second = DeliveryOptions.RequireAck(TimeSpan.FromSeconds(2));

        Assert.NotEqual(first, second);
        Assert.False(first == second);
        Assert.True(first != second);
    }
}
