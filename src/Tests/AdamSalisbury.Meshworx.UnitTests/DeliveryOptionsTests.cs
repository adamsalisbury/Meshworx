using AdamSalisbury.Meshworx.Messages;

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

    [Fact]
    public void None_DoesNotAwaitCapacity()
    {
        Assert.False(DeliveryOptions.None.AwaitCapacity);
    }

    [Fact]
    public void AwaitingCapacity_SetsAwaitCapacityWithoutRequiringAcknowledgement()
    {
        DeliveryOptions options = DeliveryOptions.AwaitingCapacity();

        Assert.True(options.AwaitCapacity);
        Assert.False(options.RequireAcknowledgement);
        Assert.Null(options.AcknowledgementTimeout);
    }

    [Fact]
    public void WithAwaitCapacity_OnRequireAck_KeepsAcknowledgementAndAddsAwaitCapacity()
    {
        DeliveryOptions options = DeliveryOptions.RequireAck(TimeSpan.FromSeconds(5)).WithAwaitCapacity();

        Assert.True(options.RequireAcknowledgement);
        Assert.Equal(TimeSpan.FromSeconds(5), options.AcknowledgementTimeout);
        Assert.True(options.AwaitCapacity);
    }

    [Fact]
    public void Equality_DifferentAwaitCapacity_AreNotEqual()
    {
        DeliveryOptions first = DeliveryOptions.None;
        DeliveryOptions second = DeliveryOptions.AwaitingCapacity();

        Assert.NotEqual(first, second);
        Assert.False(first == second);
        Assert.True(first != second);
    }

    [Fact]
    public void None_IsNormalPriority()
    {
        Assert.Equal(MessagePriority.Normal, DeliveryOptions.None.Priority);
    }

    [Fact]
    public void AtPriority_SetsPriorityWithoutRequiringAcknowledgementOrCapacity()
    {
        DeliveryOptions options = DeliveryOptions.AtPriority(MessagePriority.High);

        Assert.Equal(MessagePriority.High, options.Priority);
        Assert.False(options.RequireAcknowledgement);
        Assert.False(options.AwaitCapacity);
        Assert.Null(options.AcknowledgementTimeout);
    }

    [Fact]
    public void WithPriority_OnRequireAckWithAwaitCapacity_KeepsExistingOptionsAndAddsPriority()
    {
        DeliveryOptions options = DeliveryOptions.RequireAck(TimeSpan.FromSeconds(5))
            .WithAwaitCapacity()
            .WithPriority(MessagePriority.Low);

        Assert.True(options.RequireAcknowledgement);
        Assert.Equal(TimeSpan.FromSeconds(5), options.AcknowledgementTimeout);
        Assert.True(options.AwaitCapacity);
        Assert.Equal(MessagePriority.Low, options.Priority);
    }

    [Fact]
    public void Equality_DifferentPriority_AreNotEqual()
    {
        DeliveryOptions first = DeliveryOptions.AtPriority(MessagePriority.High);
        DeliveryOptions second = DeliveryOptions.AtPriority(MessagePriority.Low);

        Assert.NotEqual(first, second);
        Assert.False(first == second);
        Assert.True(first != second);
    }
}
