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

    [Fact]
    public void None_RequestsNoCompression()
    {
        Assert.False(DeliveryOptions.None.Compress);
        Assert.Null(DeliveryOptions.None.CompressionAlgorithmId);
    }

    [Fact]
    public void Compressed_NoAlgorithm_RequestsTheBestAvailable()
    {
        DeliveryOptions options = DeliveryOptions.Compressed();

        Assert.True(options.Compress);
        Assert.Null(options.CompressionAlgorithmId);
        Assert.False(options.RequireAcknowledgement);
        Assert.False(options.AwaitCapacity);
        Assert.Equal(MessagePriority.Normal, options.Priority);
    }

    [Fact]
    public void Compressed_NamedAlgorithm_CarriesTheId()
    {
        DeliveryOptions options = DeliveryOptions.Compressed("zstd");

        Assert.True(options.Compress);
        Assert.Equal("zstd", options.CompressionAlgorithmId);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Compressed_EmptyAlgorithm_ThrowsArgumentException(string algorithmId)
    {
        Assert.Throws<ArgumentException>(() => DeliveryOptions.Compressed(algorithmId));
    }

    [Fact]
    public void Compressed_NullAlgorithm_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => DeliveryOptions.Compressed(null!));
    }

    [Fact]
    public void WithCompression_CombinesWithEveryOtherOption()
    {
        DeliveryOptions options = DeliveryOptions
            .RequireAck(TimeSpan.FromSeconds(5))
            .WithAwaitCapacity()
            .WithPriority(MessagePriority.High)
            .WithCompression("zstd");

        Assert.True(options.RequireAcknowledgement);
        Assert.Equal(TimeSpan.FromSeconds(5), options.AcknowledgementTimeout);
        Assert.True(options.AwaitCapacity);
        Assert.Equal(MessagePriority.High, options.Priority);
        Assert.True(options.Compress);
        Assert.Equal("zstd", options.CompressionAlgorithmId);
    }

    [Fact]
    public void WithAwaitCapacityAndWithPriority_PreserveCompression()
    {
        // Both predate compression, so both had to be taught to carry it forward rather than reset it.
        DeliveryOptions options = DeliveryOptions.Compressed("zstd").WithAwaitCapacity().WithPriority(MessagePriority.Low);

        Assert.True(options.Compress);
        Assert.Equal("zstd", options.CompressionAlgorithmId);
    }

    [Fact]
    public void WithCompression_EmptyAlgorithm_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => DeliveryOptions.None.WithCompression("  "));
    }

    [Fact]
    public void Equality_DistinguishesCompressionButIgnoresAlgorithmIdCasing()
    {
        Assert.NotEqual(DeliveryOptions.None, DeliveryOptions.Compressed());
        Assert.NotEqual(DeliveryOptions.Compressed(), DeliveryOptions.Compressed("br"));
        Assert.Equal(DeliveryOptions.Compressed("br"), DeliveryOptions.Compressed("BR"));
        Assert.Equal(
            DeliveryOptions.Compressed("br").GetHashCode(), DeliveryOptions.Compressed("BR").GetHashCode());
    }
}
