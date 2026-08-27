using System.Globalization;
using System.IO.Compression;
using System.Text;
using AdamSalisbury.Meshworx.Compression;

namespace AdamSalisbury.Meshworx.UnitTests.Compression;

public sealed class BuiltInCompressionStrategyTests
{
    public static TheoryData<ICompressionStrategy> BuiltIns =>
        new() { BrotliCompressionStrategy.Default, DeflateCompressionStrategy.Default };

    [Fact]
    public void Brotli_AlgorithmId_IsTheContentCodingName()
    {
        Assert.Equal("br", BrotliCompressionStrategy.Default.AlgorithmId);
        Assert.Equal(CompressionAlgorithms.Brotli, BrotliCompressionStrategy.Default.AlgorithmId);
    }

    [Fact]
    public void Deflate_AlgorithmId_IsTheContentCodingName()
    {
        Assert.Equal("deflate", DeflateCompressionStrategy.Default.AlgorithmId);
        Assert.Equal(CompressionAlgorithms.Deflate, DeflateCompressionStrategy.Default.AlgorithmId);
    }

    [Theory]
    [MemberData(nameof(BuiltIns))]
    public void RoundTrip_CompressiblePayload_IsSmallerAndByteIdentical(ICompressionStrategy strategy)
    {
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        ReadOnlyMemory<byte> compressed = strategy.Compress(payload);
        ReadOnlyMemory<byte> restored = strategy.Decompress(compressed, payload.Length);

        Assert.True(
            compressed.Length < payload.Length,
            $"{strategy.AlgorithmId} grew a compressible payload: {payload.Length} -> {compressed.Length}.");
        Assert.Equal(payload.ToArray(), restored.ToArray());
    }

    [Theory]
    [MemberData(nameof(BuiltIns))]
    public void RoundTrip_EmptyPayload_IsEmpty(ICompressionStrategy strategy)
    {
        ReadOnlyMemory<byte> compressed = strategy.Compress(ReadOnlyMemory<byte>.Empty);

        Assert.Empty(strategy.Decompress(compressed, maxDecompressedBytes: 1).ToArray());
    }

    [Theory]
    [MemberData(nameof(BuiltIns))]
    public void RoundTrip_IncompressiblePayload_IsByteIdentical(ICompressionStrategy strategy)
    {
        // Random bytes cannot be compressed, so this is the case where Compress legitimately returns more
        // than it was given. It must still round-trip; deciding the result is not worth sending is the
        // caller's job, not the strategy's.
        byte[] payload = new byte[16 * 1024];
        new Random(20260827).NextBytes(payload);

        ReadOnlyMemory<byte> restored = strategy.Decompress(strategy.Compress(payload), payload.Length * 2);

        Assert.Equal(payload, restored.ToArray());
    }

    [Theory]
    [MemberData(nameof(BuiltIns))]
    public void Decompress_OutputExceedingTheBound_ThrowsInvalidDataException(ICompressionStrategy strategy)
    {
        // A megabyte of zeros is the decompression bomb in miniature: kilobytes on the wire, a megabyte in
        // memory. The bound must stop it rather than the caller discovering the size afterwards.
        byte[] payload = new byte[1024 * 1024];
        ReadOnlyMemory<byte> compressed = strategy.Compress(payload);

        var exception = Assert.Throws<InvalidDataException>(
            () => strategy.Decompress(compressed, maxDecompressedBytes: 1024));

        Assert.Contains("1024", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(BuiltIns))]
    public void Decompress_BoundExactlyTheOutputSize_Succeeds(ICompressionStrategy strategy)
    {
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        ReadOnlyMemory<byte> restored = strategy.Decompress(strategy.Compress(payload), payload.Length);

        Assert.Equal(payload.Length, restored.Length);
    }

    [Theory]
    [MemberData(nameof(BuiltIns))]
    public void Decompress_NonPositiveBound_ThrowsArgumentOutOfRangeException(ICompressionStrategy strategy)
    {
        ReadOnlyMemory<byte> compressed = strategy.Compress(CompressiblePayload());

        Assert.Throws<ArgumentOutOfRangeException>(() => strategy.Decompress(compressed, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => strategy.Decompress(compressed, -1));
    }

    [Theory]
    [MemberData(nameof(BuiltIns))]
    public void Decompress_Garbage_ThrowsInvalidDataException(ICompressionStrategy strategy)
    {
        // The framework disagrees with itself here — BrotliStream raises InvalidOperationException and
        // DeflateStream InvalidDataException for the same situation — so this is asserting the
        // normalisation, not just that something was thrown.
        byte[] garbage = new byte[512];
        new Random(1).NextBytes(garbage);

        Assert.Throws<InvalidDataException>(() => strategy.Decompress(garbage, 64 * 1024));
    }

    [Theory]
    [MemberData(nameof(BuiltIns))]
    public void Decompress_TruncatedBody_ReturnsAPrefixRatherThanThrowing(ICompressionStrategy strategy)
    {
        // Pinning a limitation, not endorsing it. Both decompressors read a truncated body as a stream
        // that simply ended, so half a compressed payload silently yields a prefix of the original rather
        // than an error. Nothing at this layer can tell the difference: a strategy is handed bytes and
        // has no idea how many it was supposed to produce. The guard belongs one level up, where the
        // uncompressed length is known and can travel with the message. Recorded as KI-74.
        ReadOnlyMemory<byte> payload = CompressiblePayload();
        ReadOnlyMemory<byte> compressed = strategy.Compress(payload);

        ReadOnlyMemory<byte> restored = strategy.Decompress(compressed[..(compressed.Length / 2)], payload.Length);

        Assert.True(restored.Length < payload.Length, "A truncated body unexpectedly restored in full.");
        Assert.True(payload.Span.StartsWith(restored.Span), "A truncated body restored to something other than a prefix.");
    }

    [Fact]
    public void Decompress_TheOtherAlgorithmsOutput_FailsCleanly()
    {
        // The wrong strategy for a body is a configuration mismatch, not a crash: it must surface as a
        // data error rather than as silently plausible bytes.
        ReadOnlyMemory<byte> brotli = BrotliCompressionStrategy.Default.Compress(CompressiblePayload());

        Assert.Throws<InvalidDataException>(
            () => DeflateCompressionStrategy.Default.Decompress(brotli, 64 * 1024));
    }

    [Theory]
    [InlineData(CompressionLevel.NoCompression)]
    [InlineData(CompressionLevel.Fastest)]
    [InlineData(CompressionLevel.Optimal)]
    [InlineData(CompressionLevel.SmallestSize)]
    public void Brotli_AnyLevel_IsReadableByTheDefaultInstance(CompressionLevel level)
    {
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        ReadOnlyMemory<byte> compressed = new BrotliCompressionStrategy(level).Compress(payload);

        Assert.Equal(
            payload.ToArray(),
            BrotliCompressionStrategy.Default.Decompress(compressed, payload.Length).ToArray());
    }

    [Theory]
    [InlineData(CompressionLevel.NoCompression)]
    [InlineData(CompressionLevel.Fastest)]
    [InlineData(CompressionLevel.Optimal)]
    [InlineData(CompressionLevel.SmallestSize)]
    public void Deflate_AnyLevel_IsReadableByTheDefaultInstance(CompressionLevel level)
    {
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        ReadOnlyMemory<byte> compressed = new DeflateCompressionStrategy(level).Compress(payload);

        Assert.Equal(
            payload.ToArray(),
            DeflateCompressionStrategy.Default.Decompress(compressed, payload.Length).ToArray());
    }

    [Theory]
    [MemberData(nameof(BuiltIns))]
    public async Task Strategy_UsedConcurrently_RoundTripsEveryPayload(ICompressionStrategy strategy)
    {
        // A single instance is shared by every send and receive that resolves to its id, so it has to
        // tolerate being used from many threads at once without the caller synchronising.
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        Task<bool>[] workers = [.. Enumerable.Range(0, 32).Select(_ => Task.Run(() =>
        {
            ReadOnlyMemory<byte> restored = strategy.Decompress(strategy.Compress(payload), payload.Length);
            return restored.Span.SequenceEqual(payload.Span);
        }))];

        Assert.All(await Task.WhenAll(workers), Assert.True);
    }

    private static ReadOnlyMemory<byte> CompressiblePayload()
    {
        var builder = new StringBuilder();

        for (int i = 0; i < 200; i++)
        {
            builder.Append(
                CultureInfo.InvariantCulture,
                $"{{\"deviceId\":\"sensor-{i:D4}\",\"temperature\":21.5,\"humidity\":48,\"status\":\"nominal\"}},");
        }

        return Encoding.UTF8.GetBytes(builder.ToString());
    }
}
