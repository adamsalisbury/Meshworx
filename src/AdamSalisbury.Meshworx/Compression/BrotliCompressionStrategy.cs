using System.IO.Compression;

namespace AdamSalisbury.Meshworx.Compression;

/// <summary>
/// The built-in Brotli <see cref="ICompressionStrategy"/>, registered under
/// <see cref="CompressionAlgorithms.Brotli"/>.
/// </summary>
/// <remarks>
/// The better of the two built-ins for the payloads this library tends to carry — JSON, telemetry
/// batches, anything text-shaped and repetitive — which is why
/// <see cref="CompressionStrategyRegistry.CreateDefault"/> registers it first, ahead of
/// <see cref="DeflateCompressionStrategy"/>, and so prefers it.
/// </remarks>
public sealed class BrotliCompressionStrategy : ICompressionStrategy
{
    /// <summary>
    /// A shared instance at <see cref="CompressionLevel.Optimal"/>, the one
    /// <see cref="CompressionStrategyRegistry.CreateDefault"/> registers.
    /// </summary>
    public static readonly BrotliCompressionStrategy Default = new();

    private readonly CompressionLevel _level;

    /// <summary>
    /// Initialises a new instance of the <see cref="BrotliCompressionStrategy"/> class at
    /// <see cref="CompressionLevel.Optimal"/>.
    /// </summary>
    public BrotliCompressionStrategy()
        : this(CompressionLevel.Optimal)
    {
    }

    /// <summary>
    /// Initialises a new instance of the <see cref="BrotliCompressionStrategy"/> class at a specific
    /// compression level.
    /// </summary>
    /// <param name="level">How hard to work at compressing. Affects only this endpoint's sends — a receiver decompresses any level.</param>
    public BrotliCompressionStrategy(CompressionLevel level)
    {
        _level = level;
    }

    /// <inheritdoc/>
    public string AlgorithmId => CompressionAlgorithms.Brotli;

    /// <inheritdoc/>
    public ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte> payload)
    {
        return StreamCompression.Compress(
            payload,
            _level,
            static (destination, level) => new BrotliStream(destination, level, leaveOpen: true));
    }

    /// <inheritdoc/>
    public ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> payload, int maxDecompressedBytes)
    {
        return StreamCompression.Decompress(
            payload,
            maxDecompressedBytes,
            static source => new BrotliStream(source, CompressionMode.Decompress),
            CompressionAlgorithms.Brotli);
    }
}
