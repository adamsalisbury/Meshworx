using System.IO.Compression;

namespace AdamSalisbury.Meshworx.Compression;

/// <summary>
/// The built-in raw DEFLATE <see cref="ICompressionStrategy"/>, registered under
/// <see cref="CompressionAlgorithms.Deflate"/>.
/// </summary>
/// <remarks>
/// Kept alongside <see cref="BrotliCompressionStrategy"/> because it is cheaper per byte and is
/// understood by essentially everything, which matters once the peer at the other end is not a .NET
/// process — a bridge, a gateway, or a consumer's own implementation.
/// </remarks>
public sealed class DeflateCompressionStrategy : ICompressionStrategy
{
    /// <summary>
    /// A shared instance at <see cref="CompressionLevel.Optimal"/>, the one
    /// <see cref="CompressionStrategyRegistry.CreateDefault"/> registers.
    /// </summary>
    public static readonly DeflateCompressionStrategy Default = new();

    private readonly CompressionLevel _level;

    /// <summary>
    /// Initialises a new instance of the <see cref="DeflateCompressionStrategy"/> class at
    /// <see cref="CompressionLevel.Optimal"/>.
    /// </summary>
    public DeflateCompressionStrategy()
        : this(CompressionLevel.Optimal)
    {
    }

    /// <summary>
    /// Initialises a new instance of the <see cref="DeflateCompressionStrategy"/> class at a specific
    /// compression level.
    /// </summary>
    /// <param name="level">How hard to work at compressing. Affects only this endpoint's sends — a receiver decompresses any level.</param>
    public DeflateCompressionStrategy(CompressionLevel level)
    {
        _level = level;
    }

    /// <inheritdoc/>
    public string AlgorithmId => CompressionAlgorithms.Deflate;

    /// <inheritdoc/>
    public ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte> payload)
    {
        return StreamCompression.Compress(
            payload,
            _level,
            static (destination, level) => new DeflateStream(destination, level, leaveOpen: true));
    }

    /// <inheritdoc/>
    public ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> payload, int maxDecompressedBytes)
    {
        return StreamCompression.Decompress(
            payload,
            maxDecompressedBytes,
            static source => new DeflateStream(source, CompressionMode.Decompress),
            CompressionAlgorithms.Deflate);
    }
}
