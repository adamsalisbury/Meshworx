namespace AdamSalisbury.Meshworx.Compression;

/// <summary>
/// The algorithm ids of the strategies registered by
/// <see cref="CompressionStrategyRegistry.CreateDefault"/>.
/// </summary>
/// <remarks>
/// These are the HTTP content-coding names, borrowed deliberately rather than invented: the ids travel on
/// the wire between two endpoints that may not share a Meshworx version, and reusing a registry the rest
/// of the industry already agrees on costs nothing and avoids a private vocabulary. A consumer registering
/// its own strategy should pick a name from that same registry where one exists — <c>zstd</c>, <c>gzip</c>
/// — and only invent an id for something genuinely bespoke.
/// </remarks>
public static class CompressionAlgorithms
{
    /// <summary>
    /// The id of the built-in Brotli strategy, <c>"br"</c>.
    /// </summary>
    public const string Brotli = "br";

    /// <summary>
    /// The id of the built-in Deflate strategy, <c>"deflate"</c>.
    /// </summary>
    /// <remarks>
    /// Raw DEFLATE as produced by <see cref="System.IO.Compression.DeflateStream"/>, with no zlib
    /// wrapper — matching what the HTTP content-coding of this name means in practice rather than what
    /// RFC 9110 nominally says it is.
    /// </remarks>
    public const string Deflate = "deflate";
}
