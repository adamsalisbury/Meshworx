namespace AdamSalisbury.Meshworx.Compression;

/// <summary>
/// Compresses and decompresses a message body under a single named algorithm.
/// </summary>
/// <remarks>
/// <para>
/// Compression is an agreement between the two endpoints and nothing else — the hub routes a compressed
/// body exactly as it routes any other, never inspecting it and never needing to hold a strategy of its
/// own. That is the same arrangement the codec layer already has, over in
/// <c>AdamSalisbury.Meshworx.Serialization</c>: what the bytes mean is the endpoints' business.
/// </para>
/// <para>
/// Implementations must be thread-safe and stateless between calls. A single instance is shared by every
/// send and receive that resolves to its <see cref="AlgorithmId"/>, concurrently and without
/// synchronisation by the caller.
/// </para>
/// </remarks>
public interface ICompressionStrategy
{
    /// <summary>
    /// Gets the identifier this strategy is registered and resolved under, and which travels on the wire
    /// so the receiving endpoint can find the matching strategy in its own registry.
    /// </summary>
    /// <remarks>
    /// Must be a short token of ASCII letters, digits or <c>-</c> <c>+</c> <c>.</c> <c>_</c> — the same
    /// shape as an HTTP content-coding name, and for the same reason: it has to survive a round-trip
    /// through a message header unambiguously. The built-ins use the content-coding names themselves
    /// (see <see cref="CompressionAlgorithms"/>). Resolution is case-insensitive, so <c>br</c> and
    /// <c>BR</c> are the same algorithm and cannot both be registered.
    /// </remarks>
    string AlgorithmId { get; }

    /// <summary>
    /// Compresses a message body.
    /// </summary>
    /// <param name="payload">The uncompressed body.</param>
    /// <returns>
    /// The compressed body, which <see cref="Decompress"/> on the receiving endpoint turns back into
    /// <paramref name="payload"/> byte for byte.
    /// </returns>
    /// <remarks>
    /// May legitimately return more bytes than it was given — incompressible input plus a container's
    /// framing overhead. Deciding whether the result is worth sending is the caller's business, not the
    /// strategy's, so an implementation should compress what it is handed rather than second-guessing it.
    /// </remarks>
    ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte> payload);

    /// <summary>
    /// Decompresses a message body produced by <see cref="Compress"/>.
    /// </summary>
    /// <param name="payload">The compressed body, as received.</param>
    /// <param name="maxDecompressedBytes">
    /// The largest output the caller is willing to hold. An implementation must stop and throw once the
    /// output exceeds this, rather than decompressing to completion and letting the caller discover the
    /// size afterwards.
    /// </param>
    /// <returns>The original uncompressed body.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxDecompressedBytes"/> is not positive.</exception>
    /// <exception cref="InvalidDataException">
    /// <paramref name="payload"/> is not valid output of this algorithm, or decompressing it exceeds
    /// <paramref name="maxDecompressedBytes"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The bound is part of the signature rather than a property of the implementation because a
    /// compressed body arriving from a peer is attacker-controlled: a few kilobytes on the wire can
    /// expand to gigabytes in memory. Bounding it is not optional, so a strategy is not given the chance
    /// to leave it out.
    /// </para>
    /// <para>
    /// A <i>truncated</i> body is not detected here and cannot be: both built-ins read one as a stream
    /// that simply ended, returning a prefix of the original rather than throwing, and a strategy is
    /// handed bytes with no idea how many it was supposed to produce. Detecting that needs the
    /// uncompressed length, which is known one level up and belongs with the message rather than in this
    /// contract. In practice the transport's own length-prefixed framing already rules truncation out
    /// before a body reaches a strategy at all.
    /// </para>
    /// </remarks>
    ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> payload, int maxDecompressedBytes);
}
