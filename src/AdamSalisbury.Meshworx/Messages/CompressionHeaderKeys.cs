using System.Globalization;
using AdamSalisbury.Meshworx.Compression;

namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The well-known <see cref="MessageHeaders"/> keys that mark a body as compressed and say what it takes
/// to read it back.
/// </summary>
/// <remarks>
/// <para>
/// Compression is an endpoint concern end to end, exactly as chunking is. The hub routes a compressed
/// body as an ordinary opaque frame and never reads these keys — they travel in the header block it
/// already passes through unchanged. A hub cannot tell a compressed message from any other, which is the
/// point.
/// </para>
/// <para>
/// The uncompressed length is sent alongside the algorithm id rather than left to be discovered. The
/// sender always has the whole payload before it starts, so it costs nothing to know, and it earns three
/// things at the far end: decompression is bounded at exactly the right size rather than at a blanket
/// ceiling, a restored body of the wrong length is caught instead of delivered, and — the reason it
/// exists — a <i>truncated</i> body stops being indistinguishable from a complete one. Both built-in
/// decompressors read a truncated body as a stream that simply ended and return a prefix of the
/// original; without a declared length there is nothing to compare that prefix against.
/// </para>
/// </remarks>
internal static class CompressionHeaderKeys
{
    /// <summary>
    /// The id of the <see cref="ICompressionStrategy"/> the body was compressed with, resolved against
    /// the receiving endpoint's own registry.
    /// </summary>
    internal const string Algorithm = "mesh.compression";

    /// <summary>
    /// The body's length in bytes before compression, as an invariant-culture integer.
    /// </summary>
    internal const string UncompressedLength = "mesh.compression.length";

    /// <summary>
    /// Reads and validates the two compression headers as a set.
    /// </summary>
    /// <param name="headers">The received message's headers.</param>
    /// <param name="algorithmId">The algorithm the body was compressed with, when this returns true.</param>
    /// <param name="uncompressedLength">The body's length before compression, when this returns true.</param>
    /// <returns>
    /// <see langword="true"/> when the headers describe a well-formed compressed body; otherwise
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Both must be present and well-formed, or the message is not treated as compressed at all and is
    /// delivered as the ordinary message it otherwise appears to be — the same posture
    /// <see cref="ChunkHeaderKeys.TryReadChunkHeaders"/> takes. The values come from a remote peer and
    /// are not validated by the hub, so every failure mode is handled here rather than left to throw
    /// downstream: a missing key, an empty algorithm id, an id longer than one could legitimately be, a
    /// non-numeric length, or one that is not positive — this library never compresses an empty body, so
    /// a declared length of zero is malformed rather than a degenerate case worth supporting.
    /// </remarks>
    internal static bool TryReadCompressionHeaders(
        MessageHeaders headers, out string algorithmId, out int uncompressedLength)
    {
        algorithmId = string.Empty;
        uncompressedLength = 0;

        if (!headers.TryGetValue(Algorithm, out string? rawAlgorithm)
            || !headers.TryGetValue(UncompressedLength, out string? rawLength))
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(rawAlgorithm)
            || rawAlgorithm.Length > Protocol.MaxCompressionAlgorithmIdLength)
        {
            return false;
        }

        if (!int.TryParse(rawLength, NumberStyles.Integer, CultureInfo.InvariantCulture, out uncompressedLength)
            || uncompressedLength <= 0)
        {
            return false;
        }

        algorithmId = rawAlgorithm;

        return true;
    }

    /// <summary>
    /// Rebuilds <paramref name="headers"/> with the two compression keys removed, once the body has been
    /// decompressed.
    /// </summary>
    /// <remarks>
    /// Compression is meant to be invisible: a subscriber sees exactly the headers the sender sent, and
    /// needs no code of its own to tell a compressed message from an ordinary one. Left in, they would
    /// also break the common pattern of echoing received headers back onto a reply — the far side would
    /// read them as real compression metadata and try to decompress a body that was never compressed.
    /// </remarks>
    /// <param name="headers">The decompressed message's headers, still carrying the two compression keys.</param>
    /// <returns>A new <see cref="MessageHeaders"/> instance with the two compression keys removed.</returns>
    internal static MessageHeaders WithoutCompressionHeaders(MessageHeaders headers)
    {
        return new MessageHeaders(headers.Where(
            entry => entry.Key is not (Algorithm or UncompressedLength)));
    }
}
