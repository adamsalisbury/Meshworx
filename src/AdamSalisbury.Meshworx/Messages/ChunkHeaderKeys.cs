using System.Globalization;

namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The well-known <see cref="MessageHeaders"/> keys that carry a chunked message's reassembly
/// metadata.
/// </summary>
/// <remarks>
/// Chunking is an endpoint concern end to end. The hub routes each chunk as an ordinary opaque frame,
/// never reassembles one, and never reads these keys — they travel in the header block it already
/// passes through unchanged. A hub cannot tell a chunk from any other message, which is the point:
/// nothing about a 40 MiB transfer changes what the hub has to hold.
/// <para>
/// The chunk count is sent rather than a last-chunk flag. The sender always has the whole payload
/// before it starts, so the count costs nothing to know, and it lets a receiver size its reassembly
/// buffer once, reject an out-of-range index immediately, and recognise completion without waiting for
/// a terminator that a dropped connection might never deliver.
/// </para>
/// </remarks>
internal static class ChunkHeaderKeys
{
    /// <summary>
    /// Identifies the logical message a chunk belongs to, as a GUID in "D" format. Unique per sender.
    /// </summary>
    internal const string Id = "mesh.chunk.id";

    /// <summary>
    /// The chunk's zero-based position within its logical message.
    /// </summary>
    internal const string Index = "mesh.chunk.index";

    /// <summary>
    /// How many chunks the logical message was split into.
    /// </summary>
    internal const string Count = "mesh.chunk.count";

    /// <summary>
    /// Reads and validates the three chunk headers as a set.
    /// </summary>
    /// <param name="headers">The received message's headers.</param>
    /// <param name="id">The logical message id, when this returns true.</param>
    /// <param name="index">The chunk's zero-based index, when this returns true.</param>
    /// <param name="count">The logical message's total chunk count, when this returns true.</param>
    /// <returns>
    /// <see langword="true"/> when the headers describe a well-formed chunk; otherwise
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// All three must be present and mutually consistent, or the message is not treated as a chunk at
    /// all and is delivered as the ordinary message it otherwise appears to be. The values come from a
    /// remote peer and are not validated by the hub, so every failure mode is handled here rather than
    /// left to throw somewhere downstream: a missing key, a non-numeric value, a negative or zero
    /// count, an index outside the count, or a count beyond what any single transfer could legitimately
    /// need.
    /// </remarks>
    internal static bool TryReadChunkHeaders(
        MessageHeaders headers, out Guid id, out int index, out int count)
    {
        id = Guid.Empty;
        index = 0;
        count = 0;

        if (!headers.TryGetValue(Id, out string? rawId)
            || !headers.TryGetValue(Index, out string? rawIndex)
            || !headers.TryGetValue(Count, out string? rawCount))
        {
            return false;
        }

        if (!Guid.TryParseExact(rawId, "D", out id)
            || !int.TryParse(rawIndex, NumberStyles.Integer, CultureInfo.InvariantCulture, out index)
            || !int.TryParse(rawCount, NumberStyles.Integer, CultureInfo.InvariantCulture, out count))
        {
            return false;
        }

        return count > 0 && count <= MaxChunksPerMessage && index >= 0 && index < count;
    }

    /// <summary>
    /// Rebuilds <paramref name="headers"/> with the three chunk keys removed, once a transfer has
    /// completed and been reassembled into a single message.
    /// </summary>
    /// <remarks>
    /// The chunk keys are internal reassembly bookkeeping, present on every individual chunk but
    /// meaningless once reassembly is done — <see cref="IMeshClient.SendLargeAsync(Guid, ReadOnlyMemory{byte}, MessageHeaders, CancellationToken)"/>'s contract is that
    /// a subscriber sees the headers it was sent, needing no code of its own to distinguish a chunked
    /// message from an ordinary one. Left in, they would also break the common pattern of echoing
    /// received headers back onto a reply: the far side's receive loop would read them as real chunk
    /// metadata via <see cref="TryReadChunkHeaders"/> and silently absorb the reply into its own
    /// reassembler instead of raising it.
    /// </remarks>
    /// <param name="headers">The reassembled message's headers, still carrying the three chunk keys.</param>
    /// <returns>A new <see cref="MessageHeaders"/> instance with the three chunk keys removed.</returns>
    internal static MessageHeaders WithoutChunkHeaders(MessageHeaders headers)
    {
        return new MessageHeaders(headers.Where(
            entry => entry.Key is not (Id or Index or Count)));
    }

    /// <summary>
    /// The most chunks a single logical message may claim to be split into.
    /// </summary>
    /// <remarks>
    /// A ceiling on what a peer can assert before any of it is believed. The count arrives before the
    /// chunks do and is what a receiver sizes its bookkeeping from, so an unbounded one would let a
    /// single small frame ask the receiver to allocate an arbitrarily large array. At the chunk size
    /// this library sends, 4096 chunks is roughly 4 GiB — far past any payload that belongs in a
    /// message rather than a file transfer, while leaving the per-transfer byte bound, not this, as the
    /// limit a legitimate caller ever meets.
    /// </remarks>
    internal const int MaxChunksPerMessage = 4096;
}
