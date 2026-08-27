using System.Text;

namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Encodes and decodes a list of compression algorithm ids as it travels in
/// <see cref="MessageType.AdvertiseCompression"/> and
/// <see cref="MessageType.CompressionCapabilityResponse"/> frames.
/// </summary>
/// <remarks>
/// <para>
/// A count byte followed by that many length-prefixed UTF-8 ids: <c>[count u8][len u8][utf8 id]...</c>.
/// Both prefixes are single bytes because both are already bounded far below 256 —
/// <see cref="Protocol.MaxAdvertisedCompressionAlgorithms"/> ids of at most
/// <see cref="Protocol.MaxCompressionAlgorithmIdLength"/> characters each — so a wider prefix would encode
/// nothing a narrower one cannot.
/// </para>
/// <para>
/// Deliberately not the <see cref="HeaderEnvelope"/> codec that client attributes reuse. That one encodes
/// a key/value map; this is an ordered list, and the order is the payload — it is the advertising
/// endpoint's preference order, which a map would not preserve.
/// </para>
/// </remarks>
internal static class CompressionCapabilityEnvelope
{
    /// <summary>
    /// The encoded length, in bytes, of <paramref name="algorithmIds"/> including the leading count byte.
    /// </summary>
    internal static int GetEncodedLength(IReadOnlyList<string> algorithmIds)
    {
        int length = 1;

        for (int i = 0; i < algorithmIds.Count; i++)
        {
            length += 1 + Encoding.UTF8.GetByteCount(algorithmIds[i]);
        }

        return length;
    }

    /// <summary>
    /// Writes <paramref name="algorithmIds"/> into <paramref name="destination"/>, which must be at least
    /// <see cref="GetEncodedLength"/> bytes.
    /// </summary>
    internal static void Write(IReadOnlyList<string> algorithmIds, Span<byte> destination)
    {
        destination[0] = (byte)algorithmIds.Count;
        int offset = 1;

        for (int i = 0; i < algorithmIds.Count; i++)
        {
            int written = Encoding.UTF8.GetBytes(algorithmIds[i], destination[(offset + 1)..]);
            destination[offset] = (byte)written;
            offset += 1 + written;
        }
    }

    /// <summary>
    /// Reads a list of algorithm ids.
    /// </summary>
    /// <param name="source">The encoded block, starting at its count byte.</param>
    /// <param name="algorithmIds">The decoded ids, when this returns true.</param>
    /// <returns>
    /// <see langword="true"/> when the block is well formed and within bounds; otherwise
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Every value here comes from a peer, so each way it can be wrong is rejected rather than trusted: a
    /// truncated block, a declared count past
    /// <see cref="Protocol.MaxAdvertisedCompressionAlgorithms"/>, an id longer than
    /// <see cref="Protocol.MaxCompressionAlgorithmIdLength"/>, an empty id, or trailing bytes after the
    /// last one. The whole block is rejected rather than partially accepted — a half-read advertisement
    /// would leave a peer believed to support a set neither side ever agreed on, which is worse than
    /// believing it supports nothing.
    /// </remarks>
    internal static bool TryRead(ReadOnlySpan<byte> source, out IReadOnlyList<string> algorithmIds)
    {
        algorithmIds = [];

        if (source.Length < 1)
        {
            return false;
        }

        int count = source[0];

        if (count > Protocol.MaxAdvertisedCompressionAlgorithms)
        {
            return false;
        }

        var ids = new List<string>(count);
        int offset = 1;

        for (int i = 0; i < count; i++)
        {
            if (offset >= source.Length)
            {
                return false;
            }

            int length = source[offset];
            offset++;

            if (length == 0
                || length > Protocol.MaxCompressionAlgorithmIdLength
                || offset + length > source.Length)
            {
                return false;
            }

            ids.Add(Encoding.UTF8.GetString(source.Slice(offset, length)));
            offset += length;
        }

        if (offset != source.Length)
        {
            return false;
        }

        algorithmIds = ids;

        return true;
    }
}
