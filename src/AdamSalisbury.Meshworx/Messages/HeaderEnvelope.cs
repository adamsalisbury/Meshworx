using System.Buffers.Binary;
using System.Text;

namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Encodes and decodes the header block carried by the header-bearing message frames
/// (<see cref="MessageType.SendMessageWithHeaders"/>, <see cref="MessageType.DeliverMessageWithHeaders"/>,
/// <see cref="MessageType.GroupMessageWithHeaders"/> and
/// <see cref="MessageType.DeliverGroupMessageWithHeaders"/>).
/// </summary>
/// <remarks>
/// The block is a flat, back-to-back run of entries — <c>[keyLength(1)][key][valueLength(2,
/// big-endian)][value]</c> — with no entry count: a reader consumes entries until it has read exactly
/// as many bytes as the block's own length prefix declared. Keys and values are UTF-8. A key longer
/// than 255 bytes once encoded, or a value longer than 65535 bytes, cannot be represented and is
/// rejected at encode time — as is a block whose overall encoded length would not fit the wire
/// format's own 2-byte block-length prefix.
/// </remarks>
internal static class HeaderEnvelope
{
    private const int MaxKeyByteLength = byte.MaxValue;
    private const int MaxValueByteLength = ushort.MaxValue;

    /// <summary>
    /// Computes the number of bytes <see cref="Write"/> would write for the given headers, excluding
    /// the block-length prefix that precedes it on the wire.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// The headers' total encoded length exceeds <see cref="ushort.MaxValue"/>, the largest value the
    /// wire format's block-length prefix can represent.
    /// </exception>
    public static int GetEncodedLength(MessageHeaders headers)
    {
        // Summed as a long so an implausibly large header set cannot wrap the running total past
        // int.MaxValue before the check below has a chance to reject it.
        long length = 0;
        foreach (KeyValuePair<string, string> header in headers)
        {
            length += 1 + Encoding.UTF8.GetByteCount(header.Key) + 2 + Encoding.UTF8.GetByteCount(header.Value);
        }

        if (length > ushort.MaxValue)
        {
            throw new ArgumentException(
                $"The headers encode to {length} bytes, exceeding the maximum header-block length of "
                + $"{ushort.MaxValue} bytes representable by the wire format's block-length prefix.",
                nameof(headers));
        }

        return (int)length;
    }

    /// <summary>
    /// Writes the header block content into <paramref name="destination"/>, which must be exactly
    /// <see cref="GetEncodedLength"/> bytes long.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// A header key or value is too long to encode within the block's length prefixes.
    /// </exception>
    public static void Write(MessageHeaders headers, Span<byte> destination)
    {
        int offset = 0;
        foreach (KeyValuePair<string, string> header in headers)
        {
            int keyByteLength = Encoding.UTF8.GetByteCount(header.Key);
            if (keyByteLength > MaxKeyByteLength)
            {
                throw new ArgumentException(
                    $"Header key '{header.Key}' is {keyByteLength} bytes once UTF-8 encoded, "
                    + $"exceeding the maximum of {MaxKeyByteLength}.",
                    nameof(headers));
            }

            int valueByteLength = Encoding.UTF8.GetByteCount(header.Value);
            if (valueByteLength > MaxValueByteLength)
            {
                throw new ArgumentException(
                    $"The value for header key '{header.Key}' is {valueByteLength} bytes once UTF-8 "
                    + $"encoded, exceeding the maximum of {MaxValueByteLength}.",
                    nameof(headers));
            }

            // Encoded directly into the destination span rather than via an intermediate byte[] per
            // entry — GetByteCount above already gives the length needed for the prefixes and bounds
            // checks, so nothing needs the encoded bytes before they can be written straight to their
            // final position.
            destination[offset] = (byte)keyByteLength;
            offset += 1;

            offset += Encoding.UTF8.GetBytes(header.Key, destination[offset..]);

            BinaryPrimitives.WriteUInt16BigEndian(destination.Slice(offset, 2), (ushort)valueByteLength);
            offset += 2;

            offset += Encoding.UTF8.GetBytes(header.Value, destination[offset..]);
        }
    }

    /// <summary>
    /// Decodes a header block of exactly <paramref name="blockLength"/> bytes from the start of
    /// <paramref name="source"/>.
    /// </summary>
    /// <returns><see cref="MessageHeaders.Empty"/> when <paramref name="blockLength"/> is zero.</returns>
    /// <exception cref="FormatException">
    /// The block is internally malformed — a key or value length runs past the end of the block, or
    /// the final entry does not end exactly on the block's own boundary. The <paramref name="source"/>
    /// span itself is trusted to be at least <paramref name="blockLength"/> bytes long (the caller has
    /// already validated that against the outer frame); everything past that point is untrusted
    /// sender-supplied data and is bounds-checked here before use.
    /// </exception>
    public static MessageHeaders Read(ReadOnlySpan<byte> source, int blockLength)
    {
        if (blockLength == 0)
        {
            return MessageHeaders.Empty;
        }

        ReadOnlySpan<byte> block = source[..blockLength];
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        int offset = 0;

        while (offset < block.Length)
        {
            // offset < block.Length is already guaranteed by the loop condition, so the key-length
            // byte itself is always safe to read; only the fields after it can run past the block.
            int keyLength = block[offset];
            offset += 1;

            if (offset + keyLength > block.Length)
            {
                throw new FormatException(
                    "Malformed header block: a header key runs past the declared block length.");
            }

            string key = Encoding.UTF8.GetString(block.Slice(offset, keyLength));
            offset += keyLength;

            if (offset + 2 > block.Length)
            {
                throw new FormatException(
                    "Malformed header block: truncated before a header's value-length field.");
            }

            int valueLength = BinaryPrimitives.ReadUInt16BigEndian(block.Slice(offset, 2));
            offset += 2;

            if (offset + valueLength > block.Length)
            {
                throw new FormatException(
                    "Malformed header block: a header value runs past the declared block length.");
            }

            string value = Encoding.UTF8.GetString(block.Slice(offset, valueLength));
            offset += valueLength;

            values[key] = value;
        }

        return MessageHeaders.FromOwnedDictionary(values);
    }

    /// <summary>
    /// Scans a header block of exactly <paramref name="blockLength"/> bytes for a single well-known
    /// key, without allocating the <see cref="Dictionary{TKey, TValue}"/> that decoding every entry via
    /// <see cref="Read"/> would. Intended for a hot path that only ever needs to test for one specific
    /// header — currently the hub checking a queued message's expiry — where paying for the full
    /// decode on every frame, including the vast majority that do not carry the key being searched for,
    /// would be wasteful.
    /// </summary>
    /// <returns><see langword="true"/> if <paramref name="key"/> was found within the block.</returns>
    /// <exception cref="FormatException">
    /// The block is internally malformed, exactly as documented on <see cref="Read"/>.
    /// </exception>
    public static bool TryReadValue(ReadOnlySpan<byte> source, int blockLength, string key, out string? value)
    {
        value = null;

        if (blockLength == 0)
        {
            return false;
        }

        ReadOnlySpan<byte> block = source[..blockLength];

        // Encoded once up front so each entry's key can be compared as raw bytes, rather than UTF-8
        // decoding every key in the block just to immediately discard the ones that do not match.
        Span<byte> keyBuffer = stackalloc byte[MaxKeyByteLength];
        int searchKeyLength = Encoding.UTF8.GetBytes(key, keyBuffer);
        ReadOnlySpan<byte> searchKeyBytes = keyBuffer[..searchKeyLength];

        int offset = 0;

        while (offset < block.Length)
        {
            int entryKeyLength = block[offset];
            offset += 1;

            if (offset + entryKeyLength > block.Length)
            {
                throw new FormatException(
                    "Malformed header block: a header key runs past the declared block length.");
            }

            ReadOnlySpan<byte> entryKeyBytes = block.Slice(offset, entryKeyLength);
            offset += entryKeyLength;

            if (offset + 2 > block.Length)
            {
                throw new FormatException(
                    "Malformed header block: truncated before a header's value-length field.");
            }

            int valueLength = BinaryPrimitives.ReadUInt16BigEndian(block.Slice(offset, 2));
            offset += 2;

            if (offset + valueLength > block.Length)
            {
                throw new FormatException(
                    "Malformed header block: a header value runs past the declared block length.");
            }

            if (entryKeyBytes.SequenceEqual(searchKeyBytes))
            {
                value = Encoding.UTF8.GetString(block.Slice(offset, valueLength));
                return true;
            }

            offset += valueLength;
        }

        return false;
    }
}
