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
/// rejected at encode time.
/// </remarks>
internal static class HeaderEnvelope
{
    private const int MaxKeyByteLength = byte.MaxValue;
    private const int MaxValueByteLength = ushort.MaxValue;

    /// <summary>
    /// Computes the number of bytes <see cref="Write"/> would write for the given headers, excluding
    /// the block-length prefix that precedes it on the wire.
    /// </summary>
    public static int GetEncodedLength(MessageHeaders headers)
    {
        int length = 0;
        foreach (KeyValuePair<string, string> header in headers)
        {
            length += 1 + Encoding.UTF8.GetByteCount(header.Key) + 2 + Encoding.UTF8.GetByteCount(header.Value);
        }

        return length;
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
            byte[] keyBytes = Encoding.UTF8.GetBytes(header.Key);
            if (keyBytes.Length > MaxKeyByteLength)
            {
                throw new ArgumentException(
                    $"Header key '{header.Key}' is {keyBytes.Length} bytes once UTF-8 encoded, "
                    + $"exceeding the maximum of {MaxKeyByteLength}.",
                    nameof(headers));
            }

            byte[] valueBytes = Encoding.UTF8.GetBytes(header.Value);
            if (valueBytes.Length > MaxValueByteLength)
            {
                throw new ArgumentException(
                    $"The value for header key '{header.Key}' is {valueBytes.Length} bytes once UTF-8 "
                    + $"encoded, exceeding the maximum of {MaxValueByteLength}.",
                    nameof(headers));
            }

            destination[offset] = (byte)keyBytes.Length;
            offset += 1;

            keyBytes.CopyTo(destination[offset..]);
            offset += keyBytes.Length;

            BinaryPrimitives.WriteUInt16BigEndian(destination.Slice(offset, 2), (ushort)valueBytes.Length);
            offset += 2;

            valueBytes.CopyTo(destination[offset..]);
            offset += valueBytes.Length;
        }
    }

    /// <summary>
    /// Decodes a header block of exactly <paramref name="blockLength"/> bytes from the start of
    /// <paramref name="source"/>.
    /// </summary>
    /// <returns><see cref="MessageHeaders.Empty"/> when <paramref name="blockLength"/> is zero.</returns>
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
            int keyLength = block[offset];
            offset += 1;

            string key = Encoding.UTF8.GetString(block.Slice(offset, keyLength));
            offset += keyLength;

            int valueLength = BinaryPrimitives.ReadUInt16BigEndian(block.Slice(offset, 2));
            offset += 2;

            string value = Encoding.UTF8.GetString(block.Slice(offset, valueLength));
            offset += valueLength;

            values[key] = value;
        }

        return MessageHeaders.FromOwnedDictionary(values);
    }
}
