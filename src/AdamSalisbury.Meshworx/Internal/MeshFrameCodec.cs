using System.Buffers.Binary;

namespace AdamSalisbury.Meshworx.Internal;

internal static class MeshFrameCodec
{
    private const int HeaderSize = 5;
    private const int MaxPayloadSize = 1024 * 1024;

    public static async Task WriteFrameAsync(
        Stream stream,
        MessageType type,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        var header = new byte[HeaderSize];
        header[0] = (byte)type;
        BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(1), payload.Length);

        await stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);

        if (payload.Length > 0)
        {
            await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        }

        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public static async Task<(MessageType Type, byte[] Payload)?> ReadFrameAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        var header = await ReadBytesAsync(stream, HeaderSize, cancellationToken).ConfigureAwait(false);
        if (header is null)
        {
            return null;
        }

        var type = (MessageType)header[0];
        int payloadLength = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1));

        if (payloadLength is < 0 or > MaxPayloadSize)
        {
            throw new InvalidOperationException($"Invalid payload length: {payloadLength}");
        }

        if (payloadLength == 0)
        {
            return (type, []);
        }

        var payload = await ReadBytesAsync(stream, payloadLength, cancellationToken).ConfigureAwait(false);
        if (payload is null)
        {
            return null;
        }

        return (type, payload);
    }

    private static async Task<byte[]?> ReadBytesAsync(
        Stream stream,
        int count,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[count];
        try
        {
            await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
            return buffer;
        }
        catch (EndOfStreamException)
        {
            return null;
        }
    }
}
