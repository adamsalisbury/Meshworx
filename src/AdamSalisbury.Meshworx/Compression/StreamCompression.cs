using System.Buffers;
using System.Runtime.InteropServices;
using System.IO.Compression;

namespace AdamSalisbury.Meshworx.Compression;

/// <summary>
/// The shared one-shot compress/decompress plumbing behind the built-in
/// <see cref="ICompressionStrategy"/> implementations.
/// </summary>
/// <remarks>
/// Both built-ins are a <see cref="Stream"/> decorator over the same buffering, bounding and error
/// mapping, so that part lives here once and each strategy supplies only its own decorator. A consumer
/// writing a strategy over another <see cref="Stream"/>-shaped codec has a shorter road if it copies this
/// than if each built-in had grown its own subtly different version.
/// </remarks>
internal static class StreamCompression
{
    private const int CopyBufferSize = 8 * 1024;

    internal static ReadOnlyMemory<byte> Compress(
        ReadOnlyMemory<byte> payload,
        CompressionLevel level,
        Func<Stream, CompressionLevel, Stream> compressorFactory)
    {
        using var destination = new MemoryStream();

        using (Stream compressor = compressorFactory(destination, level))
        {
            compressor.Write(payload.Span);
        }

        return Detach(destination);
    }

    internal static ReadOnlyMemory<byte> Decompress(
        ReadOnlyMemory<byte> payload,
        int maxDecompressedBytes,
        Func<Stream, Stream> decompressorFactory,
        string algorithmId)
    {
        if (maxDecompressedBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDecompressedBytes), "The maximum decompressed bytes must be positive.");
        }

        using MemoryStream source = AsStream(payload);
        using Stream decompressor = decompressorFactory(source);
        using var destination = new MemoryStream();

        byte[] buffer = ArrayPool<byte>.Shared.Rent(CopyBufferSize);

        try
        {
            int total = 0;

            while (true)
            {
                int read;

                try
                {
                    read = decompressor.Read(buffer, 0, buffer.Length);
                }
                catch (Exception ex) when (ex is InvalidOperationException or InvalidDataException or IOException)
                {
                    // The framework is not consistent about how a corrupt or truncated body surfaces:
                    // BrotliStream throws InvalidOperationException, DeflateStream InvalidDataException,
                    // and a truncation can arrive as an IOException. A caller cannot act differently on
                    // the three, and "these bytes were not valid output of this algorithm" is what
                    // happened in every case, so they are normalised to the one the contract names.
                    throw new InvalidDataException(
                        $"The body is not valid '{algorithmId}' compressed data.", ex);
                }

                if (read <= 0)
                {
                    break;
                }

                total += read;

                if (total > maxDecompressedBytes)
                {
                    // Thrown outside the catch above so it is never reported as corrupt data: the body
                    // decompressed perfectly well, there was simply more of it than the caller allowed.
                    throw new InvalidDataException(
                        $"Decompressing the '{algorithmId}' body exceeded the {maxDecompressedBytes}-byte limit.");
                }

                destination.Write(buffer, 0, read);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        return Detach(destination);
    }

    /// <summary>
    /// Reads a payload without copying it where it is already array-backed, which every body arriving from
    /// the transport is.
    /// </summary>
    private static MemoryStream AsStream(ReadOnlyMemory<byte> payload)
    {
        return MemoryMarshal.TryGetArray(payload, out ArraySegment<byte> segment) && segment.Array is not null
            ? new MemoryStream(segment.Array, segment.Offset, segment.Count, writable: false)
            : new MemoryStream(payload.ToArray(), writable: false);
    }

    /// <summary>
    /// Takes the stream's own buffer rather than copying it out. Safe because the stream is a local that
    /// is about to be disposed, so nothing else can write through the buffer afterwards, and the returned
    /// view is bounded by what was written rather than by the buffer's capacity.
    /// </summary>
    private static ReadOnlyMemory<byte> Detach(MemoryStream stream)
    {
        return stream.TryGetBuffer(out ArraySegment<byte> buffer) ? buffer.AsMemory() : stream.ToArray();
    }
}
