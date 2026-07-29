using System.Buffers;
using System.Buffers.Binary;

namespace AdamSalisbury.Meshworx.Transport.Framing;

/// <summary>
/// Shared length-prefixed message framing for transports backed by an arbitrary duplex
/// <see cref="Stream"/> — TCP, Unix domain sockets, and named pipes all frame identically once a
/// connection is established, so this is the single place that framing logic lives rather than each
/// transport reimplementing it.
/// </summary>
/// <remarks>
/// Each message is transmitted as a 4-byte big-endian length header followed by the payload bytes,
/// capped at <see cref="MaxPayloadSize"/>. Callers own the write lock and the reused header buffer —
/// this class is stateless — so each transport controls its own concurrency and allocation lifetime
/// exactly as before this helper existed.
/// </remarks>
internal static class StreamFramer
{
    /// <summary>
    /// The size, in bytes, of the length prefix that precedes every frame's payload.
    /// </summary>
    internal const int HeaderSize = 4;

    /// <summary>
    /// The maximum payload size, in bytes, a single frame may carry.
    /// </summary>
    internal const int MaxPayloadSize = 1024 * 1024;

    /// <summary>
    /// Writes a single length-prefixed frame to the stream, synchronised on <paramref name="writeLock"/>.
    /// </summary>
    /// <param name="stream">The stream to write the frame to.</param>
    /// <param name="writeLock">A semaphore, owned by the caller, that serialises writes to the stream.</param>
    /// <param name="data">The message payload to send.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="ArgumentException"><paramref name="data"/> exceeds <see cref="MaxPayloadSize"/>.</exception>
    internal static async Task SendAsync(
        Stream stream,
        SemaphoreSlim writeLock,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken)
    {
        // Reject oversized payloads up front. The receiving peer enforces the same limit and would
        // otherwise treat the frame as corrupt and drop the connection — a clear ArgumentException to
        // the caller is far better than a surprise disconnect. This also guards the frameSize addition
        // below against integer overflow.
        if (data.Length > MaxPayloadSize)
        {
            throw new ArgumentException(
                $"Payload size {data.Length} exceeds the maximum frame payload of {MaxPayloadSize} bytes.",
                nameof(data));
        }

        int frameSize = HeaderSize + data.Length;
        byte[] frame = ArrayPool<byte>.Shared.Rent(frameSize);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(frame, data.Length);
            data.Span.CopyTo(frame.AsSpan(HeaderSize));

            await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await stream.WriteAsync(frame.AsMemory(0, frameSize), cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                writeLock.Release();
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(frame);
        }
    }

    /// <summary>
    /// Writes several length-prefixed frames to the stream as a single write, synchronised on
    /// <paramref name="writeLock"/>.
    /// </summary>
    /// <remarks>
    /// Frames only the valid prefix up to the first oversize payload (if any). Writing that prefix
    /// before throwing preserves <see cref="SendAsync"/>'s deliver-then-fault behaviour: frames
    /// coalesced ahead of an oversize one are still delivered, rather than the whole batch being
    /// discarded because a later frame is invalid.
    /// </remarks>
    /// <param name="stream">The stream to write the frames to.</param>
    /// <param name="writeLock">A semaphore, owned by the caller, that serialises writes to the stream.</param>
    /// <param name="messages">The message payloads to send, in delivery order.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    internal static async Task SendBatchAsync(
        Stream stream,
        SemaphoreSlim writeLock,
        IReadOnlyList<ReadOnlyMemory<byte>> messages,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);

        if (messages.Count == 0)
        {
            return;
        }

        if (messages.Count == 1)
        {
            await SendAsync(stream, writeLock, messages[0], cancellationToken).ConfigureAwait(false);
            return;
        }

        long frameSize = 0;
        int validCount = messages.Count;
        for (int i = 0; i < messages.Count; i++)
        {
            if (messages[i].Length > MaxPayloadSize)
            {
                validCount = i;
                break;
            }

            frameSize += HeaderSize + messages[i].Length;
        }

        if (frameSize > 0)
        {
            // The send loop bounds a batch's total size, so frameSize is well within int range here; the
            // cast is safe. Renting a single buffer keeps the prefix to one write and one flush.
            byte[] frame = ArrayPool<byte>.Shared.Rent((int)frameSize);
            try
            {
                int offset = 0;
                for (int i = 0; i < validCount; i++)
                {
                    ReadOnlyMemory<byte> message = messages[i];
                    BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(offset), message.Length);
                    offset += HeaderSize;
                    message.Span.CopyTo(frame.AsSpan(offset));
                    offset += message.Length;
                }

                await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    await stream.WriteAsync(frame.AsMemory(0, offset), cancellationToken).ConfigureAwait(false);
                    await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    writeLock.Release();
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(frame);
            }
        }

        if (validCount < messages.Count)
        {
            int length = messages[validCount].Length;
            throw new ArgumentException(
                $"Payload size {length} exceeds the maximum frame payload of {MaxPayloadSize} bytes.",
                nameof(messages));
        }
    }

    /// <summary>
    /// Reads the next length-prefixed frame from the stream.
    /// </summary>
    /// <param name="stream">The stream to read the frame from.</param>
    /// <param name="headerBuffer">
    /// A reused 4-byte buffer, owned by the caller, to hold each frame's length prefix. The caller is
    /// responsible for ensuring this is not read concurrently with another call for the same stream.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The received message payload, or <see langword="null"/> if the connection has been closed.</returns>
    /// <exception cref="IOException">The frame's declared length is negative or exceeds <see cref="MaxPayloadSize"/>.</exception>
    internal static async Task<byte[]?> ReceiveAsync(
        Stream stream,
        byte[] headerBuffer,
        CancellationToken cancellationToken)
    {
        if (!await ReadExactlyAsync(stream, headerBuffer, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        int payloadLength = BinaryPrimitives.ReadInt32BigEndian(headerBuffer);

        if (payloadLength is < 0 or > MaxPayloadSize)
        {
            // A corrupt or out-of-range length prefix means the stream framing is no longer
            // trustworthy. Surface it as an I/O error so receive loops treat it as a transport failure
            // and terminate the connection cleanly, rather than faulting on an unhandled exception type.
            throw new IOException($"Invalid payload length: {payloadLength}");
        }

        if (payloadLength == 0)
        {
            return [];
        }

        var payload = new byte[payloadLength];
        if (!await ReadExactlyAsync(stream, payload, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return payload;
    }

    private static async Task<bool> ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        try
        {
            await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (EndOfStreamException)
        {
            // The peer closed the connection (cleanly, or mid-frame); signal end of stream.
            return false;
        }
    }
}
