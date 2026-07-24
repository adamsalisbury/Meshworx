using System.Buffers;
using System.Buffers.Binary;
using System.Net.Sockets;

namespace AdamSalisbury.Meshworx.Transport.Tcp;

/// <summary>
/// An <see cref="ITransport"/> implementation that communicates over TCP using length-prefixed framing.
/// </summary>
/// <remarks>
/// Each message is transmitted as a 4-byte big-endian length header followed by the payload bytes.
/// Write operations are internally synchronised, so concurrent calls to
/// <see cref="SendAsync(ReadOnlyMemory{byte}, CancellationToken)"/> from multiple threads are safe.
/// </remarks>
public sealed class TcpTransport : ITransport, IBatchSendTransport
{
    private const int HeaderSize = 4;
    private const int MaxPayloadSize = 1024 * 1024;

    private readonly TcpClient? _tcpClient;
    private readonly Stream _stream;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    internal TcpTransport(TcpClient tcpClient)
    {
        _tcpClient = tcpClient;
        _stream = tcpClient.GetStream();
    }

    internal TcpTransport(Stream stream)
    {
        _stream = stream;
    }

    /// <summary>
    /// Creates a new <see cref="TcpTransport"/> by connecting to the specified remote endpoint.
    /// </summary>
    /// <param name="host">The hostname or IP address of the remote endpoint.</param>
    /// <param name="port">The TCP port of the remote endpoint.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A connected <see cref="TcpTransport"/> ready for use.</returns>
    public static async Task<TcpTransport> ConnectAsync(
        string host,
        int port,
        CancellationToken cancellationToken = default)
    {
        var tcpClient = new TcpClient();
        try
        {
            tcpClient.NoDelay = true;
            await tcpClient.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
            return new TcpTransport(tcpClient);
        }
        catch
        {
            tcpClient.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        // Reject oversized payloads up front. The receiving peer enforces the same limit and
        // would otherwise treat the frame as corrupt and drop the connection — a clear
        // ArgumentException to the caller is far better than a surprise disconnect. This also
        // guards the frameSize addition below against integer overflow.
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

            await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _stream.WriteAsync(frame.AsMemory(0, frameSize), cancellationToken).ConfigureAwait(false);
                await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _writeLock.Release();
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(frame);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Frames every message with its own length prefix and writes the whole batch with a single
    /// buffered <see cref="Stream.WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/> and flush,
    /// so a burst of queued frames costs one syscall and one flush instead of one per frame.
    /// </remarks>
    public async Task SendAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);

        if (messages.Count == 0)
        {
            return;
        }

        if (messages.Count == 1)
        {
            await SendAsync(messages[0], cancellationToken).ConfigureAwait(false);
            return;
        }

        // Frame only the valid prefix up to the first oversize payload (if any). Writing that prefix
        // before throwing preserves the single-send path's deliver-then-fault behaviour: frames
        // coalesced ahead of an oversize one are still delivered, rather than the whole batch being
        // discarded because a later frame is invalid.
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
            // The send loop bounds a batch's total size, so frameSize is well within int range here;
            // the cast is safe. Renting a single buffer keeps the prefix to one write and one flush.
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

                await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    await _stream.WriteAsync(frame.AsMemory(0, offset), cancellationToken).ConfigureAwait(false);
                    await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _writeLock.Release();
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

    /// <inheritdoc/>
    public async Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var header = await ReadBytesAsync(HeaderSize, cancellationToken).ConfigureAwait(false);
        if (header is null)
        {
            return null;
        }

        int payloadLength = BinaryPrimitives.ReadInt32BigEndian(header);

        if (payloadLength is < 0 or > MaxPayloadSize)
        {
            // A corrupt or out-of-range length prefix means the stream framing is no longer
            // trustworthy. Surface it as an I/O error so receive loops treat it as a transport
            // failure and terminate the connection cleanly, rather than faulting on an
            // unhandled exception type.
            throw new IOException($"Invalid payload length: {payloadLength}");
        }

        if (payloadLength == 0)
        {
            return [];
        }

        return await ReadBytesAsync(payloadLength, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync().ConfigureAwait(false);
        _tcpClient?.Dispose();
        _writeLock.Dispose();
    }

    private async Task<byte[]?> ReadBytesAsync(int count, CancellationToken cancellationToken)
    {
        var buffer = new byte[count];
        try
        {
            await _stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
            return buffer;
        }
        catch (EndOfStreamException)
        {
            return null;
        }
    }
}
