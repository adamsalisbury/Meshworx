using System.Net.Sockets;
using AdamSalisbury.Meshworx.Transport.Framing;

namespace AdamSalisbury.Meshworx.Transport.Unix;

/// <summary>
/// An <see cref="ITransport"/> implementation that communicates over a Unix domain socket, for
/// low-latency, portless inter-process communication between a hub and clients on the same host.
/// </summary>
/// <remarks>
/// Framing is identical to <see cref="Tcp.TcpTransport"/> — a 4-byte big-endian length prefix per
/// message — sharing the same <see cref="StreamFramer"/> helper rather than reimplementing it, since a
/// Unix domain socket is stream-oriented exactly as TCP is. Write operations are internally
/// synchronised, so concurrent calls to <see cref="SendAsync(ReadOnlyMemory{byte}, CancellationToken)"/>
/// from multiple threads are safe.
/// <para>
/// Unix domain sockets are local to the host: reachable only by processes with filesystem access to the
/// bound path, and never over a network. There is no equivalent of TCP's TLS option here — the operating
/// system's filesystem permissions on the socket path are the access control, not a cryptographic one.
/// </para>
/// </remarks>
public sealed class UnixSocketTransport : ITransport, IBatchSendTransport
{
    private readonly Socket? _socket;
    private readonly Stream _stream;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    // Reused across reads to hold each frame's length prefix. The transport is single-reader (see
    // ITransport), so ReceiveAsync is never called concurrently and the buffer cannot be aliased.
    private readonly byte[] _headerBuffer = new byte[StreamFramer.HeaderSize];

    internal UnixSocketTransport(Socket socket)
        : this(socket, new NetworkStream(socket, ownsSocket: true))
    {
    }

    internal UnixSocketTransport(Stream stream)
        : this(null, stream)
    {
    }

    private UnixSocketTransport(Socket? socket, Stream stream)
    {
        _socket = socket;
        _stream = stream;
    }

    /// <summary>
    /// Creates a new <see cref="UnixSocketTransport"/> by connecting to the Unix domain socket bound at
    /// the given filesystem path.
    /// </summary>
    /// <param name="path">The filesystem path of the listening Unix domain socket.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A connected <see cref="UnixSocketTransport"/> ready for use.</returns>
    /// <exception cref="ArgumentException"><paramref name="path"/> is null or empty.</exception>
    public static async Task<UnixSocketTransport> ConnectAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            await socket.ConnectAsync(new UnixDomainSocketEndPoint(path), cancellationToken).ConfigureAwait(false);
            return new UnixSocketTransport(socket);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        return StreamFramer.SendAsync(_stream, _writeLock, data, cancellationToken);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Frames every message with its own length prefix and writes the whole batch with a single
    /// buffered write and flush, so a burst of queued frames costs one syscall instead of one per frame
    /// — matching <see cref="Tcp.TcpTransport"/>'s batching behaviour exactly, since both share
    /// <see cref="StreamFramer"/>.
    /// </remarks>
    public Task SendAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> messages,
        CancellationToken cancellationToken = default)
    {
        return StreamFramer.SendBatchAsync(_stream, _writeLock, messages, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        return StreamFramer.ReceiveAsync(_stream, _headerBuffer, cancellationToken);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Deliberately does not dispose <see cref="_writeLock"/>. <see cref="SemaphoreSlim.Dispose()"/>
    /// abandons rather than completes any queued <see cref="SemaphoreSlim.WaitAsync()"/> waiter, so a
    /// concurrent <see cref="SendAsync(ReadOnlyMemory{byte}, CancellationToken)"/> racing this teardown
    /// would hang for ever instead of observing the stream fault it is actually waiting behind. The
    /// semaphore never touches <see cref="SemaphoreSlim.AvailableWaitHandle"/>, so it holds no unmanaged
    /// resource and leaving it undisposed is safe — it is simply collected once this transport is.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync().ConfigureAwait(false);
        _socket?.Dispose();
    }
}
