using System.IO.Pipes;
using AdamSalisbury.Meshworx.Transport.Framing;

namespace AdamSalisbury.Meshworx.Transport.NamedPipes;

/// <summary>
/// An <see cref="ITransport"/> implementation that communicates over a Windows named pipe, for
/// low-latency inter-process communication between a hub and clients on the same host.
/// </summary>
/// <remarks>
/// Framing is identical to <see cref="Tcp.TcpTransport"/> and <see cref="Unix.UnixSocketTransport"/> — a
/// 4-byte big-endian length prefix per message — sharing the same <see cref="StreamFramer"/> helper
/// rather than reimplementing it, since a named pipe is stream-oriented exactly as TCP and a Unix domain
/// socket are. Write operations are internally synchronised, so concurrent calls to
/// <see cref="SendAsync(ReadOnlyMemory{byte}, CancellationToken)"/> from multiple threads are safe.
/// <para>
/// Named pipes are a Windows-only mechanism. <see cref="ConnectAsync"/> throws
/// <see cref="PlatformNotSupportedException"/> on every other operating system — use
/// <see cref="Unix.UnixSocketTransport"/> for the equivalent local inter-process transport on Linux and
/// macOS.
/// </para>
/// </remarks>
public sealed class NamedPipeTransport : ITransport, IBatchSendTransport
{
    private readonly PipeStream _pipeStream;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    // Reused across reads to hold each frame's length prefix. The transport is single-reader (see
    // ITransport), so ReceiveAsync is never called concurrently and the buffer cannot be aliased.
    private readonly byte[] _headerBuffer = new byte[StreamFramer.HeaderSize];

    internal NamedPipeTransport(PipeStream pipeStream)
    {
        _pipeStream = pipeStream;
    }

    /// <summary>
    /// Creates a new <see cref="NamedPipeTransport"/> by connecting to a named pipe server.
    /// </summary>
    /// <param name="pipeName">The name of the pipe to connect to, as passed to the server's listener.</param>
    /// <param name="serverName">
    /// The name of the remote computer hosting the pipe, or <c>"."</c> (the default) for the local
    /// computer.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A connected <see cref="NamedPipeTransport"/> ready for use.</returns>
    /// <exception cref="ArgumentException"><paramref name="pipeName"/> is null or empty.</exception>
    /// <exception cref="PlatformNotSupportedException">The current operating system is not Windows.</exception>
    public static async Task<NamedPipeTransport> ConnectAsync(
        string pipeName,
        string serverName = ".",
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipeName);

        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Named pipes are only supported on Windows. Use UnixSocketTransport for local "
                    + "inter-process communication on Linux and macOS.");
        }

        var client = new NamedPipeClientStream(
            serverName, pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
        try
        {
            await client.ConnectAsync(cancellationToken).ConfigureAwait(false);
            return new NamedPipeTransport(client);
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc/>
    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        return StreamFramer.SendAsync(_pipeStream, _writeLock, data, cancellationToken);
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
        return StreamFramer.SendBatchAsync(_pipeStream, _writeLock, messages, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        return StreamFramer.ReceiveAsync(_pipeStream, _headerBuffer, cancellationToken);
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
        await _pipeStream.DisposeAsync().ConfigureAwait(false);
    }
}
