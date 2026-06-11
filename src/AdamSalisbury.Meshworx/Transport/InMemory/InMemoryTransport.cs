using System.Threading.Channels;

namespace AdamSalisbury.Meshworx.Transport.InMemory;

/// <summary>
/// An in-process <see cref="ITransport"/> that exchanges messages with a paired endpoint through
/// channels rather than a network. Useful for hosting a hub and clients in the same process and for
/// fast, deterministic testing.
/// </summary>
/// <remarks>
/// Each message passed to <see cref="SendAsync"/> is copied, so callers may reuse their buffers. The
/// channels preserve message boundaries, so no framing is applied.
/// </remarks>
public sealed class InMemoryTransport : ITransport
{
    private readonly ChannelReader<byte[]> _receive;
    private readonly ChannelWriter<byte[]> _send;
    private int _disposed;

    internal InMemoryTransport(ChannelReader<byte[]> receive, ChannelWriter<byte[]> send)
    {
        _receive = receive;
        _send = send;
    }

    /// <summary>
    /// Creates a connected pair of in-memory transports. A message sent on one endpoint is received on
    /// the other.
    /// </summary>
    /// <returns>The two connected endpoints.</returns>
    public static (InMemoryTransport First, InMemoryTransport Second) CreatePair()
    {
        var firstInbound = Channel.CreateUnbounded<byte[]>();
        var secondInbound = Channel.CreateUnbounded<byte[]>();

        var first = new InMemoryTransport(firstInbound.Reader, secondInbound.Writer);
        var second = new InMemoryTransport(secondInbound.Reader, firstInbound.Writer);

        return (first, second);
    }

    /// <inheritdoc/>
    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        // Copy: the message stays queued until the peer reads it, so it must not alias the caller's buffer.
        _send.TryWrite(data.ToArray());
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _receive.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            // The peer disposed its endpoint and completed this channel: signal a closed connection.
            return null;
        }
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            // Completing the send channel ends the peer's ReceiveAsync, mirroring a closed connection.
            _send.TryComplete();
        }

        return ValueTask.CompletedTask;
    }
}
