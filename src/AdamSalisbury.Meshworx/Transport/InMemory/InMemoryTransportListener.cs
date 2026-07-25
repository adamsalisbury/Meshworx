using System.Threading.Channels;

namespace AdamSalisbury.Meshworx.Transport.InMemory;

/// <summary>
/// An in-process <see cref="ITransportListener"/> that pairs with <see cref="InMemoryTransport"/>.
/// Clients establish a connection by calling <see cref="Connect"/>, and the hub accepts the
/// corresponding server endpoint through <see cref="AcceptAsync"/>.
/// </summary>
public sealed class InMemoryTransportListener : ITransportListener
{
    private readonly Channel<InMemoryTransport> _pendingConnections = Channel.CreateUnbounded<InMemoryTransport>();
    private int _started;
    private int _disposed;

    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (Interlocked.Exchange(ref _started, 1) == 1)
        {
            throw new InvalidOperationException("The listener is already running.");
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Establishes a new in-process connection to the hub and returns the client endpoint.
    /// </summary>
    /// <returns>The client side of a connected transport pair.</returns>
    /// <exception cref="InvalidOperationException">The listener has not been started or has been disposed.</exception>
    public ITransport Connect()
    {
        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("The listener has not been started.");
        }

        (InMemoryTransport client, InMemoryTransport server) = InMemoryTransport.CreatePair();

        if (!_pendingConnections.Writer.TryWrite(server))
        {
            throw new InvalidOperationException("The listener has been stopped.");
        }

        return client;
    }

    /// <inheritdoc/>
    public async Task<ITransport> AcceptAsync(CancellationToken cancellationToken = default)
    {
        // Checked ahead of the started guard, and ahead of the read: a disposed listener reports itself as
        // disposed whether or not it ever ran, and must not hand out a connection that was queued before
        // it was disposed — completing the channel does not discard what is already buffered.
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("The listener has not been started.");
        }

        try
        {
            return await _pendingConnections.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            throw new ObjectDisposedException(nameof(InMemoryTransportListener));
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _pendingConnections.Writer.TryComplete();

        // Connections that were established but never accepted belong to nobody else once the listener is
        // gone; close them rather than leaving a client parked on a server end that will never be read.
        while (_pendingConnections.Reader.TryRead(out InMemoryTransport? pending))
        {
            await pending.DisposeAsync().ConfigureAwait(false);
        }
    }
}
