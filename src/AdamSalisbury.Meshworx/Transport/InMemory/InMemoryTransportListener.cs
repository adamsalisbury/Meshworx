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
    public ValueTask DisposeAsync()
    {
        _pendingConnections.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
