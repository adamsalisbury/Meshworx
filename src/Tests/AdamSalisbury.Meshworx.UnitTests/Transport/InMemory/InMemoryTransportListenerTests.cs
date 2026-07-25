using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.InMemory;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.InMemory;

public sealed class InMemoryTransportListenerTests
{
    /// <summary>
    /// A disposed listener reports itself as disposed, rather than as one that was never started, so a
    /// caller's accept loop can stop on it.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AcceptAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var listener = new InMemoryTransportListener();
        await listener.StartAsync().ConfigureAwait(false);

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// The same holds for a listener that was disposed without ever being started: disposal is the more
    /// useful of the two facts, and it is the one the interface asks for.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AcceptAsync_DisposedWithoutEverStarting_ThrowsObjectDisposedException()
    {
        var listener = new InMemoryTransportListener();

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// A connection established but not yet accepted is closed by disposal, not handed out afterwards.
    /// Completing the channel does not discard what is already queued, so without an explicit drain a
    /// disposed listener would still deal out live connections and leave their clients parked on a server
    /// end nobody would ever read.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_WithConnectionQueued_ClosesItRatherThanServingIt()
    {
        var listener = new InMemoryTransportListener();
        await listener.StartAsync().ConfigureAwait(false);

        ITransport client = listener.Connect();

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());

        // The server end was disposed, which completes the channel the client reads from: a null read is
        // how this transport reports a closed connection.
        Assert.Null(await client.ReceiveAsync().ConfigureAwait(false));

        await client.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Disposal is idempotent, and safe to call from several threads at once.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_CalledRepeatedlyAndConcurrently_DoesNotThrow()
    {
        const int disposers = 8;

        var listener = new InMemoryTransportListener();
        await listener.StartAsync().ConfigureAwait(false);

        using var start = new SemaphoreSlim(0, disposers);
        var disposals = new Task[disposers];

        for (int i = 0; i < disposers; i++)
        {
            disposals[i] = Task.Run(async () =>
            {
                await start.WaitAsync().ConfigureAwait(false);
                await listener.DisposeAsync().ConfigureAwait(false);
            });
        }

        start.Release(disposers);

        await Task.WhenAll(disposals).ConfigureAwait(false);
        await listener.DisposeAsync().ConfigureAwait(false);
    }
}
