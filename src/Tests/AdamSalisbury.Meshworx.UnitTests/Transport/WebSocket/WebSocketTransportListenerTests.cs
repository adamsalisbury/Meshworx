using System.Net;
using System.Net.Security;
using AdamSalisbury.Meshworx.Transport.WebSocket;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.WebSocket;

public sealed class WebSocketTransportListenerTests
{
    /// <summary>
    /// When the endpoint is null, an ArgumentNullException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_NullEndPoint_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(
            () => new WebSocketTransportListener((IPEndPoint)null!));
    }

    /// <summary>
    /// When the upgrade path is null or empty, an ArgumentException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_EmptyPath_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(
            () => new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0), path: string.Empty));
    }

    /// <summary>
    /// When TLS options are supplied without a certificate, a certificate context, or a selection
    /// callback, an ArgumentException is thrown — otherwise every handshake would fail without any
    /// obvious cause.
    /// </summary>
    [Fact]
    public void Constructor_TlsOptionsWithoutCertificate_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(
            () => new WebSocketTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0),
                tlsOptions: new SslServerAuthenticationOptions()));
    }

    /// <summary>
    /// When the handshake timeout is not positive, an ArgumentOutOfRangeException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_NonPositiveHandshakeTimeout_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new WebSocketTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0), handshakeTimeout: TimeSpan.Zero));
    }

    /// <summary>
    /// When the maximum concurrent handshake count is not positive, an ArgumentOutOfRangeException is
    /// thrown.
    /// </summary>
    [Fact]
    public void Constructor_NonPositiveMaxConcurrentHandshakes_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new WebSocketTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0), maxConcurrentHandshakes: 0));
    }

    /// <summary>
    /// When StartAsync is called on a listener that is already running, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_AlreadyRunning_ThrowsInvalidOperationException()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());

        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// When AcceptAsync is called before the listener has been started, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AcceptAsync_NotStarted_ThrowsInvalidOperationException()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.AcceptAsync());

        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// When DisposeAsync is called on a started listener, a subsequent accept reports the disposal
    /// rather than hanging or reporting the listener as never started.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_AfterStart_AcceptAsyncThrowsObjectDisposedException()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync();

        await listener.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// A disposed listener stays disposed rather than being restartable.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.StartAsync());
    }

    /// <summary>
    /// DisposeAsync is safe to call more than once.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);

        await listener.DisposeAsync().ConfigureAwait(false);
        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Disposing the same listener from several threads at once does not throw: no call trips over state
    /// another has already cleared, and the listener is disposed once they have all returned.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_CalledConcurrently_DoesNotThrowAndLeavesListenerDisposed()
    {
        const int disposers = 8;

        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
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

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// Repeatedly racing an accept against a dispose only ever ends in the disposal being reported,
    /// mirroring the same guarantee TcpTransportListener locks in for its own pump-backed AcceptAsync.
    /// </summary>
    /// <remarks>
    /// The accept and the dispose are dispatched to separate threads and released together. Calling them
    /// in sequence on one thread would not race at all: an accept called first has always registered
    /// itself before it yields, so it would only ever exercise the pending-accept path.
    /// </remarks>
    [Fact(Timeout = 30000)]
    public async Task AcceptAsync_RacedAgainstDispose_OnlyEverReportsDisposal()
    {
        const int attempts = 50;

        for (int attempt = 0; attempt < attempts; attempt++)
        {
            var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
            await listener.StartAsync().ConfigureAwait(false);

            using var released = new SemaphoreSlim(0, 2);

            Task<Exception?> acceptTask = Task.Run<Exception?>(async () =>
            {
                await released.WaitAsync().ConfigureAwait(false);
                return await Record.ExceptionAsync(() => listener.AcceptAsync()).ConfigureAwait(false);
            });

            Task disposeTask = Task.Run(async () =>
            {
                await released.WaitAsync().ConfigureAwait(false);
                await listener.DisposeAsync().ConfigureAwait(false);
            });

            released.Release(2);

            await disposeTask.ConfigureAwait(false);
            Exception? caught = await acceptTask.ConfigureAwait(false);

            Assert.IsType<ObjectDisposedException>(caught);
        }
    }
}
