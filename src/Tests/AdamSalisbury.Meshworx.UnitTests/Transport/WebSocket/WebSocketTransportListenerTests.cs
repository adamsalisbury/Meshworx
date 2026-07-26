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
}
