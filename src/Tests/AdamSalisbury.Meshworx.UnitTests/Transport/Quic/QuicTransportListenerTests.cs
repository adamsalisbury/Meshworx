using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using AdamSalisbury.Meshworx.Transport.Quic;
using AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Quic;

/// <summary>
/// QUIC requires QuicListener.IsSupported to be true — typically meaning the native msquic library is
/// installed and the platform's TLS stack supports TLS 1.3. Every test in this file is skipped as a
/// no-op where that is not the case, since the constructor and lifecycle contract this file verifies is
/// otherwise identical to TcpTransportListener's.
/// </summary>
public sealed class QuicTransportListenerTests
{
    private static SslServerAuthenticationOptions CreateTlsOptions()
    {
        return new SslServerAuthenticationOptions { ServerCertificate = TestCertificates.CreateSelfSigned() };
    }

    /// <summary>
    /// When the endpoint is null, an ArgumentNullException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_NullEndPoint_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(
            () => new QuicTransportListener((IPEndPoint)null!, CreateTlsOptions()));
    }

    /// <summary>
    /// When the TLS options are null, an ArgumentNullException is thrown — QUIC mandates TLS, unlike
    /// the TCP transport where it is optional.
    /// </summary>
    [Fact]
    public void Constructor_NullTlsOptions_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(
            () => new QuicTransportListener(new IPEndPoint(IPAddress.Loopback, 0), null!));
    }

    /// <summary>
    /// When TLS options are supplied without a certificate, a certificate context, or a selection
    /// callback, an ArgumentException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_TlsOptionsWithoutCertificate_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(
            () => new QuicTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0), new SslServerAuthenticationOptions()));
    }

    /// <summary>
    /// When the stream-open timeout is not positive, an ArgumentOutOfRangeException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_NonPositiveStreamOpenTimeout_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new QuicTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0), CreateTlsOptions(), streamOpenTimeout: TimeSpan.Zero));
    }

    /// <summary>
    /// When the maximum concurrent negotiation count is not positive, an ArgumentOutOfRangeException is
    /// thrown.
    /// </summary>
    [Fact]
    public void Constructor_NonPositiveMaxConcurrentNegotiations_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new QuicTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0), CreateTlsOptions(), maxConcurrentNegotiations: 0));
    }

    /// <summary>
    /// When StartAsync is called on a listener that is already running, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_AlreadyRunning_ThrowsInvalidOperationException()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        var listener = new QuicTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = certificate });

        if (!System.Net.Quic.QuicListener.IsSupported)
        {
            await Assert.ThrowsAsync<PlatformNotSupportedException>(() => listener.StartAsync());
            return;
        }

        await listener.StartAsync().ConfigureAwait(false);

        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When AcceptAsync is called before the listener has been started, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AcceptAsync_NotStarted_ThrowsInvalidOperationException()
    {
        var listener = new QuicTransportListener(new IPEndPoint(IPAddress.Loopback, 0), CreateTlsOptions());

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.AcceptAsync());

        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// When DisposeAsync is called on a started listener, a subsequent accept reports the disposal
    /// rather than hanging or reporting the listener as never started.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_AfterStart_AcceptAsyncThrowsObjectDisposedException()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        var listener = new QuicTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = certificate });

        if (!System.Net.Quic.QuicListener.IsSupported)
        {
            return;
        }

        await listener.StartAsync();

        await listener.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// A disposed listener stays disposed rather than being restartable.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        var listener = new QuicTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = certificate });

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.StartAsync());
    }

    /// <summary>
    /// DisposeAsync is safe to call more than once.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        var listener = new QuicTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = certificate });

        if (System.Net.Quic.QuicListener.IsSupported)
        {
            await listener.StartAsync().ConfigureAwait(false);
        }

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
        if (!System.Net.Quic.QuicListener.IsSupported)
        {
            return;
        }

        const int disposers = 8;

        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        var listener = new QuicTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = certificate });
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
}
