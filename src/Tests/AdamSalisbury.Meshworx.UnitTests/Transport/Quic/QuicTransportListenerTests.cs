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

    /// <summary>
    /// A pending accept, raced against dispose, only ever ends in the disposal being reported — the same
    /// guarantee <c>TcpTransportListenerTests</c> and <c>UnixSocketTransportListenerTests</c> lock in for
    /// their own listeners.
    /// </summary>
    /// <remarks>
    /// The accept and the dispose are dispatched to separate threads and released together. Calling them
    /// in sequence on one thread would not race at all: an accept called first has always registered
    /// itself before it yields, so it would only ever exercise the pending-accept path.
    /// </remarks>
    [Fact(Timeout = 30000)]
    public async Task AcceptAsync_RacedAgainstDispose_OnlyEverReportsDisposal()
    {
        if (!System.Net.Quic.QuicListener.IsSupported)
        {
            return;
        }

        const int attempts = 25;

        for (int attempt = 0; attempt < attempts; attempt++)
        {
            using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
            var listener = new QuicTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0),
                new SslServerAuthenticationOptions { ServerCertificate = certificate });
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

    /// <summary>
    /// A DisposeAsync racing an in-flight StartAsync never leaves a listener neither published nor
    /// torn down. This race has no analogue on the other transport listeners in this repo: they all bind
    /// synchronously inside a lock, so the equivalent window cannot exist for them —
    /// <c>QuicListener.ListenAsync</c> is itself the async bind/listen call, with no synchronous
    /// constructor to take the lock around, so
    /// <see cref="QuicTransportListener.StartAsync"/> instead rechecks disposal once that await
    /// completes.
    /// </summary>
    /// <remarks>
    /// DisposeAsync does no I/O when nothing has been published yet, so it reliably completes before
    /// StartAsync's own await on the real socket bind in practice — reliably enough to exercise the race
    /// this test targets across repeated attempts, without needing an artificial synchronisation point
    /// inside StartAsync itself.
    /// </remarks>
    [Fact(Timeout = 30000)]
    public async Task DisposeAsync_RacedAgainstStartAsync_NeverLeavesAnUnpublishedListenerRunning()
    {
        if (!System.Net.Quic.QuicListener.IsSupported)
        {
            return;
        }

        const int attempts = 25;

        for (int attempt = 0; attempt < attempts; attempt++)
        {
            using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
            var listener = new QuicTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0),
                new SslServerAuthenticationOptions { ServerCertificate = certificate });

            using var released = new SemaphoreSlim(0, 2);

            Task<Exception?> startTask = Task.Run<Exception?>(async () =>
            {
                await released.WaitAsync().ConfigureAwait(false);
                return await Record.ExceptionAsync(() => listener.StartAsync()).ConfigureAwait(false);
            });

            Task disposeTask = Task.Run(async () =>
            {
                await released.WaitAsync().ConfigureAwait(false);
                await listener.DisposeAsync().ConfigureAwait(false);
            });

            released.Release(2);

            await disposeTask.ConfigureAwait(false);
            Exception? startException = await startTask.ConfigureAwait(false);

            // Whichever way the race resolves, StartAsync either completed normally (it published the
            // listener before Dispose's teardown ran) or reports the disposal — never anything else, and
            // never a listener left running that nothing tracks.
            if (startException is not null)
            {
                Assert.IsType<ObjectDisposedException>(startException);
            }

            await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());

            // Idempotent regardless of which side of the race published state that needs tearing down.
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }
}
