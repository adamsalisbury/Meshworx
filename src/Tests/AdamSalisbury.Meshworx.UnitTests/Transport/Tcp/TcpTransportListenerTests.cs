using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;

public sealed class TcpTransportListenerTests
{
    /// <summary>
    /// When StartAsync is called on a listener that is already running, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_AlreadyRunning_ThrowsInvalidOperationException()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
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
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.AcceptAsync());

        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// When DisposeAsync is called on a started listener, the listener is stopped and a subsequent accept
    /// reports the disposal rather than claiming the listener was never started — an accept loop stops on
    /// the former and retries the latter for ever.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_AfterStart_AcceptAsyncThrowsObjectDisposedException()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync();

        await listener.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// A disposed listener stays disposed. Restarting one would bind a fresh socket onto an object that is
    /// being torn down, leaving a running listener behind that nothing owns and no longer tracks.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.StartAsync());
    }

    /// <summary>
    /// When a cleartext listener is disposed while an accept is pending, the wait ends with an
    /// ObjectDisposedException rather than the socket-level error the stopped listener actually raises,
    /// matching what the TLS path already reports.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_CleartextListenerWithPendingAccept_AcceptThrowsObjectDisposedException()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);

        Task<ITransport> acceptTask = listener.AcceptAsync();

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => acceptTask);
    }

    /// <summary>
    /// Every way a stopped TcpListener can report itself is one the accept path recognises as disposal.
    /// </summary>
    /// <remarks>
    /// The exception depends on where the stop lands. An accept already pending gets a SocketException or
    /// an ObjectDisposedException; an accept issued just after the stop gets a plain
    /// InvalidOperationException — "Not listening" — since to the TcpListener it was never started. That
    /// third case sits in a window too narrow to reach reliably from a test, so it is asserted here
    /// against the framework directly rather than through the listener, which is the only way it would be
    /// noticed if a framework upgrade changed it.
    /// </remarks>
    [Fact(Timeout = 5000)]
    public async Task IsStoppedListenerFailure_WhatAStoppedTcpListenerActuallyThrows_IsRecognised()
    {
        var pendingAccept = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        pendingAccept.Start();
        Task<System.Net.Sockets.TcpClient> pending = pendingAccept.AcceptTcpClientAsync();
        pendingAccept.Stop();

        Exception? whilePending = await Record.ExceptionAsync(() => pending).ConfigureAwait(false);
        Assert.NotNull(whilePending);
        Assert.True(
            TcpTransportListener.IsStoppedListenerFailure(whilePending),
            $"An accept pending when the listener stopped threw {whilePending.GetType()}.");

        var stopped = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        stopped.Start();
        stopped.Stop();

        Exception? afterStop = await Record.ExceptionAsync(() => stopped.AcceptTcpClientAsync())
            .ConfigureAwait(false);
        Assert.NotNull(afterStop);
        Assert.True(
            TcpTransportListener.IsStoppedListenerFailure(afterStop),
            $"An accept issued after the listener stopped threw {afterStop.GetType()}.");
    }

    /// <summary>
    /// Repeatedly racing an accept against a dispose only ever ends in the disposal being reported. Any
    /// other outcome — a NullReferenceException from the listener being cleared mid-accept, a claim that
    /// the listener was never started, or a raw socket error — is a caller-visible symptom of the two
    /// paths reading the same fields without agreement.
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
            var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
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

            // The accept may lose the race outright and never start, in which case it is refused up front;
            // either way the caller is told the same thing.
            Assert.IsType<ObjectDisposedException>(caught);
        }
    }

    /// <summary>
    /// Disposing the same listener from several threads at once does not throw: no call trips over state
    /// another has already cleared, and the listener is disposed once they have all returned.
    /// </summary>
    /// <remarks>
    /// This locks in the guarantee rather than reproducing a failure. The shape it replaced was racy but
    /// not reliably so — the window in which a second disposer could touch an already-disposed
    /// cancellation source was a few instructions wide — so this asserts the property the elected single
    /// teardown makes structural.
    /// </remarks>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_CalledConcurrently_DoesNotThrowAndLeavesListenerDisposed()
    {
        const int disposers = 8;

        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
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

        // Every disposer has returned, so the teardown is complete for all of them, not just the one that
        // performed it.
        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// The same holds for a TLS listener, whose teardown owns considerably more: a cancellation source, a
    /// handshake pump and the connections it has already negotiated. A second disposer must not cancel or
    /// dispose any of those a second time.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task DisposeAsync_TlsListenerCalledConcurrently_DoesNotThrowAndLeavesListenerDisposed()
    {
        const int disposers = 8;

        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = serverCertificate },
            // The disposal is what must unwind the silent peer's handshake. Keeping the handshake's own
            // deadline well inside this test's means that if it ever stops doing so, the test fails on the
            // assertion rather than expiring, which says nothing about why.
            tlsHandshakeTimeout: TimeSpan.FromSeconds(2));

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        // A peer that connects and then says nothing leaves a handshake in flight for the teardown to
        // unwind, so the disposers overlap on real work rather than on an idle listener.
        using var silent = new System.Net.Sockets.TcpClient();
        await silent.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);

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
    /// Disposing twice in sequence is a no-op the second time, so a caller that disposes a listener it
    /// also owns through a using block is not punished for it.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);

        await listener.DisposeAsync().ConfigureAwait(false);
        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// A listener that was never started can still be disposed, and reports itself as disposed afterwards.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_NeverStarted_DoesNotThrowAndBlocksLaterUse()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.StartAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }
}