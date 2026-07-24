using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading.Channels;

namespace AdamSalisbury.Meshworx.Transport.Tcp;

/// <summary>
/// An <see cref="ITransportListener"/> implementation that accepts incoming TCP connections, optionally
/// completing a TLS handshake on each one before handing it to the hub.
/// </summary>
/// <remarks>
/// Without TLS options the accepted connections are cleartext. Supplying
/// <see cref="SslServerAuthenticationOptions"/> makes every accepted connection encrypted and
/// integrity-protected, and — with <see cref="SslServerAuthenticationOptions.ClientCertificateRequired"/>
/// — mutually authenticated. The framing is identical either way.
/// </remarks>
public sealed class TcpTransportListener : ITransportListener
{
    private static readonly TimeSpan DefaultTlsHandshakeTimeout = TimeSpan.FromSeconds(10);
    private const int DefaultMaxConcurrentTlsHandshakes = 64;

    // How many connections may be waiting to negotiate at once, as a multiple of the handshake
    // concurrency limit. Negotiating connections are mostly idle — waiting on the peer's next flight — so
    // this is deliberately far larger than the CPU bound: it exists to cap memory and descriptors, not
    // work. Sizing it off the handshake limit keeps one knob meaningful instead of two.
    private const int PendingHandshakeMultiplier = 16;

    // Pause after a failed accept, so a persistent failure such as descriptor exhaustion cannot spin the
    // pump hot while still recovering promptly once the condition clears.
    private static readonly TimeSpan AcceptRetryDelay = TimeSpan.FromMilliseconds(50);

    private readonly IPEndPoint _endPoint;
    private readonly SslServerAuthenticationOptions? _tlsOptions;
    private readonly TimeSpan _tlsHandshakeTimeout;
    private readonly int _maxConcurrentTlsHandshakes;
    private readonly int _maxPendingTlsHandshakes;

    private TcpListener? _listener;

    // Only used when TLS is configured. The handshake runs off the accept path, so a peer that opens a
    // connection and then stalls cannot hold up every other client's accept.
    private Channel<TcpTransport>? _handshakenTransports;
    private CancellationTokenSource? _handshakeCts;
    private Task? _handshakePumpTask;

    internal EndPoint? LocalEndPoint => _listener?.LocalEndpoint;

    /// <summary>
    /// Creates a listener bound to the given endpoint.
    /// </summary>
    /// <param name="endPoint">The endpoint to bind to.</param>
    /// <param name="tlsOptions">
    /// TLS options to authenticate each accepted connection as the server, or <see langword="null"/>
    /// (the default) to accept cleartext connections. Set
    /// <see cref="SslServerAuthenticationOptions.ServerCertificate"/> to the hub's certificate, and
    /// <see cref="SslServerAuthenticationOptions.ClientCertificateRequired"/> with a
    /// <see cref="SslServerAuthenticationOptions.RemoteCertificateValidationCallback"/> for mutual TLS.
    /// The options are copied, so later mutation of the caller's instance does not affect this listener.
    /// </param>
    /// <param name="tlsHandshakeTimeout">
    /// How long a single TLS handshake may take before the connection is abandoned. Bounds the work an
    /// unauthenticated peer can hold open. Defaults to 10 seconds. Ignored without
    /// <paramref name="tlsOptions"/>.
    /// </param>
    /// <param name="maxConcurrentTlsHandshakes">
    /// The maximum number of TLS handshakes to run at once. The handshake is asymmetric cryptography on
    /// unauthenticated input, so this bounds the CPU a connection flood can demand. A connection only
    /// counts against this once its peer has actually sent something, so peers that connect and stay
    /// silent do not consume the budget. Sixteen times this value may be waiting to negotiate at any
    /// moment, beyond which new connections are refused rather than queued. Defaults to 64. Ignored
    /// without <paramref name="tlsOptions"/>.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="endPoint"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="tlsOptions"/> was supplied with neither a
    /// <see cref="SslServerAuthenticationOptions.ServerCertificate"/> nor a
    /// <see cref="SslServerAuthenticationOptions.ServerCertificateContext"/> nor a
    /// <see cref="SslServerAuthenticationOptions.ServerCertificateSelectionCallback"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="tlsHandshakeTimeout"/> or <paramref name="maxConcurrentTlsHandshakes"/> is not
    /// positive.
    /// </exception>
    public TcpTransportListener(
        IPEndPoint endPoint,
        SslServerAuthenticationOptions? tlsOptions = null,
        TimeSpan? tlsHandshakeTimeout = null,
        int? maxConcurrentTlsHandshakes = null)
    {
        ArgumentNullException.ThrowIfNull(endPoint);

        if (tlsHandshakeTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(tlsHandshakeTimeout), "The TLS handshake timeout must be positive.");
        }

        if (maxConcurrentTlsHandshakes is { } maxHandshakes && maxHandshakes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConcurrentTlsHandshakes), "The maximum concurrent TLS handshake count must be positive.");
        }

        if (tlsOptions is not null
            && tlsOptions.ServerCertificate is null
            && tlsOptions.ServerCertificateContext is null
            && tlsOptions.ServerCertificateSelectionCallback is null)
        {
            // Caught here rather than per-connection: without a certificate every handshake fails, and a
            // listener that accepts nothing is far harder to diagnose at run time than a failed
            // construction.
            throw new ArgumentException(
                "The TLS options must supply a server certificate, a certificate context, or a certificate "
                    + "selection callback.",
                nameof(tlsOptions));
        }

        _endPoint = endPoint;
        _tlsOptions = tlsOptions is null ? null : CloneServerOptions(tlsOptions);
        _tlsHandshakeTimeout = tlsHandshakeTimeout ?? DefaultTlsHandshakeTimeout;
        _maxConcurrentTlsHandshakes = maxConcurrentTlsHandshakes ?? DefaultMaxConcurrentTlsHandshakes;
        _maxPendingTlsHandshakes = _maxConcurrentTlsHandshakes * PendingHandshakeMultiplier;
    }

    /// <summary>
    /// Creates a listener bound to the loopback interface on the given port.
    /// </summary>
    /// <remarks>
    /// Binds to <see cref="IPAddress.Loopback"/>, not every interface, so a hub created this way is not
    /// exposed to other hosts by default. The hub has no built-in authentication unless a
    /// <see cref="ClientAuthenticator"/> is supplied, so remote exposure should be an explicit choice —
    /// use the <see cref="TcpTransportListener(IPEndPoint, SslServerAuthenticationOptions, TimeSpan?, int?)"/>
    /// constructor with the desired address for that, and give it TLS options unless the segment is
    /// already trusted.
    /// </remarks>
    /// <param name="port">The TCP port to listen on.</param>
    /// <param name="tlsOptions">
    /// TLS options to authenticate each accepted connection as the server, or <see langword="null"/>
    /// (the default) to accept cleartext connections.
    /// </param>
    /// <param name="tlsHandshakeTimeout">
    /// How long a single TLS handshake may take before the connection is abandoned. Defaults to 10
    /// seconds.
    /// </param>
    /// <param name="maxConcurrentTlsHandshakes">
    /// The maximum number of TLS handshakes to run at once. Defaults to 64.
    /// </param>
    public TcpTransportListener(
        int port,
        SslServerAuthenticationOptions? tlsOptions = null,
        TimeSpan? tlsHandshakeTimeout = null,
        int? maxConcurrentTlsHandshakes = null)
        : this(new IPEndPoint(IPAddress.Loopback, port), tlsOptions, tlsHandshakeTimeout, maxConcurrentTlsHandshakes)
    {
    }

    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_listener is not null)
        {
            throw new InvalidOperationException("The listener is already running.");
        }

        _listener = new TcpListener(_endPoint);
        _listener.Start();

        if (_tlsOptions is not null)
        {
            // Hold at most one completed handshake per concurrent handshake slot, so a slow consumer
            // exerts back-pressure through the pump rather than letting finished connections pile up.
            _handshakenTransports = Channel.CreateBounded<TcpTransport>(
                new BoundedChannelOptions(_maxConcurrentTlsHandshakes)
                {
                    SingleReader = true,
                    SingleWriter = false,
                });

            _handshakeCts = new CancellationTokenSource();
            _handshakePumpTask = HandshakePumpAsync(_listener, _handshakenTransports.Writer, _handshakeCts.Token);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task<ITransport> AcceptAsync(CancellationToken cancellationToken = default)
    {
        if (_listener is null)
        {
            throw new InvalidOperationException("The listener has not been started.");
        }

        if (_handshakenTransports is { } handshaken)
        {
            try
            {
                return await handshaken.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ChannelClosedException ex)
            {
                // The pump has stopped, either because the listener was disposed or because accepting
                // failed outright; either way this listener will never produce another connection.
                // Surface it as ObjectDisposedException, which the hub's accept loop treats as a reason
                // to stop — rethrowing the underlying error instead would be logged and retried by that
                // loop, spinning against a listener that is never coming back. The cause is preserved as
                // the inner exception.
                throw new ObjectDisposedException(
                    $"The {nameof(TcpTransportListener)} is no longer accepting connections.",
                    ex.InnerException ?? ex);
            }
        }

        TcpClient tcpClient = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            tcpClient.NoDelay = true;
            return new TcpTransport(tcpClient);
        }
        catch
        {
            // Setting NoDelay or acquiring the stream can fail if the peer reset the
            // connection immediately after it was accepted. Dispose the socket rather
            // than leaking it, then let the caller's accept loop continue.
            tcpClient.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        // Cancel first, so in-flight handshakes unwind, then stop the listener to unblock the pump's
        // pending accept.
        if (_handshakeCts is { } cts)
        {
            await cts.CancelAsync().ConfigureAwait(false);
        }

        _listener?.Stop();
        _listener = null;

        if (_handshakePumpTask is { } pumpTask)
        {
            // The pump never faults — it completes the channel with any error instead — so awaiting it
            // cannot throw here.
            await pumpTask.ConfigureAwait(false);
            _handshakePumpTask = null;
        }

        _handshakeCts?.Dispose();
        _handshakeCts = null;

        if (_handshakenTransports is { } handshaken)
        {
            // Connections that finished their handshake but were never accepted are owned by nobody else;
            // close them rather than leaking the sockets.
            while (handshaken.Reader.TryRead(out TcpTransport? pending))
            {
                await pending.DisposeAsync().ConfigureAwait(false);
            }

            _handshakenTransports = null;
        }
    }

    /// <summary>
    /// Copies every setting from the caller's options so a later mutation of their instance cannot change
    /// how this listener authenticates.
    /// </summary>
    /// <remarks>
    /// Every settable property must be copied. A property left out here silently discards the caller's
    /// intent, which for a security setting means quietly weakening every accepted connection — so this
    /// is covered by a reflection test that fails when the framework type gains a property this does not
    /// handle.
    /// </remarks>
    internal static SslServerAuthenticationOptions CloneServerOptions(SslServerAuthenticationOptions source)
    {
        var clone = new SslServerAuthenticationOptions
        {
            AllowRenegotiation = source.AllowRenegotiation,
            AllowTlsResume = source.AllowTlsResume,
            ApplicationProtocols = source.ApplicationProtocols,
            CertificateChainPolicy = source.CertificateChainPolicy,
            CertificateRevocationCheckMode = source.CertificateRevocationCheckMode,
            CipherSuitesPolicy = source.CipherSuitesPolicy,
            ClientCertificateRequired = source.ClientCertificateRequired,
            EnabledSslProtocols = source.EnabledSslProtocols,
            EncryptionPolicy = source.EncryptionPolicy,
            RemoteCertificateValidationCallback = source.RemoteCertificateValidationCallback,
            ServerCertificate = source.ServerCertificate,
            ServerCertificateContext = source.ServerCertificateContext,
            ServerCertificateSelectionCallback = source.ServerCertificateSelectionCallback,
        };

        // The RSA padding switches only exist on Linux and Windows; reading them elsewhere throws. They
        // still have to be carried across where they do exist, since a caller that turned off PKCS#1 v1.5
        // padding must not have it silently restored by the copy.
        if (OperatingSystem.IsLinux() || OperatingSystem.IsWindows())
        {
            clone.AllowRsaPkcs1Padding = source.AllowRsaPkcs1Padding;
            clone.AllowRsaPssPadding = source.AllowRsaPssPadding;
        }

        return clone;
    }

    /// <summary>
    /// Accepts sockets and negotiates each one's TLS handshake off the accept path, publishing the
    /// successfully authenticated transports for <see cref="AcceptAsync"/> to return.
    /// </summary>
    /// <remarks>
    /// Running the handshake here rather than inside <see cref="AcceptAsync"/> is what stops one slow or
    /// hostile peer serialising behind itself every other client waiting to connect: the hub's accept loop
    /// consumes one connection at a time, so an inline handshake would be head-of-line blocking on
    /// unauthenticated input.
    /// <para>
    /// The accept itself is never gated on a handshake bound. Waiting for a free handshake slot before
    /// accepting would hand an attacker the whole listener: a few dozen peers that connect and then send
    /// nothing would hold every slot until their timeout, and the loop would stop accepting entirely.
    /// Admission is instead capped by a much larger pending bound that is polled, never waited on, so a
    /// flood is shed as refused connections while the loop keeps draining the backlog.
    /// </para>
    /// </remarks>
    private async Task HandshakePumpAsync(
        TcpListener listener,
        ChannelWriter<TcpTransport> writer,
        CancellationToken cancellationToken)
    {
        // Yield so StartAsync returns before the first accept is issued, keeping it non-blocking.
        await Task.Yield();

        using var handshakeSlots = new SemaphoreSlim(_maxConcurrentTlsHandshakes, _maxConcurrentTlsHandshakes);
        using var pendingSlots = new SemaphoreSlim(_maxPendingTlsHandshakes, _maxPendingTlsHandshakes);
        var inFlight = new ConcurrentDictionary<Task, byte>();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient tcpClient;
                try
                {
                    tcpClient = await listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (SocketException)
                {
                    // A single accept failing — a peer resetting between the SYN and the accept, an
                    // interrupted call, or a transient descriptor shortage — must not end the listener for
                    // good. The cleartext path recovers from this because the hub's own accept loop logs
                    // and continues; the pump has to recover for itself. Pause briefly so a persistent
                    // failure such as descriptor exhaustion cannot spin this loop hot.
                    await Task.Delay(AcceptRetryDelay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // Poll, never wait: a full pending set means shedding this connection immediately is far
                // better than parking the accept loop, which is precisely the failure being avoided.
                if (!pendingSlots.Wait(0, CancellationToken.None))
                {
                    tcpClient.Dispose();
                    continue;
                }

                Task handshakeTask = HandshakeAsync(
                    tcpClient, writer, handshakeSlots, pendingSlots, cancellationToken);
                inFlight.TryAdd(handshakeTask, 0);
                _ = handshakeTask.ContinueWith(t => inFlight.TryRemove(t, out _), TaskScheduler.Default);
            }

            writer.TryComplete();
        }
        catch (OperationCanceledException)
        {
            writer.TryComplete();
        }
        catch (ObjectDisposedException)
        {
            // The listener was stopped underneath the pending accept.
            writer.TryComplete();
        }
        catch (Exception ex)
        {
            // Whatever went wrong, the channel must be completed: a reader blocked in AcceptAsync would
            // otherwise wait for a pump that is never coming back. This is a background loop with no
            // logger available, so completing the channel with the error is how the cause reaches the
            // caller. Catching broadly here is deliberate, and mirrors the hub's own accept loop.
            writer.TryComplete(ex);
        }
        finally
        {
            // Wait for the outstanding handshakes so the semaphores are not disposed while they still hold
            // slots, and so DisposeAsync does not return with sockets still being negotiated. HandshakeAsync
            // never faults — it swallows and disposes — so this cannot throw. No handshake is added after
            // the loop exits, so the snapshot is complete.
            await Task.WhenAll(inFlight.Keys).ConfigureAwait(false);
        }
    }

    private async Task HandshakeAsync(
        TcpClient tcpClient,
        ChannelWriter<TcpTransport> writer,
        SemaphoreSlim handshakeSlots,
        SemaphoreSlim pendingSlots,
        CancellationToken cancellationToken)
    {
        TcpTransport? transport = null;
        try
        {
            tcpClient.NoDelay = true;

            NetworkStream networkStream = tcpClient.GetStream();
            var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false);
            transport = new TcpTransport(tcpClient, sslStream);

            // Bound the whole negotiation: an unauthenticated peer must not be able to hold a connection
            // open for as long as it likes by connecting and then going quiet.
            using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            handshakeCts.CancelAfter(_tlsHandshakeTimeout);

            // Wait for the peer to actually send something before spending a handshake slot on it. A
            // zero-byte read consumes nothing and completes only once data has arrived, so a peer that
            // never sends a ClientHello waits out its timeout without ever occupying one of the slots that
            // bound handshake CPU — which is what keeps a flood of silent peers from starving genuine
            // clients of the ability to negotiate.
            await networkStream.ReadAsync(Memory<byte>.Empty, handshakeCts.Token).ConfigureAwait(false);

            await handshakeSlots.WaitAsync(handshakeCts.Token).ConfigureAwait(false);
            try
            {
                await sslStream.AuthenticateAsServerAsync(_tlsOptions!, handshakeCts.Token)
                    .ConfigureAwait(false);
            }
            finally
            {
                handshakeSlots.Release();
            }

            await writer.WriteAsync(transport, cancellationToken).ConfigureAwait(false);
            transport = null;
        }
        catch
        {
            // A failed handshake — an untrusted or absent client certificate, a protocol mismatch, a
            // timeout, a peer that reset — concerns only this connection. Drop it quietly and keep the
            // listener serving; anything else would let one bad peer stop the hub. There is no logger on
            // the transport layer, so the hub sees this as a connection that never arrived.
            if (transport is not null)
            {
                await transport.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                tcpClient.Dispose();
            }
        }
        finally
        {
            pendingSlots.Release();
        }
    }
}
