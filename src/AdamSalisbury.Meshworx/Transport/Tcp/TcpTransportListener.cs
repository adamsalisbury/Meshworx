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

    private readonly IPEndPoint _endPoint;
    private readonly SslServerAuthenticationOptions? _tlsOptions;
    private readonly TimeSpan _tlsHandshakeTimeout;
    private readonly int _maxConcurrentTlsHandshakes;

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
    /// unauthenticated input, so this bounds the CPU a connection flood can demand; further connections
    /// wait in the socket backlog. Defaults to 64. Ignored without <paramref name="tlsOptions"/>.
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
            catch (ChannelClosedException)
            {
                // The pump stopped because the listener was disposed. Surface it the way a disposed
                // listener does, so the hub's accept loop ends rather than spinning.
                throw new ObjectDisposedException(nameof(TcpTransportListener));
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

    private static SslServerAuthenticationOptions CloneServerOptions(SslServerAuthenticationOptions source)
    {
        return new SslServerAuthenticationOptions
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
    }

    /// <summary>
    /// Accepts sockets and hands each to a bounded pool of concurrent TLS handshakes, publishing the
    /// successfully authenticated transports for <see cref="AcceptAsync"/> to return.
    /// </summary>
    /// <remarks>
    /// Running the handshake here rather than inside <see cref="AcceptAsync"/> is what stops one slow or
    /// hostile peer serialising behind itself every other client waiting to connect: the hub's accept loop
    /// consumes one connection at a time, so an inline handshake would be head-of-line blocking on
    /// unauthenticated input.
    /// </remarks>
    private async Task HandshakePumpAsync(
        TcpListener listener,
        ChannelWriter<TcpTransport> writer,
        CancellationToken cancellationToken)
    {
        // Yield so StartAsync returns before the first accept is issued, keeping it non-blocking.
        await Task.Yield();

        using var slots = new SemaphoreSlim(_maxConcurrentTlsHandshakes, _maxConcurrentTlsHandshakes);
        var inFlight = new ConcurrentDictionary<Task, byte>();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Take a slot before accepting, so saturation leaves connections queued in the socket
                // backlog — where the kernel bounds them — instead of in our own memory.
                await slots.WaitAsync(cancellationToken).ConfigureAwait(false);

                TcpClient tcpClient;
                try
                {
                    tcpClient = await listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    slots.Release();
                    throw;
                }

                Task handshakeTask = HandshakeAsync(tcpClient, writer, slots, cancellationToken);
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
        catch (SocketException ex)
        {
            writer.TryComplete(ex);
        }
        finally
        {
            // Wait for the outstanding handshakes so the semaphore is not disposed while they still hold
            // slots, and so DisposeAsync does not return with sockets still being negotiated. HandshakeAsync
            // never faults — it swallows and disposes — so this cannot throw. No handshake is added after
            // the loop exits, so the snapshot is complete.
            await Task.WhenAll(inFlight.Keys).ConfigureAwait(false);
        }
    }

    private async Task HandshakeAsync(
        TcpClient tcpClient,
        ChannelWriter<TcpTransport> writer,
        SemaphoreSlim slots,
        CancellationToken cancellationToken)
    {
        TcpTransport? transport = null;
        try
        {
            tcpClient.NoDelay = true;

            var sslStream = new SslStream(tcpClient.GetStream(), leaveInnerStreamOpen: false);
            transport = new TcpTransport(tcpClient, sslStream);

            // Bound the handshake: an unauthenticated peer must not be able to occupy a slot for as long
            // as it likes by opening a connection and then going quiet.
            using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            handshakeCts.CancelAfter(_tlsHandshakeTimeout);

            await sslStream.AuthenticateAsServerAsync(_tlsOptions!, handshakeCts.Token).ConfigureAwait(false);

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
            slots.Release();
        }
    }
}
