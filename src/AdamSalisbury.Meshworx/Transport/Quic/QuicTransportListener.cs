using System.Collections.Concurrent;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.Transport.Quic;

/// <summary>
/// An <see cref="ITransportListener"/> implementation that accepts incoming QUIC connections and, for
/// each one, the single bidirectional stream Meshworx uses to back an <see cref="ITransport"/>.
/// </summary>
/// <remarks>
/// QUIC mandates TLS at the protocol level, so — unlike <see cref="TcpTransportListener"/> — TLS options
/// are required rather than optional here. Requires <see cref="QuicListener.IsSupported"/> to be
/// <see langword="true"/>; on Linux this typically means the native <c>libmsquic</c> package is
/// installed.
/// <para>
/// Accepting a connection is not the same as it being ready to use: the QUIC handshake itself completes
/// inside <see cref="QuicListener.AcceptConnectionAsync"/> (msquic handles amplification limiting and
/// retry internally, the way TLS 1.3 0-RTT/1-RTT setup requires), but the client's first stream can still
/// arrive — or never arrive — after that. Waiting for that stream runs off the accept path, the same
/// shape as <see cref="AdamSalisbury.Meshworx.Transport.WebSocket.WebSocketTransportListener"/> waiting
/// for the HTTP upgrade request and <see cref="TcpTransportListener"/> waiting for a TLS handshake, so a
/// connected-but-silent peer cannot head-of-line block every other client's own accept call. Unlike
/// those two, though, there is no cheap way here to tell a peer that will eventually open a stream apart
/// from one that never will before actually waiting for it — see the <c>maxConcurrentNegotiations</c>
/// constructor parameter for what that means for a flood of such peers.
/// </para>
/// </remarks>
public sealed class QuicTransportListener : ITransportListener
{
    private static readonly TimeSpan DefaultStreamOpenTimeout = TimeSpan.FromSeconds(10);
    private const int DefaultMaxConcurrentNegotiations = 64;

    private static readonly TimeSpan AcceptRetryDelay = TimeSpan.FromMilliseconds(50);

    // The network-prefix length an IPv6 address is masked to before it keys the per-source negotiation
    // cap — the same reasoning and the same prefix length as MeshHub's own per-remote-endpoint cap: a
    // single host is routinely handed an entire /64 (or larger) allocation, so keying on the full
    // address would let one attacker defeat the cap just by rotating addresses within it.
    private const int IPv6CapPrefixLength = 64;

    private readonly IPEndPoint _endPoint;
    private readonly SslServerAuthenticationOptions _tlsOptions;
    private readonly List<SslApplicationProtocol> _applicationProtocols;
    private readonly TimeSpan _streamOpenTimeout;
    private readonly int _maxConcurrentNegotiations;
    private readonly int _maxConcurrentNegotiationsPerSource;

    // Guards every mutable field below, following the same discipline as TcpTransportListener: each
    // caller takes the state it needs under the lock and then works from locals, and nothing that
    // blocks or awaits runs while holding it.
    private readonly Lock _stateLock = new();

    private QuicListener? _listener;
    private Channel<QuicTransport>? _negotiatedTransports;

    /// <summary>
    /// The address and port actually bound, once <see cref="StartAsync"/> has completed — useful when
    /// constructed with an ephemeral port (0) to discover which one was assigned.
    /// </summary>
    internal EndPoint? LocalEndPoint
    {
        get
        {
            lock (_stateLock)
            {
#pragma warning disable CA1416 // Windows/Linux/macOS-only API: only non-null once StartAsync has already confirmed QuicListener.IsSupported.
                return _listener?.LocalEndPoint;
#pragma warning restore CA1416
            }
        }
    }

    private CancellationTokenSource? _negotiationCts;
    private Task? _negotiationPumpTask;
    private Task? _disposeTask;
    private volatile bool _disposed;

    // Claims the right to proceed past the "already running" check while QuicListener.ListenAsync is
    // in flight. Unlike every other listener in this library, that call is itself the async bind step —
    // there is no synchronous constructor to serialise concurrent starts around — so without this flag
    // two overlapping StartAsync calls could both pass the check, both genuinely bind a QuicListener,
    // and the second to publish would silently overwrite the first's fields: leaking a bound listener
    // and orphaning its negotiation pump task rather than the second call failing the way it should.
    // Always read and written under _stateLock, mirroring MeshHub.StartAsync's identical pattern.
    private bool _starting;

    /// <summary>
    /// Creates a listener bound to the given endpoint.
    /// </summary>
    /// <param name="endPoint">The endpoint to bind to.</param>
    /// <param name="tlsOptions">
    /// TLS options to authenticate accepted connections as the server. Required — QUIC mandates TLS,
    /// unlike the optional TLS on <see cref="TcpTransportListener"/>. The options are copied, so later
    /// mutation of the caller's instance does not affect this listener. If
    /// <see cref="SslServerAuthenticationOptions.ApplicationProtocols"/> is left unset it defaults to
    /// <see cref="QuicTransport.DefaultApplicationProtocol"/>, which a connecting
    /// <see cref="QuicTransport.ConnectAsync"/> caller must then also leave unset (or match explicitly).
    /// </param>
    /// <param name="streamOpenTimeout">
    /// How long a connected peer has to open its first stream before the connection is abandoned.
    /// Bounds how long an accepted-but-silent connection can occupy a negotiation slot. Defaults to 10
    /// seconds.
    /// </param>
    /// <param name="maxConcurrentNegotiations">
    /// The maximum number of connections waiting for their first stream at once. A connection beyond
    /// this limit is refused immediately rather than queued — unlike <see cref="TcpTransportListener"/>'s
    /// TLS handshake pump, there is no cheap way to tell a connection that will eventually open a stream
    /// apart from one that never will, so a flood of connect-and-never-send peers up to this limit will
    /// genuinely occupy every slot for up to <paramref name="streamOpenTimeout"/>. Defaults to 64.
    /// </param>
    /// <param name="maxConcurrentNegotiationsPerSource">
    /// The maximum number of connections from a single source address (an IPv6 source is masked to its
    /// /64 network prefix first, exactly as <c>MeshHub</c>'s own per-remote-endpoint cap does) that may
    /// be waiting for their first stream at once. Without this, one source completing real — not
    /// spoofed — QUIC handshakes and never opening a stream on any of them could occupy the entire
    /// <paramref name="maxConcurrentNegotiations"/> pool by itself, since a QUIC handshake carries no
    /// cheap "has this peer sent anything" signal to gate on the way <see cref="TcpTransportListener"/>'s
    /// TLS pump has. Defaults to one eighth of <paramref name="maxConcurrentNegotiations"/> (minimum 1).
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="endPoint"/> or <paramref name="tlsOptions"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="tlsOptions"/> was supplied with neither a
    /// <see cref="SslServerAuthenticationOptions.ServerCertificate"/> nor a
    /// <see cref="SslServerAuthenticationOptions.ServerCertificateContext"/> nor a
    /// <see cref="SslServerAuthenticationOptions.ServerCertificateSelectionCallback"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="streamOpenTimeout"/>, <paramref name="maxConcurrentNegotiations"/>, or
    /// <paramref name="maxConcurrentNegotiationsPerSource"/> is not positive.
    /// </exception>
    public QuicTransportListener(
        IPEndPoint endPoint,
        SslServerAuthenticationOptions tlsOptions,
        TimeSpan? streamOpenTimeout = null,
        int? maxConcurrentNegotiations = null,
        int? maxConcurrentNegotiationsPerSource = null)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        ArgumentNullException.ThrowIfNull(tlsOptions);

        if (streamOpenTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(streamOpenTimeout), "The stream-open timeout must be positive.");
        }

        if (maxConcurrentNegotiations is { } maxNegotiations && maxNegotiations <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConcurrentNegotiations), "The maximum concurrent negotiation count must be positive.");
        }

        if (maxConcurrentNegotiationsPerSource is { } maxPerSource && maxPerSource <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConcurrentNegotiationsPerSource),
                "The maximum concurrent per-source negotiation count must be positive.");
        }

        if (tlsOptions.ServerCertificate is null
            && tlsOptions.ServerCertificateContext is null
            && tlsOptions.ServerCertificateSelectionCallback is null)
        {
            throw new ArgumentException(
                "The TLS options must supply a server certificate, a certificate context, or a certificate "
                    + "selection callback — QUIC requires TLS, so this cannot be left unset.",
                nameof(tlsOptions));
        }

        _endPoint = endPoint;
        _tlsOptions = TcpTransportListener.CloneServerOptions(tlsOptions);
        _applicationProtocols = _tlsOptions.ApplicationProtocols is { Count: > 0 } protocols
            ? protocols
            : [QuicTransport.DefaultApplicationProtocol];
        _tlsOptions.ApplicationProtocols = _applicationProtocols;
        _streamOpenTimeout = streamOpenTimeout ?? DefaultStreamOpenTimeout;
        _maxConcurrentNegotiations = maxConcurrentNegotiations ?? DefaultMaxConcurrentNegotiations;
        _maxConcurrentNegotiationsPerSource =
            maxConcurrentNegotiationsPerSource ?? Math.Max(1, _maxConcurrentNegotiations / 8);
    }

    /// <summary>
    /// Creates a listener bound to the loopback interface on the given port.
    /// </summary>
    /// <remarks>
    /// Binds to <see cref="IPAddress.Loopback"/>, not every interface, so a hub created this way is not
    /// exposed to other hosts by default. Use the
    /// <see cref="QuicTransportListener(IPEndPoint, SslServerAuthenticationOptions, TimeSpan?, int?, int?)"/>
    /// constructor with the desired address to listen more broadly.
    /// </remarks>
    /// <param name="port">The UDP port to listen on.</param>
    /// <param name="tlsOptions">TLS options to authenticate accepted connections as the server. Required.</param>
    /// <param name="streamOpenTimeout">How long a connected peer has to open its first stream. Defaults to 10 seconds.</param>
    /// <param name="maxConcurrentNegotiations">The maximum number of connections negotiating at once. Defaults to 64.</param>
    /// <param name="maxConcurrentNegotiationsPerSource">
    /// The maximum number of connections from a single source address negotiating at once. Defaults to
    /// one eighth of <paramref name="maxConcurrentNegotiations"/> (minimum 1).
    /// </param>
    public QuicTransportListener(
        int port,
        SslServerAuthenticationOptions tlsOptions,
        TimeSpan? streamOpenTimeout = null,
        int? maxConcurrentNegotiations = null,
        int? maxConcurrentNegotiationsPerSource = null)
        : this(
            new IPEndPoint(IPAddress.Loopback, port),
            tlsOptions,
            streamOpenTimeout,
            maxConcurrentNegotiations,
            maxConcurrentNegotiationsPerSource)
    {
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">
    /// The listener has been disposed, including while this call was itself in flight.
    /// </exception>
    /// <exception cref="PlatformNotSupportedException"><see cref="QuicListener.IsSupported"/> is <see langword="false"/>.</exception>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_negotiationCts is not null || _starting)
            {
                throw new InvalidOperationException("The listener is already running.");
            }

            // Claim the running slot with a flag rather than by publishing state early. QuicListener's
            // async-only bind means a second concurrent StartAsync could otherwise pass the check above
            // too, genuinely bind a second QuicListener, and overwrite this call's fields once it
            // publishes — leaking the first listener and orphaning its negotiation pump task instead of
            // the second call failing the way it should.
            _starting = true;
        }

        QuicListener listener;
        try
        {
            if (!QuicListener.IsSupported)
            {
                throw new PlatformNotSupportedException(
                    "QUIC is not supported on this platform. This typically means the native msquic "
                        + "library is not installed (on Debian/Ubuntu: 'apt install libmsquic'), or the "
                        + "platform's TLS stack does not support TLS 1.3.");
            }

            // Windows/Linux/macOS-only APIs from here down to the ListenAsync call: guarded at run time
            // by the QuicListener.IsSupported check above.
#pragma warning disable CA1416
            var options = new QuicListenerOptions
            {
                ListenEndPoint = _endPoint,
                ApplicationProtocols = _applicationProtocols,
                ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(
                    new QuicServerConnectionOptions
                    {
                        ServerAuthenticationOptions = _tlsOptions,

                        // The framework's own default (-1) is rejected by validation; Meshworx never
                        // aborts a stream or connection with an application-defined error code, so 0 is
                        // simply "none".
                        DefaultStreamErrorCode = 0,
                        DefaultCloseErrorCode = 0,
                    }),
            };

            listener = await QuicListener.ListenAsync(options, cancellationToken).ConfigureAwait(false);
#pragma warning restore CA1416
        }
        catch
        {
            // Release the claim, so a listener that failed to bind is startable again rather than
            // permanently reporting itself as already running. Nothing else has seen any state from
            // this attempt, so releasing here cannot race a concurrent DisposeAsync.
            lock (_stateLock)
            {
                _starting = false;
            }

            throw;
        }

        lock (_stateLock)
        {
            _starting = false;

            if (!_disposed)
            {
                var negotiatedTransports = Channel.CreateBounded<QuicTransport>(
                    new BoundedChannelOptions(_maxConcurrentNegotiations)
                    {
                        SingleReader = true,
                        SingleWriter = false,
                    });

                var negotiationCts = new CancellationTokenSource();

                _listener = listener;
                _negotiatedTransports = negotiatedTransports;
                _negotiationCts = negotiationCts;

                // Started on the thread pool rather than called directly: an async method runs
                // synchronously up to its first await on the calling thread, and that first await here has
                // no ConfigureAwait to fall back on — YieldAwaitable posts its continuation to
                // SynchronizationContext.Current when one exists, so a caller starting the listener from a
                // UI thread would silently strand the pump's continuation on that thread's message pump.
                // Task.Run sidesteps the problem entirely: the whole pump, including its synchronous
                // prefix, runs on a thread-pool thread with no SynchronizationContext to capture. Task.Run's
                // own cancellation parameter is deliberately CancellationToken.None, not
                // negotiationCts.Token, even though that token is what the pump itself observes: passing it
                // to Task.Run risks the work item never running at all if DisposeAsync cancels the token
                // before the thread pool gets to it, which would surface as the awaited task being Canceled
                // rather than completed — breaking DisposeCoreAsync's documented assumption that awaiting
                // this task cannot throw, since the pump's own try/catch never got to run.
                _negotiationPumpTask = Task.Run(
                    () => NegotiationPumpAsync(listener, negotiatedTransports.Writer, negotiationCts.Token),
                    CancellationToken.None);
                return;
            }
        }

        // Disposed while this start was in flight. Nothing else owns the listener created above.
#pragma warning disable CA1416
        await listener.DisposeAsync().ConfigureAwait(false);
#pragma warning restore CA1416
        throw new ObjectDisposedException(nameof(QuicTransportListener));
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">
    /// The listener has been disposed, or was disposed while this accept was pending.
    /// </exception>
    public async Task<ITransport> AcceptAsync(CancellationToken cancellationToken = default)
    {
        Channel<QuicTransport> negotiatedTransports;

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            negotiatedTransports = _negotiatedTransports
                ?? throw new InvalidOperationException("The listener has not been started.");
        }

        try
        {
            return await negotiatedTransports.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException ex)
        {
            throw new ObjectDisposedException(
                $"The {nameof(QuicTransportListener)} is no longer accepting connections.",
                ex.InnerException ?? ex);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Safe to call more than once, and from more than one thread at a time. Only the first call tears
    /// the listener down; every call — first or not — returns only once that teardown is complete.
    /// </remarks>
    public ValueTask DisposeAsync()
    {
        Task disposal;

        lock (_stateLock)
        {
            if (_disposeTask is null)
            {
                _disposed = true;

                QuicListener? listener = _listener;
                CancellationTokenSource? negotiationCts = _negotiationCts;
                Task? negotiationPumpTask = _negotiationPumpTask;
                Channel<QuicTransport>? negotiatedTransports = _negotiatedTransports;

                _listener = null;
                _negotiatedTransports = null;
                _negotiationCts = null;
                _negotiationPumpTask = null;

                _disposeTask = DisposeCoreAsync(listener, negotiationCts, negotiationPumpTask, negotiatedTransports);
            }

            disposal = _disposeTask;
        }

        return new ValueTask(disposal);
    }

    private static async Task DisposeCoreAsync(
        QuicListener? listener,
        CancellationTokenSource? negotiationCts,
        Task? negotiationPumpTask,
        Channel<QuicTransport>? negotiatedTransports)
    {
        if (negotiationCts is not null)
        {
            await negotiationCts.CancelAsync().ConfigureAwait(false);
        }

        if (listener is not null)
        {
#pragma warning disable CA1416 // Windows/Linux/macOS-only API: only reachable once StartAsync has already confirmed QuicListener.IsSupported.
            await listener.DisposeAsync().ConfigureAwait(false);
#pragma warning restore CA1416
        }

        if (negotiationPumpTask is not null)
        {
            // The pump never faults — it completes the channel with any error instead — so awaiting it
            // cannot throw here.
            await negotiationPumpTask.ConfigureAwait(false);
        }

        negotiationCts?.Dispose();

        if (negotiatedTransports is not null)
        {
            // Connections that finished negotiation but were never accepted are owned by nobody else;
            // close them rather than leaking them.
            while (negotiatedTransports.Reader.TryRead(out QuicTransport? pending))
            {
                await pending.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Accepts connections and waits for each one's first stream off the accept path, publishing the
    /// successfully negotiated transports for <see cref="AcceptAsync"/> to return.
    /// </summary>
    /// <remarks>
    /// The QUIC handshake itself — including TLS 1.3 and msquic's own amplification-limiting and retry
    /// handling — completes inside <see cref="QuicListener.AcceptConnectionAsync"/> before it ever
    /// returns a connection, so unlike <see cref="TcpTransportListener"/>'s TLS pump there is no separate
    /// handshake step to run off this path, and no CPU-expensive work left to bound. What is bounded here
    /// is simply how many connections may be concurrently waiting for their first stream: a single
    /// <see cref="SemaphoreSlim"/> admits up to <see cref="_maxConcurrentNegotiations"/> at once, and a
    /// connection that finds it full is dropped immediately rather than queued.
    /// <para>
    /// Unlike <see cref="TcpTransportListener"/>'s handshake pump or
    /// <see cref="AdamSalisbury.Meshworx.Transport.WebSocket.WebSocketTransportListener"/>'s negotiation
    /// pump, there is no cheap way to tell a connection that will eventually open a stream apart from one
    /// that never will before actually waiting for it — <see cref="QuicConnection.AcceptInboundStreamAsync"/>
    /// is the only detection mechanism there is. A flood of connect-and-never-open-a-stream peers up to
    /// this limit will therefore genuinely occupy every slot for up to <see cref="_streamOpenTimeout"/>,
    /// during which a further connection is shed rather than delayed — this listener cannot distinguish
    /// the two cases the way the TLS-handshake pumps can distinguish "sent nothing yet" from "consuming a
    /// handshake slot". Size <see cref="_maxConcurrentNegotiations"/> and <see cref="_streamOpenTimeout"/>
    /// for the deployment's real concurrent-connection expectations with this in mind. A per-source cap
    /// (<see cref="_maxConcurrentNegotiationsPerSource"/>) is applied first, precisely because that
    /// global pool has no cheap pre-check to protect it: without it, a single source completing real
    /// handshakes and never opening a stream could occupy the whole pool alone.
    /// </para>
    /// </remarks>
    private async Task NegotiationPumpAsync(
        QuicListener listener,
        ChannelWriter<QuicTransport> writer,
        CancellationToken cancellationToken)
    {
        // Started via Task.Run at the call site, which is what keeps StartAsync non-blocking — nothing
        // here needs to yield first.
        using var negotiationSlots = new SemaphoreSlim(_maxConcurrentNegotiations, _maxConcurrentNegotiations);
        var negotiationsPerSource = new ConcurrentDictionary<IPAddress, int>();
        var inFlight = new ConcurrentDictionary<Task, byte>();

        void ShedInBackground(QuicConnection connectionToShed)
        {
            // Disposing a fully-established QUIC connection is not free (it tears down the TLS 1.3
            // session and the underlying msquic connection object), so this runs off the accept loop
            // via the same inFlight tracking a genuine negotiation uses, rather than being awaited
            // inline — an inline await here would serialise that cost onto the one loop that also has
            // to keep accepting everyone else.
#pragma warning disable CA1416
            Task disposeTask = connectionToShed.DisposeAsync().AsTask();
#pragma warning restore CA1416
            inFlight.TryAdd(disposeTask, 0);
            _ = disposeTask.ContinueWith(t => inFlight.TryRemove(t, out _), TaskScheduler.Default);
        }

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                QuicConnection connection;
                try
                {
#pragma warning disable CA1416 // Windows/Linux/macOS-only API: only reachable once StartAsync has already confirmed QuicListener.IsSupported.
                    connection = await listener.AcceptConnectionAsync(cancellationToken).ConfigureAwait(false);
#pragma warning restore CA1416
                }
                catch (QuicException)
                {
                    // A single connection attempt failing — a malformed initial packet, a peer that
                    // reset mid-handshake — must not end the listener for good. Pause briefly so a
                    // persistent failure cannot spin this loop hot.
                    await Task.Delay(AcceptRetryDelay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

#pragma warning disable CA1416
                IPAddress? sourceAddress = connection.RemoteEndPoint is IPEndPoint remoteEndPoint
                    ? NormaliseForSourceCap(remoteEndPoint.Address)
                    : null;
#pragma warning restore CA1416

                // Checked before the global pool, and independently of it: this is what actually bounds
                // how much of that pool a single source can hold, since the pool itself has no cheap way
                // to tell a genuine peer from one that will never send anything.
                if (sourceAddress is not null && !TryAdmitSource(negotiationsPerSource, sourceAddress, _maxConcurrentNegotiationsPerSource))
                {
                    ShedInBackground(connection);
                    continue;
                }

                if (!negotiationSlots.Wait(0, CancellationToken.None))
                {
                    if (sourceAddress is not null)
                    {
                        ReleaseSource(negotiationsPerSource, sourceAddress);
                    }

                    ShedInBackground(connection);
                    continue;
                }

                Task negotiationTask = NegotiateAsync(
                    connection, writer, negotiationSlots, negotiationsPerSource, sourceAddress, cancellationToken);
                inFlight.TryAdd(negotiationTask, 0);
                _ = negotiationTask.ContinueWith(t => inFlight.TryRemove(t, out _), TaskScheduler.Default);
            }

            writer.TryComplete();
        }
        catch (OperationCanceledException)
        {
            writer.TryComplete();
        }
        catch (ObjectDisposedException)
        {
            writer.TryComplete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
        finally
        {
            await Task.WhenAll(inFlight.Keys).ConfigureAwait(false);
        }
    }

    private async Task NegotiateAsync(
        QuicConnection connection,
        ChannelWriter<QuicTransport> writer,
        SemaphoreSlim negotiationSlots,
        ConcurrentDictionary<IPAddress, int> negotiationsPerSource,
        IPAddress? sourceAddress,
        CancellationToken cancellationToken)
    {
        QuicTransport? transport = null;
        try
        {
            using var negotiationCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            negotiationCts.CancelAfter(_streamOpenTimeout);

#pragma warning disable CA1416
            QuicStream stream = await connection.AcceptInboundStreamAsync(negotiationCts.Token).ConfigureAwait(false);
#pragma warning restore CA1416
            transport = new QuicTransport(connection, stream);

            await writer.WriteAsync(transport, cancellationToken).ConfigureAwait(false);
            transport = null;
        }
        catch
        {
            // A failed negotiation — the peer never opened a stream, a timeout, a reset — concerns only
            // this connection. Drop it quietly and keep the listener serving; anything else would let
            // one bad peer stop the hub.
            if (transport is not null)
            {
                await transport.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
#pragma warning disable CA1416
                await connection.DisposeAsync().ConfigureAwait(false);
#pragma warning restore CA1416
            }
        }
        finally
        {
            negotiationSlots.Release();

            if (sourceAddress is not null)
            {
                ReleaseSource(negotiationsPerSource, sourceAddress);
            }
        }
    }

    /// <summary>
    /// Attempts to claim one of <paramref name="maxPerSource"/> negotiation slots reserved for
    /// <paramref name="address"/>, without blocking and without exceeding the cap even under
    /// concurrent callers for the same address.
    /// </summary>
    internal static bool TryAdmitSource(ConcurrentDictionary<IPAddress, int> perSourceCounts, IPAddress address, int maxPerSource)
    {
        while (true)
        {
            int current = perSourceCounts.GetOrAdd(address, 0);
            if (current >= maxPerSource)
            {
                return false;
            }

            if (perSourceCounts.TryUpdate(address, current + 1, current))
            {
                return true;
            }

            // Another caller updated the count for this address between the read and the write above;
            // retry against the now-current value rather than risk under- or over-counting.
        }
    }

    /// <summary>
    /// Gives back a negotiation slot claimed by <see cref="TryAdmitSource"/>, removing the address's
    /// entry entirely once its count reaches zero so the dictionary is bounded by the number of sources
    /// currently negotiating, not every source ever seen.
    /// </summary>
    internal static void ReleaseSource(ConcurrentDictionary<IPAddress, int> perSourceCounts, IPAddress address)
    {
        while (true)
        {
            if (!perSourceCounts.TryGetValue(address, out int current))
            {
                // Should not happen — every admit has a matching release — but a cleanup path must
                // never throw over bookkeeping.
                return;
            }

            int updated = current - 1;
            if (updated <= 0)
            {
                if (perSourceCounts.TryRemove(new KeyValuePair<IPAddress, int>(address, current)))
                {
                    return;
                }
            }
            else if (perSourceCounts.TryUpdate(address, updated, current))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Reduces an address to the key the per-source negotiation cap treats it as coming from — the
    /// same normalisation <c>MeshHub</c> applies for its own per-remote-endpoint cap, duplicated here
    /// rather than shared across the layer boundary between the transport and hub assemblies.
    /// </summary>
    internal static IPAddress NormaliseForSourceCap(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetworkV6)
        {
            return address;
        }

        Span<byte> addressBytes = stackalloc byte[16];
        address.TryWriteBytes(addressBytes, out _);

        // Zero everything past the /64 network prefix (the low 8 bytes, the interface identifier), so
        // addresses that differ only there key to the same masked address.
        addressBytes[(IPv6CapPrefixLength / 8)..].Clear();

        return new IPAddress(addressBytes);
    }
}
