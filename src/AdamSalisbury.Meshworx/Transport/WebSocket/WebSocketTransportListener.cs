using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Transport.Tcp;
using SystemWebSocket = System.Net.WebSockets.WebSocket;

namespace AdamSalisbury.Meshworx.Transport.WebSocket;

/// <summary>
/// An <see cref="ITransportListener"/> implementation that accepts incoming WebSocket connections,
/// negotiating the HTTP upgrade handshake — and, if configured, a TLS handshake first — on each one
/// before handing it to the hub.
/// </summary>
/// <remarks>
/// Without TLS options the accepted connections are cleartext (<c>ws://</c>). Supplying
/// <see cref="SslServerAuthenticationOptions"/> makes every accepted connection encrypted
/// (<c>wss://</c>); the WebSocket handshake and framing are otherwise identical either way.
/// <para>
/// Negotiation — the TLS handshake where configured, followed by parsing the HTTP upgrade request —
/// runs off the accept path, exactly as <see cref="TcpTransportListener"/> runs its TLS handshake off
/// the accept path: the hub's accept loop consumes one connection at a time, so negotiating inline
/// would let a single slow or hostile peer head-of-line block every other client waiting to connect.
/// </para>
/// </remarks>
public sealed class WebSocketTransportListener : ITransportListener
{
    private const string WebSocketAcceptMagic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private const int MaxRequestHeaderBytes = 16 * 1024;

    private static readonly TimeSpan DefaultHandshakeTimeout = TimeSpan.FromSeconds(10);
    private const int DefaultMaxConcurrentHandshakes = 64;

    // How many connections may be waiting to negotiate at once, as a multiple of the handshake
    // concurrency limit. See TcpTransportListener's PendingHandshakeMultiplier for the same reasoning:
    // this bounds memory and descriptors, not work, so it is deliberately far larger than the CPU bound.
    private const int PendingHandshakeMultiplier = 16;

    // Pause after a failed accept, so a persistent failure such as descriptor exhaustion cannot spin
    // the pump hot while still recovering promptly once the condition clears.
    private static readonly TimeSpan AcceptRetryDelay = TimeSpan.FromMilliseconds(50);

    private readonly IPEndPoint _endPoint;
    private readonly string _path;
    private readonly SslServerAuthenticationOptions? _tlsOptions;
    private readonly TimeSpan _handshakeTimeout;
    private readonly int _maxConcurrentHandshakes;
    private readonly int _maxPendingHandshakes;

    // Guards every mutable field below, following the same discipline as TcpTransportListener: each
    // caller takes the state it needs under the lock and then works from locals, and nothing that
    // blocks or awaits runs while holding it.
    private readonly Lock _stateLock = new();

    private TcpListener? _listener;
    private Channel<WebSocketTransport>? _negotiatedTransports;
    private CancellationTokenSource? _negotiationCts;
    private Task? _negotiationPumpTask;
    private Task? _disposeTask;
    private volatile bool _disposed;

    internal EndPoint? LocalEndPoint
    {
        get
        {
            lock (_stateLock)
            {
                return _listener?.LocalEndpoint;
            }
        }
    }

    /// <summary>
    /// Creates a listener bound to the given endpoint.
    /// </summary>
    /// <param name="endPoint">The endpoint to bind to.</param>
    /// <param name="path">
    /// The HTTP request path a client must upgrade on. Defaults to <c>"/"</c>. A request for any other
    /// path is refused with <c>404 Not Found</c> and the connection is closed.
    /// </param>
    /// <param name="tlsOptions">
    /// TLS options to authenticate each accepted connection as the server, or <see langword="null"/>
    /// (the default) to accept cleartext (<c>ws://</c>) connections. The options are copied, so later
    /// mutation of the caller's instance does not affect this listener.
    /// </param>
    /// <param name="handshakeTimeout">
    /// How long a single connection's negotiation — the TLS handshake where configured, plus the HTTP
    /// upgrade request — may take before the connection is abandoned. Defaults to 10 seconds.
    /// </param>
    /// <param name="maxConcurrentHandshakes">
    /// The maximum number of connections to negotiate at once. Sixteen times this value may be waiting
    /// to negotiate at any moment, beyond which new connections are refused rather than queued.
    /// Defaults to 64.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="endPoint"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="path"/> is null or empty, or <paramref name="tlsOptions"/> was supplied with
    /// neither a <see cref="SslServerAuthenticationOptions.ServerCertificate"/> nor a
    /// <see cref="SslServerAuthenticationOptions.ServerCertificateContext"/> nor a
    /// <see cref="SslServerAuthenticationOptions.ServerCertificateSelectionCallback"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="handshakeTimeout"/> or <paramref name="maxConcurrentHandshakes"/> is not
    /// positive.
    /// </exception>
    public WebSocketTransportListener(
        IPEndPoint endPoint,
        string path = "/",
        SslServerAuthenticationOptions? tlsOptions = null,
        TimeSpan? handshakeTimeout = null,
        int? maxConcurrentHandshakes = null)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        ArgumentException.ThrowIfNullOrEmpty(path);

        if (handshakeTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(handshakeTimeout), "The handshake timeout must be positive.");
        }

        if (maxConcurrentHandshakes is { } maxHandshakes && maxHandshakes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConcurrentHandshakes), "The maximum concurrent handshake count must be positive.");
        }

        if (tlsOptions is not null
            && tlsOptions.ServerCertificate is null
            && tlsOptions.ServerCertificateContext is null
            && tlsOptions.ServerCertificateSelectionCallback is null)
        {
            throw new ArgumentException(
                "The TLS options must supply a server certificate, a certificate context, or a certificate "
                    + "selection callback.",
                nameof(tlsOptions));
        }

        _endPoint = endPoint;
        _path = path.StartsWith('/') ? path : $"/{path}";
        _tlsOptions = tlsOptions is null ? null : TcpTransportListener.CloneServerOptions(tlsOptions);
        _handshakeTimeout = handshakeTimeout ?? DefaultHandshakeTimeout;
        _maxConcurrentHandshakes = maxConcurrentHandshakes ?? DefaultMaxConcurrentHandshakes;
        _maxPendingHandshakes = _maxConcurrentHandshakes * PendingHandshakeMultiplier;
    }

    /// <summary>
    /// Creates a listener bound to the loopback interface on the given port.
    /// </summary>
    /// <remarks>
    /// Binds to <see cref="IPAddress.Loopback"/>, not every interface, so a hub created this way is not
    /// exposed to other hosts by default. Use the
    /// <see cref="WebSocketTransportListener(IPEndPoint, string, SslServerAuthenticationOptions, TimeSpan?, int?)"/>
    /// constructor with the desired address to listen more broadly, and give it TLS options unless the
    /// segment is already trusted.
    /// </remarks>
    /// <param name="port">The TCP port to listen on.</param>
    /// <param name="path">The HTTP request path a client must upgrade on. Defaults to <c>"/"</c>.</param>
    /// <param name="tlsOptions">
    /// TLS options to authenticate each accepted connection as the server, or <see langword="null"/>
    /// (the default) to accept cleartext (<c>ws://</c>) connections.
    /// </param>
    /// <param name="handshakeTimeout">How long a single connection's negotiation may take. Defaults to 10 seconds.</param>
    /// <param name="maxConcurrentHandshakes">The maximum number of connections to negotiate at once. Defaults to 64.</param>
    public WebSocketTransportListener(
        int port,
        string path = "/",
        SslServerAuthenticationOptions? tlsOptions = null,
        TimeSpan? handshakeTimeout = null,
        int? maxConcurrentHandshakes = null)
        : this(new IPEndPoint(IPAddress.Loopback, port), path, tlsOptions, handshakeTimeout, maxConcurrentHandshakes)
    {
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">The listener has been disposed.</exception>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_listener is not null)
            {
                throw new InvalidOperationException("The listener is already running.");
            }

            var listener = new TcpListener(_endPoint);
            listener.Start();
            _listener = listener;

            // Hold at most one negotiated connection per concurrent handshake slot, so a slow consumer
            // exerts back-pressure through the pump rather than letting finished connections pile up.
            _negotiatedTransports = Channel.CreateBounded<WebSocketTransport>(
                new BoundedChannelOptions(_maxConcurrentHandshakes)
                {
                    SingleReader = true,
                    SingleWriter = false,
                });

            _negotiationCts = new CancellationTokenSource();
            _negotiationPumpTask = NegotiationPumpAsync(listener, _negotiatedTransports.Writer, _negotiationCts.Token);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">
    /// The listener has been disposed, or was disposed while this accept was pending.
    /// </exception>
    public async Task<ITransport> AcceptAsync(CancellationToken cancellationToken = default)
    {
        Channel<WebSocketTransport> negotiatedTransports;

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
            // The pump has stopped, either because the listener was disposed or because accepting
            // failed outright; either way this listener will never produce another connection. Surface
            // it as ObjectDisposedException so the hub's accept loop stops rather than spinning.
            throw new ObjectDisposedException(
                $"The {nameof(WebSocketTransportListener)} is no longer accepting connections.",
                ex.InnerException ?? ex);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Safe to call more than once, and from more than one thread at a time. The first call takes
    /// ownership of the listener's state and performs the teardown; every other call awaits that same
    /// teardown.
    /// </remarks>
    public ValueTask DisposeAsync()
    {
        Task disposal;

        lock (_stateLock)
        {
            if (_disposeTask is null)
            {
                _disposed = true;

                TcpListener? listener = _listener;
                CancellationTokenSource? negotiationCts = _negotiationCts;
                Task? negotiationPumpTask = _negotiationPumpTask;
                Channel<WebSocketTransport>? negotiatedTransports = _negotiatedTransports;

                _listener = null;
                _negotiationCts = null;
                _negotiationPumpTask = null;
                _negotiatedTransports = null;

                _disposeTask = DisposeCoreAsync(listener, negotiationCts, negotiationPumpTask, negotiatedTransports);
            }

            disposal = _disposeTask;
        }

        return new ValueTask(disposal);
    }

    private static async Task DisposeCoreAsync(
        TcpListener? listener,
        CancellationTokenSource? negotiationCts,
        Task? negotiationPumpTask,
        Channel<WebSocketTransport>? negotiatedTransports)
    {
        if (negotiationCts is not null)
        {
            await negotiationCts.CancelAsync().ConfigureAwait(false);
        }

        listener?.Stop();

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
            // close them rather than leaking the sockets.
            while (negotiatedTransports.Reader.TryRead(out WebSocketTransport? pending))
            {
                await pending.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Accepts sockets and negotiates each one — TLS where configured, then the HTTP upgrade request —
    /// off the accept path, publishing the successfully negotiated transports for
    /// <see cref="AcceptAsync"/> to return.
    /// </summary>
    private async Task NegotiationPumpAsync(
        TcpListener listener,
        ChannelWriter<WebSocketTransport> writer,
        CancellationToken cancellationToken)
    {
        // Yield so StartAsync returns before the first accept is issued, keeping it non-blocking.
        await Task.Yield();

        using var negotiationSlots = new SemaphoreSlim(_maxConcurrentHandshakes, _maxConcurrentHandshakes);
        using var pendingSlots = new SemaphoreSlim(_maxPendingHandshakes, _maxPendingHandshakes);
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
                    await Task.Delay(AcceptRetryDelay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (!pendingSlots.Wait(0, CancellationToken.None))
                {
                    tcpClient.Dispose();
                    continue;
                }

                Task negotiationTask = NegotiateAsync(tcpClient, writer, negotiationSlots, pendingSlots, cancellationToken);
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
        TcpClient tcpClient,
        ChannelWriter<WebSocketTransport> writer,
        SemaphoreSlim negotiationSlots,
        SemaphoreSlim pendingSlots,
        CancellationToken cancellationToken)
    {
        WebSocketTransport? transport = null;

        // Tracks whichever stream is currently negotiating — the plain NetworkStream, or the SslStream
        // wrapping it once TLS is configured — so the catch block below can dispose it if negotiation
        // fails at any point. Assigning this the instant the SslStream is constructed, rather than only
        // once AuthenticateAsServerAsync has succeeded, is what stops a handshake failure from leaking
        // it: TcpTransportListener.HandshakeAsync avoids the same leak by constructing its owning
        // transport before authenticating, for the identical reason.
        Stream? stream = null;
        try
        {
            tcpClient.NoDelay = true;

            using var negotiationCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            negotiationCts.CancelAfter(_handshakeTimeout);

            NetworkStream networkStream = tcpClient.GetStream();

            // Wait for the peer to actually send something before spending a negotiation slot on it. A
            // zero-byte read consumes nothing and completes only once data has arrived — whether that is
            // a TLS ClientHello or a plaintext HTTP request line — so a peer that connects and then stays
            // silent waits out its own timeout without ever occupying one of the slots that bound
            // negotiation concurrency. Mirrors TcpTransportListener.HandshakeAsync, which does the same
            // zero-byte read before its own handshake-slot semaphore for the identical reason: without
            // it, a flood of connect-then-idle peers could hold every slot and starve genuine clients.
            await networkStream.ReadAsync(Memory<byte>.Empty, negotiationCts.Token).ConfigureAwait(false);

            await negotiationSlots.WaitAsync(negotiationCts.Token).ConfigureAwait(false);
            try
            {
                stream = networkStream;
                if (_tlsOptions is not null)
                {
                    var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false);
                    stream = sslStream;
                    await sslStream.AuthenticateAsServerAsync(_tlsOptions, negotiationCts.Token).ConfigureAwait(false);
                }

                (string? webSocketKey, byte[] leftover) =
                    await ReadUpgradeRequestAsync(stream, negotiationCts.Token).ConfigureAwait(false);
                if (webSocketKey is null)
                {
                    await WriteResponseAsync(stream, "400 Bad Request", negotiationCts.Token).ConfigureAwait(false);
                    await stream.DisposeAsync().ConfigureAwait(false);
                    tcpClient.Dispose();
                    return;
                }

                await WriteUpgradeResponseAsync(stream, webSocketKey, negotiationCts.Token).ConfigureAwait(false);

                // A peer is not required to wait for this 101 response before sending its first
                // WebSocket frame, and the buffered header read above may have already consumed the
                // start of it along with the terminating blank line. Prepending it back is what stops
                // that data from being silently lost.
                if (leftover.Length > 0)
                {
                    stream = new LeftoverPrefixedStream(stream, leftover);
                }

                SystemWebSocket webSocket = SystemWebSocket.CreateFromStream(
                    stream, isServer: true, subProtocol: null, keepAliveInterval: TimeSpan.Zero);

                transport = new WebSocketTransport(
                    webSocket, tcpClient.Client.RemoteEndPoint, isEncrypted: _tlsOptions is not null);
            }
            finally
            {
                negotiationSlots.Release();
            }

            await writer.WriteAsync(transport, cancellationToken).ConfigureAwait(false);
            transport = null;
        }
        catch
        {
            // A failed negotiation — a TLS handshake failure, a malformed or missing upgrade request, a
            // timeout, a peer that reset — concerns only this connection. Drop it quietly and keep the
            // listener serving; anything else would let one bad peer stop the hub. Disposing whichever
            // stream was negotiating (the SslStream, once one exists, rather than the plain
            // NetworkStream underneath it) is what releases a partially or fully negotiated TLS session
            // rather than leaking it — the socket itself closes as part of that, so tcpClient.Dispose()
            // is only needed when negotiation failed before any stream was even selected.
            if (transport is not null)
            {
                await transport.DisposeAsync().ConfigureAwait(false);
            }
            else if (stream is not null)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
                tcpClient.Dispose();
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

    /// <summary>
    /// Reads and validates an HTTP WebSocket upgrade request from the stream, consuming exactly the
    /// request line and headers and not a single byte more, so nothing is stolen from the WebSocket
    /// frames that follow once the connection is upgraded.
    /// </summary>
    /// <returns>
    /// The client's <c>Sec-WebSocket-Key</c> header value if the request is a well-formed upgrade for
    /// the configured path, or <see langword="null"/> if it is not, together with any bytes read past
    /// the header block's terminating blank line — a peer is not required to wait for the <c>101</c>
    /// response before sending its first WebSocket frame, so a buffered read can legitimately capture
    /// the start of one.
    /// </returns>
    private async Task<(string? Key, byte[] Leftover)> ReadUpgradeRequestAsync(
        Stream stream, CancellationToken cancellationToken)
    {
        (List<string> lines, byte[] leftover) = await ReadHeaderLinesAsync(stream, cancellationToken).ConfigureAwait(false);
        if (lines.Count == 0)
        {
            return (null, leftover);
        }

        string[] requestLine = lines[0].Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (requestLine.Length < 2
            || !string.Equals(requestLine[0], "GET", StringComparison.OrdinalIgnoreCase)
            || !string.Equals(requestLine[1], _path, StringComparison.Ordinal))
        {
            return (null, leftover);
        }

        string? upgrade = null;
        string? connection = null;
        string? webSocketKey = null;
        string? webSocketVersion = null;

        for (int i = 1; i < lines.Count; i++)
        {
            int separator = lines[i].IndexOf(':', StringComparison.Ordinal);
            if (separator < 0)
            {
                continue;
            }

            string name = lines[i][..separator].Trim();
            string value = lines[i][(separator + 1)..].Trim();

            if (string.Equals(name, "Upgrade", StringComparison.OrdinalIgnoreCase))
            {
                upgrade = value;
            }
            else if (string.Equals(name, "Connection", StringComparison.OrdinalIgnoreCase))
            {
                connection = value;
            }
            else if (string.Equals(name, "Sec-WebSocket-Key", StringComparison.OrdinalIgnoreCase))
            {
                webSocketKey = value;
            }
            else if (string.Equals(name, "Sec-WebSocket-Version", StringComparison.OrdinalIgnoreCase))
            {
                webSocketVersion = value;
            }
        }

        bool isValid =
            !string.IsNullOrEmpty(webSocketKey)
            && string.Equals(webSocketVersion, "13", StringComparison.Ordinal)
            && string.Equals(upgrade, "websocket", StringComparison.OrdinalIgnoreCase)
            && connection is not null
            && connection.Split(',', StringSplitOptions.TrimEntries)
                .Any(token => string.Equals(token, "Upgrade", StringComparison.OrdinalIgnoreCase));

        return (isValid ? webSocketKey : null, leftover);
    }

    private static readonly byte[] HeaderTerminator = "\r\n\r\n"u8.ToArray();

    /// <summary>
    /// Reads raw ASCII lines from the stream in bounded chunks until the blank line that terminates an
    /// HTTP header block, bounded overall by <see cref="MaxRequestHeaderBytes"/> so a peer that never
    /// sends one cannot hold the buffer open indefinitely.
    /// </summary>
    /// <remarks>
    /// Reads in chunks rather than one byte at a time — a single connection's handshake would otherwise
    /// cost roughly one <see cref="Stream.ReadAsync(Memory{byte}, CancellationToken)"/> call per header
    /// byte, which is significant overhead multiplied across every negotiating connection, and
    /// particularly under a reconnect storm where many clients renegotiate at once. Because the terminator
    /// can be read past in the same chunk that contains it, any bytes after it are not header bytes at
    /// all — they are the start of the first WebSocket frame, which a peer is not required to wait for
    /// the <c>101</c> response before sending. Those bytes are returned as leftover input rather than
    /// discarded, so the caller can hand them to the WebSocket layer instead of silently dropping them.
    /// </remarks>
    private static async Task<(List<string> Lines, byte[] Leftover)> ReadHeaderLinesAsync(
        Stream stream, CancellationToken cancellationToken)
    {
        var buffer = new byte[MaxRequestHeaderBytes];
        int filled = 0;

        while (true)
        {
            int terminatorIndex = buffer.AsSpan(0, filled).IndexOf(HeaderTerminator);
            if (terminatorIndex >= 0)
            {
                // Everything up to and including the last header line's own "\r\n" — the blank line's
                // second "\r\n" is the terminator itself and carries no line of its own.
                string headerText = Encoding.ASCII.GetString(buffer, 0, terminatorIndex + 2);
                string[] lines = headerText.Split(
                    "\r\n", StringSplitOptions.RemoveEmptyEntries);

                int consumed = terminatorIndex + HeaderTerminator.Length;
                byte[] leftover = buffer[consumed..filled];
                return ([.. lines], leftover);
            }

            if (filled >= buffer.Length)
            {
                // Exceeded the bound without ever finding the terminating blank line.
                return ([], []);
            }

            int bytesRead = await stream.ReadAsync(
                buffer.AsMemory(filled, buffer.Length - filled), cancellationToken).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                // The peer closed the connection before finishing the request.
                return ([], []);
            }

            filled += bytesRead;
        }
    }

    private static Task WriteUpgradeResponseAsync(Stream stream, string webSocketKey, CancellationToken cancellationToken)
    {
        // SHA-1 here is not a security control — RFC 6455 mandates this exact algorithm to compute the
        // Sec-WebSocket-Accept value, proving only that the server read the client's key. It carries no
        // confidentiality or integrity guarantee and is not a cryptographic weakness in this context.
#pragma warning disable CA5350
        string acceptKey = Convert.ToBase64String(
            SHA1.HashData(Encoding.ASCII.GetBytes(webSocketKey + WebSocketAcceptMagic)));
#pragma warning restore CA5350

        string response =
            "HTTP/1.1 101 Switching Protocols\r\n"
            + "Upgrade: websocket\r\n"
            + "Connection: Upgrade\r\n"
            + $"Sec-WebSocket-Accept: {acceptKey}\r\n"
            + "\r\n";

        return WriteResponseRawAsync(stream, response, cancellationToken);
    }

    private static Task WriteResponseAsync(Stream stream, string statusLine, CancellationToken cancellationToken)
    {
        string response = $"HTTP/1.1 {statusLine}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
        return WriteResponseRawAsync(stream, response, cancellationToken);
    }

    private static async Task WriteResponseRawAsync(Stream stream, string response, CancellationToken cancellationToken)
    {
        byte[] bytes = Encoding.ASCII.GetBytes(response);
        await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Wraps a stream so a handful of already-read bytes are served back before reads continue against
    /// the underlying stream, and writes pass straight through.
    /// </summary>
    /// <remarks>
    /// Used only to hand the buffered header reader's leftover bytes — the start of the first WebSocket
    /// frame, read incidentally while looking for the header block's terminating blank line — back to
    /// <see cref="SystemWebSocket.CreateFromStream(Stream, bool, string?, TimeSpan)"/>, so they are not
    /// silently dropped.
    /// </remarks>
    private sealed class LeftoverPrefixedStream(Stream inner, byte[] leftover) : Stream
    {
        private int _leftoverOffset;

        public override bool CanRead => true;

        public override bool CanWrite => inner.CanWrite;

        public override bool CanSeek => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_leftoverOffset < leftover.Length)
            {
                int available = leftover.Length - _leftoverOffset;
                int toCopy = Math.Min(available, buffer.Length);
                leftover.AsSpan(_leftoverOffset, toCopy).CopyTo(buffer.Span);
                _leftoverOffset += toCopy;
                return toCopy;
            }

            return await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            return inner.WriteAsync(buffer, cancellationToken);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return inner.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            inner.Write(buffer, offset, count);
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return inner.FlushAsync(cancellationToken);
        }

        public override void Flush()
        {
            inner.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
            }

            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await inner.DisposeAsync().ConfigureAwait(false);
            await base.DisposeAsync().ConfigureAwait(false);
        }
    }
}
