using System.Net;
using System.Net.Quic;
using System.Net.Security;
using AdamSalisbury.Meshworx.Transport.Framing;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.Transport.Quic;

/// <summary>
/// An <see cref="ITransport"/> implementation backed by a single bidirectional <see cref="QuicStream"/>
/// over a <see cref="QuicConnection"/>, giving TLS 1.3, faster connection setup, and head-of-line-blocking
/// resistance versus TCP.
/// </summary>
/// <remarks>
/// Framing is identical to <see cref="TcpTransport"/> and the other stream-oriented transports — a
/// 4-byte big-endian length prefix per message — sharing the same <see cref="StreamFramer"/> helper.
/// Write operations are internally synchronised, so concurrent calls to
/// <see cref="SendAsync(ReadOnlyMemory{byte}, CancellationToken)"/> from multiple threads are safe.
/// <para>
/// QUIC requires <see cref="QuicListener.IsSupported"/>/<see cref="QuicConnection.IsSupported"/> to be
/// <see langword="true"/> — typically meaning the native <c>msquic</c> library is present and the
/// platform's TLS stack supports TLS 1.3 (on Linux, install the <c>libmsquic</c> package). TLS is not
/// optional here, unlike the TCP and WebSocket transports: QUIC mandates it at the protocol level, so
/// <see cref="ConnectAsync"/> always takes <see cref="SslClientAuthenticationOptions"/>.
/// </para>
/// <para>
/// Meshworx uses exactly one bidirectional stream per connection — matching the one-channel-per-client
/// shape <see cref="ITransport"/> models — rather than the several concurrent streams a single QUIC
/// connection can multiplex. That capability is what makes QUIC attractive for a future large-message
/// or multi-channel feature, not something this transport itself needs yet.
/// </para>
/// </remarks>
public sealed class QuicTransport : ITransport, IBatchSendTransport, IRemoteEndPointTransport
{
    private const string DefaultApplicationProtocolName = "meshworx";

    /// <summary>
    /// The ALPN protocol both <see cref="QuicTransport.ConnectAsync"/> and
    /// <see cref="QuicTransportListener"/> advertise when the caller's TLS options do not already
    /// specify one. QUIC mandates ALPN negotiation, unlike TCP's optional TLS, so the two ends must
    /// agree on at least one protocol name for the handshake to succeed at all.
    /// </summary>
    internal static readonly SslApplicationProtocol DefaultApplicationProtocol = new(DefaultApplicationProtocolName);

    private readonly QuicConnection? _connection;
    private readonly Stream _stream;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    // Reused across reads to hold each frame's length prefix. The transport is single-reader (see
    // ITransport), so ReceiveAsync is never called concurrently and the buffer cannot be aliased.
    private readonly byte[] _headerBuffer = new byte[StreamFramer.HeaderSize];

    internal QuicTransport(QuicConnection connection, QuicStream stream)
        : this((QuicConnection?)connection, (Stream)stream)
    {
    }

    /// <summary>
    /// Constructs a transport directly over an arbitrary stream, with no underlying
    /// <see cref="QuicConnection"/> and therefore no <see cref="RemoteEndPoint"/> to report. Used by
    /// tests that exercise the shared framing against a plain in-memory stream — a real
    /// <see cref="QuicStream"/> cannot be constructed without a genuine connection.
    /// </summary>
    internal QuicTransport(Stream stream)
        : this(null, stream)
    {
    }

    private QuicTransport(QuicConnection? connection, Stream stream)
    {
        _connection = connection;
        _stream = stream;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The remote address of the underlying <see cref="QuicConnection"/>. QUIC runs over UDP, so this
    /// is always meaningful — unlike the Unix domain socket and named-pipe transports, which have no
    /// network address to report, a connection reachable through this transport is subject to
    /// <see cref="MeshHub"/>'s per-remote-endpoint connection cap exactly as the TCP and WebSocket
    /// transports are.
    /// </remarks>
    public EndPoint? RemoteEndPoint
    {
        get
        {
#pragma warning disable CA1416 // Windows/Linux/macOS-only API: this property is only ever read after a successful QuicConnection.ConnectAsync/QuicListener.AcceptConnectionAsync, which itself requires QuicConnection.IsSupported/QuicListener.IsSupported to be true.
            return _connection?.RemoteEndPoint;
#pragma warning restore CA1416
        }
    }

    /// <summary>
    /// Creates a new <see cref="QuicTransport"/> by connecting to the specified remote endpoint and
    /// opening a bidirectional stream on it.
    /// </summary>
    /// <param name="host">The hostname or IP address of the remote endpoint.</param>
    /// <param name="port">The UDP port of the remote endpoint.</param>
    /// <param name="tlsOptions">
    /// The TLS client options to authenticate with. Required — QUIC mandates TLS. The options are
    /// copied, so later mutation of the caller's instance does not affect this connection. If
    /// <see cref="SslClientAuthenticationOptions.TargetHost"/> is left unset it defaults to
    /// <paramref name="host"/>. If <see cref="SslClientAuthenticationOptions.ApplicationProtocols"/> is
    /// left unset it defaults to <see cref="DefaultApplicationProtocol"/>, which must then match
    /// whatever the listener advertises.
    /// </param>
    /// <param name="cancellationToken">
    /// A token to cancel the connect and the handshake. The handshake is only bounded by this token, so
    /// pass one that expires if a peer must not be able to stall the caller indefinitely.
    /// </param>
    /// <returns>A connected <see cref="QuicTransport"/> ready for use.</returns>
    /// <remarks>
    /// Opening the stream is purely local — QUIC does not notify the peer a stream exists until data
    /// (or a FIN) is actually sent on it — so <see cref="QuicTransportListener.AcceptAsync"/> on the
    /// other end will not complete until this transport's <c>SendAsync</c> is called at least once.
    /// This is never an issue in the normal Meshworx flow, since <c>MeshClient.ConnectAsync</c> sends
    /// the registration frame immediately after being handed a transport, but it matters for anyone
    /// testing against this transport directly: call <c>SendAsync</c> before waiting on the listener's
    /// <c>AcceptAsync</c>, not after, or the two ends deadlock waiting on each other.
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="host"/> is null or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="tlsOptions"/> is <see langword="null"/>.</exception>
    /// <exception cref="PlatformNotSupportedException">
    /// <see cref="QuicConnection.IsSupported"/> is <see langword="false"/> on this platform.
    /// </exception>
    public static async Task<QuicTransport> ConnectAsync(
        string host,
        int port,
        SslClientAuthenticationOptions tlsOptions,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(host);
        ArgumentNullException.ThrowIfNull(tlsOptions);

        if (!QuicConnection.IsSupported)
        {
            throw new PlatformNotSupportedException(
                "QUIC is not supported on this platform. This typically means the native msquic library "
                    + "is not installed (on Debian/Ubuntu: 'apt install libmsquic'), or the platform's TLS "
                    + "stack does not support TLS 1.3.");
        }

        SslClientAuthenticationOptions options = TcpTransport.CloneClientOptions(tlsOptions, host);
        if (options.ApplicationProtocols is not { Count: > 0 })
        {
            options.ApplicationProtocols = [DefaultApplicationProtocol];
        }

        var connectionOptions = new QuicClientConnectionOptions
        {
            RemoteEndPoint = new DnsEndPoint(host, port),
            ClientAuthenticationOptions = options,

            // The framework's own default (-1) is rejected by validation; Meshworx never aborts a
            // stream or connection with an application-defined error code, so 0 is simply "none".
            DefaultStreamErrorCode = 0,
            DefaultCloseErrorCode = 0,
        };

        QuicConnection? connection = null;
        QuicStream? stream = null;
        try
        {
            connection = await QuicConnection.ConnectAsync(connectionOptions, cancellationToken).ConfigureAwait(false);
            stream = await connection.OpenOutboundStreamAsync(QuicStreamType.Bidirectional, cancellationToken)
                .ConfigureAwait(false);
            return new QuicTransport(connection, stream);
        }
        catch
        {
            if (stream is not null)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }

            if (connection is not null)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        return StreamFramer.SendAsync(_stream, _writeLock, data, cancellationToken);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Frames every message with its own length prefix and writes the whole batch with a single
    /// buffered write and flush, so a burst of queued frames costs one syscall instead of one per frame
    /// — matching <see cref="TcpTransport"/>'s batching behaviour exactly, since both share
    /// <see cref="StreamFramer"/>.
    /// </remarks>
    public Task SendAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> messages,
        CancellationToken cancellationToken = default)
    {
        return StreamFramer.SendBatchAsync(_stream, _writeLock, messages, cancellationToken);
    }

    /// <inheritdoc/>
    public Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        return StreamFramer.ReceiveAsync(_stream, _headerBuffer, cancellationToken);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Deliberately does not dispose <see cref="_writeLock"/>. <see cref="SemaphoreSlim.Dispose()"/>
    /// abandons rather than completes any queued <see cref="SemaphoreSlim.WaitAsync()"/> waiter, so a
    /// concurrent <see cref="SendAsync(ReadOnlyMemory{byte}, CancellationToken)"/> racing this teardown
    /// would hang for ever instead of observing the stream fault it is actually waiting behind. The
    /// semaphore never touches <see cref="SemaphoreSlim.AvailableWaitHandle"/>, so it holds no unmanaged
    /// resource and leaving it undisposed is safe — it is simply collected once this transport is.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync().ConfigureAwait(false);

        if (_connection is not null)
        {
            // Windows/Linux/macOS-only API: reachable only when _connection is non-null, which only
            // happens after a successful QuicConnection.ConnectAsync/AcceptConnectionAsync — both of
            // which already require QuicConnection.IsSupported/QuicListener.IsSupported to be true.
#pragma warning disable CA1416
            await _connection.DisposeAsync().ConfigureAwait(false);
#pragma warning restore CA1416
        }
    }
}
