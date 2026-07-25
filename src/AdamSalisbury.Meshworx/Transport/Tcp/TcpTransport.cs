using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;

namespace AdamSalisbury.Meshworx.Transport.Tcp;

/// <summary>
/// An <see cref="ITransport"/> implementation that communicates over TCP using length-prefixed framing,
/// optionally secured with TLS.
/// </summary>
/// <remarks>
/// Each message is transmitted as a 4-byte big-endian length header followed by the payload bytes.
/// Write operations are internally synchronised, so concurrent calls to
/// <see cref="SendAsync(ReadOnlyMemory{byte}, CancellationToken)"/> from multiple threads are safe.
/// <para>
/// The plain <see cref="ConnectAsync(string, int, CancellationToken)"/> factory produces a cleartext
/// connection with no confidentiality, integrity or peer authentication. Use
/// <see cref="ConnectAsync(string, int, SslClientAuthenticationOptions, CancellationToken)"/> — paired
/// with a TLS-configured <see cref="TcpTransportListener"/> — whenever traffic crosses a network you do
/// not already trust. Framing is identical either way; only the byte stream changes.
/// </para>
/// </remarks>
public sealed class TcpTransport : ITransport, IBatchSendTransport, IRemoteEndPointTransport
{
    private const int HeaderSize = 4;
    private const int MaxPayloadSize = 1024 * 1024;

    private readonly TcpClient? _tcpClient;
    private readonly Stream _stream;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    // Reused across reads to hold each frame's length prefix. The transport is single-reader (see
    // ITransport), so ReceiveAsync is never called concurrently and the buffer cannot be aliased.
    private readonly byte[] _headerBuffer = new byte[HeaderSize];

    internal TcpTransport(TcpClient tcpClient)
        : this(tcpClient, tcpClient.GetStream())
    {
    }

    internal TcpTransport(Stream stream)
    {
        _stream = stream;
    }

    internal TcpTransport(TcpClient tcpClient, Stream stream)
    {
        _tcpClient = tcpClient;
        _stream = stream;
    }

    /// <summary>
    /// Gets a value indicating whether this transport's byte stream is secured with TLS.
    /// </summary>
    /// <remarks>
    /// <see langword="false"/> means the connection is cleartext: readable and modifiable by anything on
    /// the path. Useful for asserting in a health check that a deployment really is encrypted.
    /// </remarks>
    public bool IsEncrypted => _stream is SslStream { IsEncrypted: true };

    /// <inheritdoc/>
    /// <remarks>
    /// <see langword="null"/> when this instance was constructed directly from a <see cref="Stream"/>
    /// rather than a connected <see cref="TcpClient"/> — the internal constructor used by tests that
    /// exercise framing against an arbitrary stream, which has no socket to report an address for.
    /// </remarks>
    public EndPoint? RemoteEndPoint => _tcpClient?.Client.RemoteEndPoint;

    /// <summary>
    /// Creates a new, unencrypted <see cref="TcpTransport"/> by connecting to the specified remote
    /// endpoint.
    /// </summary>
    /// <remarks>
    /// The resulting connection is cleartext, so every client name, assigned id, group name and message
    /// payload crosses the wire in the clear and can be modified in flight. Only use this on a network
    /// segment you already trust, or inside an existing encrypted channel. For a secured connection use
    /// <see cref="ConnectAsync(string, int, SslClientAuthenticationOptions, CancellationToken)"/>.
    /// </remarks>
    /// <param name="host">The hostname or IP address of the remote endpoint.</param>
    /// <param name="port">The TCP port of the remote endpoint.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A connected <see cref="TcpTransport"/> ready for use.</returns>
    public static async Task<TcpTransport> ConnectAsync(
        string host,
        int port,
        CancellationToken cancellationToken = default)
    {
        var tcpClient = new TcpClient();
        try
        {
            tcpClient.NoDelay = true;
            await tcpClient.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
            return new TcpTransport(tcpClient);
        }
        catch
        {
            tcpClient.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Creates a new <see cref="TcpTransport"/> by connecting to the specified remote endpoint and
    /// completing a TLS handshake as the client before any framing is exchanged.
    /// </summary>
    /// <remarks>
    /// The handshake authenticates the hub, and encrypts and integrity-protects everything sent over the
    /// connection thereafter. The framing is unchanged — the same 4-byte length prefix simply runs inside
    /// the TLS record layer — so this is interchangeable with the cleartext factory as far as the client
    /// and hub are concerned.
    /// <para>
    /// The options are copied before use, so later mutation of the caller's instance does not affect this
    /// connection. If <see cref="SslClientAuthenticationOptions.TargetHost"/> is left unset it defaults to
    /// <paramref name="host"/>, which is what the server certificate is then validated against. Leave
    /// <see cref="SslClientAuthenticationOptions.EnabledSslProtocols"/> at its default
    /// (<see cref="System.Security.Authentication.SslProtocols.None"/>) so the platform negotiates its
    /// best available version rather than a pinned, ageing one.
    /// </para>
    /// <para>
    /// Certificate validation is the platform default unless the caller supplies a
    /// <see cref="SslClientAuthenticationOptions.RemoteCertificateValidationCallback"/>. A callback that
    /// unconditionally returns <see langword="true"/> accepts any certificate from anyone and reduces TLS
    /// to obfuscation — an on-path attacker can then impersonate the hub. Pin or validate properly.
    /// </para>
    /// <para>
    /// Supply a client certificate through
    /// <see cref="SslClientAuthenticationOptions.ClientCertificates"/> for mutual TLS.
    /// </para>
    /// </remarks>
    /// <param name="host">The hostname or IP address of the remote endpoint.</param>
    /// <param name="port">The TCP port of the remote endpoint.</param>
    /// <param name="tlsOptions">
    /// The TLS client options to authenticate with. Required; use the cleartext overload if TLS is not
    /// wanted.
    /// </param>
    /// <param name="cancellationToken">
    /// A token to cancel the connect and the handshake. The handshake is only bounded by this token, so
    /// pass one that expires if a peer must not be able to stall the caller indefinitely.
    /// </param>
    /// <returns>A connected, TLS-secured <see cref="TcpTransport"/> ready for use.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tlsOptions"/> is <see langword="null"/>.</exception>
    /// <exception cref="AuthenticationException">The TLS handshake failed.</exception>
    public static async Task<TcpTransport> ConnectAsync(
        string host,
        int port,
        SslClientAuthenticationOptions tlsOptions,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tlsOptions);

        // Copy the caller's options so a later mutation of their instance cannot change how this
        // connection was authenticated, and so defaulting TargetHost below is not a visible side effect
        // on an object the caller may reuse for other connections.
        SslClientAuthenticationOptions options = CloneClientOptions(tlsOptions, host);

        var tcpClient = new TcpClient();
        SslStream? sslStream = null;
        try
        {
            tcpClient.NoDelay = true;
            await tcpClient.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);

            sslStream = new SslStream(tcpClient.GetStream(), leaveInnerStreamOpen: false);
            await sslStream.AuthenticateAsClientAsync(options, cancellationToken).ConfigureAwait(false);

            return new TcpTransport(tcpClient, sslStream);
        }
        catch
        {
            // Dispose the SslStream first: it owns the NetworkStream and unwinds the partially
            // negotiated session before the socket goes away.
            if (sslStream is not null)
            {
                await sslStream.DisposeAsync().ConfigureAwait(false);
            }

            tcpClient.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Copies every setting from the caller's options, defaulting an unset target host to
    /// <paramref name="host"/>.
    /// </summary>
    /// <remarks>
    /// Every settable property must be copied. A property left out here silently discards the caller's
    /// intent, which for a security setting means quietly weakening the connection — so this is covered
    /// by a reflection test that fails when the framework type gains a property this does not handle.
    /// </remarks>
    internal static SslClientAuthenticationOptions CloneClientOptions(
        SslClientAuthenticationOptions source,
        string host)
    {
        var clone = new SslClientAuthenticationOptions
        {
            // Without a target host the platform cannot match the certificate's subject against anything,
            // so fall back to the host we dialled. That is the name the caller expressed trust in.
            TargetHost = string.IsNullOrEmpty(source.TargetHost) ? host : source.TargetHost,
            AllowRenegotiation = source.AllowRenegotiation,
            AllowTlsResume = source.AllowTlsResume,
            ApplicationProtocols = source.ApplicationProtocols,
            CertificateChainPolicy = source.CertificateChainPolicy,
            CertificateRevocationCheckMode = source.CertificateRevocationCheckMode,
            CipherSuitesPolicy = source.CipherSuitesPolicy,
            ClientCertificateContext = source.ClientCertificateContext,
            ClientCertificates = source.ClientCertificates,
            EnabledSslProtocols = source.EnabledSslProtocols,
            EncryptionPolicy = source.EncryptionPolicy,
            LocalCertificateSelectionCallback = source.LocalCertificateSelectionCallback,
            RemoteCertificateValidationCallback = source.RemoteCertificateValidationCallback,
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

    /// <inheritdoc/>
    public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        // Reject oversized payloads up front. The receiving peer enforces the same limit and
        // would otherwise treat the frame as corrupt and drop the connection — a clear
        // ArgumentException to the caller is far better than a surprise disconnect. This also
        // guards the frameSize addition below against integer overflow.
        if (data.Length > MaxPayloadSize)
        {
            throw new ArgumentException(
                $"Payload size {data.Length} exceeds the maximum frame payload of {MaxPayloadSize} bytes.",
                nameof(data));
        }

        int frameSize = HeaderSize + data.Length;
        byte[] frame = ArrayPool<byte>.Shared.Rent(frameSize);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(frame, data.Length);
            data.Span.CopyTo(frame.AsSpan(HeaderSize));

            await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _stream.WriteAsync(frame.AsMemory(0, frameSize), cancellationToken).ConfigureAwait(false);
                await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _writeLock.Release();
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(frame);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Frames every message with its own length prefix and writes the whole batch with a single
    /// buffered <see cref="Stream.WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/> and flush,
    /// so a burst of queued frames costs one syscall and one flush instead of one per frame.
    /// </remarks>
    public async Task SendAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);

        if (messages.Count == 0)
        {
            return;
        }

        if (messages.Count == 1)
        {
            await SendAsync(messages[0], cancellationToken).ConfigureAwait(false);
            return;
        }

        // Frame only the valid prefix up to the first oversize payload (if any). Writing that prefix
        // before throwing preserves the single-send path's deliver-then-fault behaviour: frames
        // coalesced ahead of an oversize one are still delivered, rather than the whole batch being
        // discarded because a later frame is invalid.
        long frameSize = 0;
        int validCount = messages.Count;
        for (int i = 0; i < messages.Count; i++)
        {
            if (messages[i].Length > MaxPayloadSize)
            {
                validCount = i;
                break;
            }

            frameSize += HeaderSize + messages[i].Length;
        }

        if (frameSize > 0)
        {
            // The send loop bounds a batch's total size, so frameSize is well within int range here;
            // the cast is safe. Renting a single buffer keeps the prefix to one write and one flush.
            byte[] frame = ArrayPool<byte>.Shared.Rent((int)frameSize);
            try
            {
                int offset = 0;
                for (int i = 0; i < validCount; i++)
                {
                    ReadOnlyMemory<byte> message = messages[i];
                    BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(offset), message.Length);
                    offset += HeaderSize;
                    message.Span.CopyTo(frame.AsSpan(offset));
                    offset += message.Length;
                }

                await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    await _stream.WriteAsync(frame.AsMemory(0, offset), cancellationToken).ConfigureAwait(false);
                    await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _writeLock.Release();
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(frame);
            }
        }

        if (validCount < messages.Count)
        {
            int length = messages[validCount].Length;
            throw new ArgumentException(
                $"Payload size {length} exceeds the maximum frame payload of {MaxPayloadSize} bytes.",
                nameof(messages));
        }
    }

    /// <inheritdoc/>
    public async Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        // Read the length prefix into the reused header buffer; only the payload array below is
        // allocated per frame, because it is handed back to the caller.
        if (!await ReadExactlyAsync(_headerBuffer, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        int payloadLength = BinaryPrimitives.ReadInt32BigEndian(_headerBuffer);

        if (payloadLength is < 0 or > MaxPayloadSize)
        {
            // A corrupt or out-of-range length prefix means the stream framing is no longer
            // trustworthy. Surface it as an I/O error so receive loops treat it as a transport
            // failure and terminate the connection cleanly, rather than faulting on an
            // unhandled exception type.
            throw new IOException($"Invalid payload length: {payloadLength}");
        }

        if (payloadLength == 0)
        {
            return [];
        }

        var payload = new byte[payloadLength];
        if (!await ReadExactlyAsync(payload, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return payload;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync().ConfigureAwait(false);
        _tcpClient?.Dispose();
        _writeLock.Dispose();
    }

    private async Task<bool> ReadExactlyAsync(byte[] buffer, CancellationToken cancellationToken)
    {
        try
        {
            await _stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (EndOfStreamException)
        {
            // The peer closed the connection (cleanly, or mid-frame); signal end of stream.
            return false;
        }
    }
}
