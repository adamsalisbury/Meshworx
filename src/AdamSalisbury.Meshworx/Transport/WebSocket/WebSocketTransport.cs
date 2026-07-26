using System.Buffers;
using System.Net;
using System.Net.WebSockets;
using SystemWebSocket = System.Net.WebSockets.WebSocket;

namespace AdamSalisbury.Meshworx.Transport.WebSocket;

/// <summary>
/// An <see cref="ITransport"/> implementation that communicates over a WebSocket connection, so a hub
/// can be reached from a browser or through proxies and firewalls that block arbitrary TCP ports.
/// </summary>
/// <remarks>
/// One WebSocket binary message carries exactly one Meshworx frame — the transport owns its own
/// framing, as <see cref="ITransport"/> requires, and needs no separate length prefix because the
/// WebSocket protocol already delimits messages. The 1 MiB payload cap is shared with the TCP
/// transport; an oversized outbound message is rejected up front, and an oversized inbound message
/// ends the connection rather than being buffered without bound.
/// <para>
/// Write operations are internally synchronised, so concurrent calls to
/// <see cref="SendAsync(ReadOnlyMemory{byte}, CancellationToken)"/> from multiple threads are safe.
/// </para>
/// </remarks>
public sealed class WebSocketTransport : ITransport, IRemoteEndPointTransport
{
    private const int MaxPayloadSize = 1024 * 1024;
    private const int ReceiveChunkSize = 8 * 1024;

    private readonly SystemWebSocket _webSocket;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    internal WebSocketTransport(SystemWebSocket webSocket, EndPoint? remoteEndPoint = null, bool isEncrypted = false)
    {
        _webSocket = webSocket;
        RemoteEndPoint = remoteEndPoint;
        IsEncrypted = isEncrypted;
    }

    /// <summary>
    /// Gets a value indicating whether this transport's byte stream is secured with TLS.
    /// </summary>
    /// <remarks>
    /// <see langword="false"/> means the connection is cleartext (<c>ws://</c>): readable and
    /// modifiable by anything on the path. Useful for asserting in a health check that a deployment
    /// really is encrypted.
    /// </remarks>
    public bool IsEncrypted { get; }

    /// <inheritdoc/>
    /// <remarks>
    /// The remote address the underlying socket was accepted from on the listener side, or
    /// <see langword="null"/> for a client-side connection — <see cref="ClientWebSocket"/> exposes no
    /// underlying socket to report an address from.
    /// </remarks>
    public EndPoint? RemoteEndPoint { get; }

    /// <summary>
    /// Creates a new <see cref="WebSocketTransport"/> by connecting to the specified WebSocket
    /// endpoint.
    /// </summary>
    /// <remarks>
    /// Use a <c>ws://</c> URI for a cleartext connection or <c>wss://</c> for one secured with TLS.
    /// Certificate validation for <c>wss://</c> follows the platform default unless
    /// <paramref name="configureOptions"/> sets
    /// <see cref="ClientWebSocketOptions.RemoteCertificateValidationCallback"/> or
    /// <see cref="ClientWebSocketOptions.ClientCertificates"/> for mutual TLS.
    /// </remarks>
    /// <param name="uri">The <c>ws://</c> or <c>wss://</c> URI of the remote endpoint.</param>
    /// <param name="configureOptions">
    /// An optional callback to configure the underlying <see cref="ClientWebSocketOptions"/> — for
    /// example to supply a client certificate or a custom certificate validation callback.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the connect and the WebSocket handshake.</param>
    /// <returns>A connected <see cref="WebSocketTransport"/> ready for use.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="uri"/> is <see langword="null"/>.</exception>
    public static async Task<WebSocketTransport> ConnectAsync(
        Uri uri,
        Action<ClientWebSocketOptions>? configureOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var clientWebSocket = new ClientWebSocket();
        try
        {
            configureOptions?.Invoke(clientWebSocket.Options);
            await clientWebSocket.ConnectAsync(uri, cancellationToken).ConfigureAwait(false);

            bool isEncrypted = string.Equals(uri.Scheme, "wss", StringComparison.OrdinalIgnoreCase);
            return new WebSocketTransport(clientWebSocket, remoteEndPoint: null, isEncrypted);
        }
        catch
        {
            clientWebSocket.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        // Reject oversized payloads up front, matching the TCP transport: the receiving peer enforces
        // the same limit and would otherwise treat the message as corrupt and drop the connection.
        if (data.Length > MaxPayloadSize)
        {
            throw new ArgumentException(
                $"Payload size {data.Length} exceeds the maximum frame payload of {MaxPayloadSize} bytes.",
                nameof(data));
        }

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _webSocket.SendAsync(
                data, WebSocketMessageType.Binary, endOfMessage: true, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <inheritdoc/>
    public async Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        byte[] chunk = ArrayPool<byte>.Shared.Rent(ReceiveChunkSize);
        try
        {
            using var message = new MemoryStream();
            ValueWebSocketReceiveResult result;
            do
            {
                try
                {
                    result = await _webSocket.ReceiveAsync(chunk.AsMemory(), cancellationToken).ConfigureAwait(false);
                }
                catch (WebSocketException ex) when (ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
                {
                    // The peer dropped the connection without completing the close handshake. Treat it
                    // the same as TCP's peer-closed-mid-frame case: end of stream, not a transport fault.
                    return null;
                }

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await CloseOutputBestEffortAsync(cancellationToken).ConfigureAwait(false);
                    return null;
                }

                if (result.MessageType != WebSocketMessageType.Binary)
                {
                    // Meshworx only ever sends binary frames. A text frame from a non-conformant or
                    // hostile peer means the framing can no longer be trusted; surface it the same way
                    // TCP surfaces a corrupt length prefix — as an I/O error that ends the connection.
                    throw new IOException($"Unexpected WebSocket message type: {result.MessageType}");
                }

                if (message.Length + result.Count > MaxPayloadSize)
                {
                    throw new IOException(
                        $"Received message exceeds the maximum frame payload of {MaxPayloadSize} bytes.");
                }

                message.Write(chunk, 0, result.Count);
            }
            while (!result.EndOfMessage);

            return message.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunk);
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_webSocket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                using var closeCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, null, closeCts.Token)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Best-effort close; the socket is disposed regardless.
        }
        catch (WebSocketException)
        {
            // The peer may already have gone away. Best-effort close; the socket is disposed regardless.
        }
        finally
        {
            _webSocket.Dispose();
            _writeLock.Dispose();
        }
    }

    private async Task CloseOutputBestEffortAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (WebSocketException)
        {
            // The peer may already have torn down the connection; nothing more to do.
        }
    }
}
