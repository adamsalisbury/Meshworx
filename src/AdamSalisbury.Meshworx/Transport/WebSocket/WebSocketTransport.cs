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
public sealed class WebSocketTransport : ITransport, IBatchSendTransport, IRemoteEndPointTransport
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
    /// <remarks>
    /// Each message is still sent as its own WebSocket message — unlike the TCP transport's
    /// length-prefixed stream, the WebSocket protocol has no way to coalesce several logical messages
    /// into a single wire write. What this still saves is the lock: acquiring the write lock once for
    /// the whole batch, rather than once per message, means a fan-out burst (a broadcast or group send)
    /// no longer pays one acquisition and release per queued frame.
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

        // Mirror the single-send path's deliver-then-fault behaviour: messages ahead of the first
        // oversize one are still sent before the batch throws, rather than discarding the whole batch
        // because a later message in it is invalid.
        int validCount = messages.Count;
        for (int i = 0; i < messages.Count; i++)
        {
            if (messages[i].Length > MaxPayloadSize)
            {
                validCount = i;
                break;
            }
        }

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            for (int i = 0; i < validCount; i++)
            {
                await _webSocket.SendAsync(
                    messages[i], WebSocketMessageType.Binary, endOfMessage: true, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            _writeLock.Release();
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
    /// <remarks>
    /// The common case — a message that arrives in a single WebSocket frame, which is every message up
    /// to <see cref="ReceiveChunkSize"/> — copies the payload exactly once, matching the TCP transport's
    /// single exact-size read. Only a message spanning several frames falls back to accumulating in a
    /// <see cref="MemoryStream"/>, which costs a second copy on the final <c>ToArray()</c>.
    /// </remarks>
    public async Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        byte[] chunk = ArrayPool<byte>.Shared.Rent(ReceiveChunkSize);
        try
        {
            ValueWebSocketReceiveResult result = await ReceiveChunkAsync(chunk, cancellationToken).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                await CloseOutputBestEffortAsync(cancellationToken).ConfigureAwait(false);
                return null;
            }

            if (result.EndOfMessage)
            {
                // Fast path: the whole message fit in the first frame. One copy, no MemoryStream.
                return chunk.AsSpan(0, result.Count).ToArray();
            }

            using var message = new MemoryStream();
            message.Write(chunk, 0, result.Count);

            while (!result.EndOfMessage)
            {
                result = await ReceiveChunkAsync(chunk, cancellationToken).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await CloseOutputBestEffortAsync(cancellationToken).ConfigureAwait(false);
                    return null;
                }

                if (message.Length + result.Count > MaxPayloadSize)
                {
                    throw new IOException(
                        $"Received message exceeds the maximum frame payload of {MaxPayloadSize} bytes.");
                }

                message.Write(chunk, 0, result.Count);
            }

            return message.ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunk);
        }
    }

    /// <summary>
    /// Receives a single WebSocket frame into <paramref name="chunk"/>, translating a premature
    /// disconnect into a synthetic close result and validating the frame is binary and within the
    /// payload cap.
    /// </summary>
    private async Task<ValueWebSocketReceiveResult> ReceiveChunkAsync(byte[] chunk, CancellationToken cancellationToken)
    {
        ValueWebSocketReceiveResult result;
        try
        {
            result = await _webSocket.ReceiveAsync(chunk.AsMemory(), cancellationToken).ConfigureAwait(false);
        }
        catch (WebSocketException ex) when (ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
        {
            // The peer dropped the connection without completing the close handshake. Treat it the same
            // as TCP's peer-closed-mid-frame case: end of stream, not a transport fault. Reported as a
            // synthetic Close result so both call sites in ReceiveAsync handle it identically.
            return new ValueWebSocketReceiveResult(0, WebSocketMessageType.Close, endOfMessage: true);
        }

        if (result.MessageType == WebSocketMessageType.Close)
        {
            return result;
        }

        if (result.MessageType != WebSocketMessageType.Binary)
        {
            // Meshworx only ever sends binary frames. A text frame from a non-conformant or hostile peer
            // means the framing can no longer be trusted; surface it the same way TCP surfaces a corrupt
            // length prefix — as an I/O error that ends the connection.
            throw new IOException($"Unexpected WebSocket message type: {result.MessageType}");
        }

        return result;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Deliberately does not dispose <see cref="_writeLock"/>. <see cref="SemaphoreSlim.Dispose()"/>
    /// abandons rather than completes any queued <see cref="SemaphoreSlim.WaitAsync()"/> waiter, so a
    /// concurrent <see cref="SendAsync(ReadOnlyMemory{byte}, CancellationToken)"/> racing this teardown
    /// would hang for ever instead of observing the socket fault it is actually waiting behind. The
    /// semaphore never touches <see cref="SemaphoreSlim.AvailableWaitHandle"/>, so it holds no unmanaged
    /// resource and leaving it undisposed is safe — it is simply collected once this transport is.
    /// </remarks>
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
