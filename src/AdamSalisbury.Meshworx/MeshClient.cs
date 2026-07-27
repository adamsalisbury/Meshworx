using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Sockets;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;

namespace AdamSalisbury.Meshworx;

public sealed class MeshClient : IMeshClient, IAsyncDisposable
{
    private readonly ILogger<MeshClient> _logger;
    private readonly Lock _stateLock = new();
    private readonly SemaphoreSlim _lookupLock = new(1, 1);

    // Set true within the receive loop's execution flow (AsyncLocal flows to the synchronous
    // event handlers it invokes, but never back to external callers). DisconnectAsync reads it
    // to avoid awaiting the receive loop from inside the receive loop, which would deadlock.
    private readonly AsyncLocal<bool> _inReceiveLoop = new();

    private ConnectionState _state = ConnectionState.Disconnected;

    // Set by DisconnectAsync when it finds a teardown already in flight, and read by that teardown
    // immediately before it would raise Disconnected. Guarded by _stateLock. It exists so the outcome
    // of a local disconnect racing a remote drop does not depend on which side wins: whoever tears the
    // connection down, an application-initiated disconnect stays silent. Reset by ConnectAsync so a
    // claim left over from one connection cannot silence a genuine drop on the next.
    private bool _localDisconnectRequested;

    private ITransport? _transport;
    private CancellationTokenSource? _cts;
    private Task? _receiveLoopTask;

    // Single-slot pending lookup, serialised by _lookupLock. Each request carries a
    // correlation id echoed by the hub; the receive loop only completes the pending
    // lookup when the ids match, so a response from a cancelled request cannot resolve
    // a subsequent lookup with a stale result.
    private PendingLookup? _pendingLookup;
    private int _lookupCorrelationId;

    // Concurrent, unlike the single-slot lookup above: multiple RequestAsync calls may be in flight
    // together, each tracked independently by its own correlation id until its reply arrives, it times
    // out, or the connection tears down.
    private readonly ConcurrentDictionary<long, PendingRequest> _pendingRequests = new();
    private long _requestCorrelationId;

    private readonly Lock _groupMembershipLock = new();
    private readonly HashSet<string> _joinedGroups = new(StringComparer.Ordinal);

    private static readonly TimeSpan DefaultSendRetryDelay = TimeSpan.FromMilliseconds(100);

    private readonly TimeSpan? _idleTimeout;
    private readonly TimeSpan? _sendTimeout;
    private readonly int _maxSendAttempts;
    private readonly TimeSpan _sendRetryDelay;

    /// <param name="logger">The logger used to record client activity.</param>
    /// <param name="idleTimeout">
    /// The maximum time the client will wait without receiving any frame from the hub before treating
    /// the connection as lost and raising <see cref="Disconnected"/>. Set this above the hub's heartbeat
    /// interval so the hub's pings keep the connection alive. Defaults to <see langword="null"/> (no timeout).
    /// </param>
    /// <param name="sendTimeout">
    /// The maximum time a single message send may take before it is cancelled and fails with a
    /// <see cref="TimeoutException"/>. Cancelling releases the transport so a stalled send does not block
    /// the connection. Applies to <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, CancellationToken)"/>,
    /// <see cref="BroadcastAsync"/> and
    /// <see cref="SendToGroupAsync(string, ReadOnlyMemory{byte}, CancellationToken)"/>.
    /// A timed-out send is not retried. Defaults to <see langword="null"/>
    /// (no timeout).
    /// </param>
    /// <param name="maxSendAttempts">
    /// The maximum number of attempts a send is given when it fails with a transient transport I/O error
    /// (an <see cref="IOException"/> or <see cref="SocketException"/>). The first attempt counts, so a
    /// value of 1 disables retrying. Timeouts, logic errors, cancellation, and a closed connection are
    /// never retried. Defaults to 1.
    /// </param>
    /// <param name="sendRetryDelay">
    /// The base delay between send retries; each successive retry waits this multiplied by the attempt
    /// number (linear back-off). Only used when <paramref name="maxSendAttempts"/> is greater than 1.
    /// Defaults to 100 milliseconds.
    /// </param>
    public MeshClient(
        ILogger<MeshClient> logger,
        TimeSpan? idleTimeout = null,
        TimeSpan? sendTimeout = null,
        int maxSendAttempts = 1,
        TimeSpan? sendRetryDelay = null)
    {
        ArgumentNullException.ThrowIfNull(logger);

        if (idleTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(idleTimeout), "The idle timeout must be positive.");
        }

        if (sendTimeout is { } sendTimeoutValue && sendTimeoutValue <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(sendTimeout), "The send timeout must be positive.");
        }

        if (maxSendAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxSendAttempts), "The maximum send attempts must be at least one.");
        }

        if (sendRetryDelay is { } retryDelay && retryDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(sendRetryDelay), "The send retry delay must be positive.");
        }

        _logger = logger;
        _idleTimeout = idleTimeout;
        _sendTimeout = sendTimeout;
        _maxSendAttempts = maxSendAttempts;
        _sendRetryDelay = sendRetryDelay ?? DefaultSendRetryDelay;
    }

    /// <inheritdoc/>
    public Guid Id { get; private set; }

    /// <inheritdoc/>
    public string Name { get; private set; } = string.Empty;

    /// <inheritdoc/>
    public byte NegotiatedProtocolVersion { get; private set; }

    /// <inheritdoc/>
    public bool IsConnected
    {
        get
        {
            lock (_stateLock)
            {
                return _state is ConnectionState.Connected;
            }
        }
    }

    /// <inheritdoc/>
    public IReadOnlyCollection<string> JoinedGroups
    {
        get
        {
            lock (_groupMembershipLock)
            {
                return _joinedGroups.ToArray();
            }
        }
    }

    /// <inheritdoc/>
    public event EventHandler<MessageReceivedEventArgs>? MessageReceived;

    /// <inheritdoc/>
    public event EventHandler<GroupMessageReceivedEventArgs>? GroupMessageReceived;

    /// <inheritdoc/>
    public event EventHandler<GroupJoinRefusedEventArgs>? GroupJoinRefused;

    /// <inheritdoc/>
    public event EventHandler<DisconnectedEventArgs>? Disconnected;

    /// <inheritdoc/>
    public async Task ConnectAsync(
        ITransport transport,
        string clientName,
        ReadOnlyMemory<byte> credential = default,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentException.ThrowIfNullOrEmpty(clientName);

        if (clientName.Length > Protocol.MaxClientNameLength)
        {
            throw new ArgumentException(
                $"Client name exceeds the maximum length of {Protocol.MaxClientNameLength} characters.",
                nameof(clientName));
        }

        lock (_stateLock)
        {
            if (_state is not ConnectionState.Disconnected)
            {
                throw new InvalidOperationException(_state switch
                {
                    ConnectionState.Connecting => "A connection attempt is already in progress.",
                    ConnectionState.Disconnecting => "A disconnect is in progress.",
                    _ => "Already connected to a hub.",
                });
            }

            _state = ConnectionState.Connecting;
            _localDisconnectRequested = false;
            _transport = transport;
        }

        try
        {
            // Registration frame: [type][versionMin][versionMax][name length (2, big-endian)][name][credential].
            byte[] nameBytes = Encoding.UTF8.GetBytes(clientName);
            var requestPayload = new byte[3 + 2 + nameBytes.Length + credential.Length];
            requestPayload[0] = (byte)MessageType.RegistrationRequest;
            requestPayload[1] = Protocol.MinSupportedVersion;
            requestPayload[2] = Protocol.MaxSupportedVersion;
            BinaryPrimitives.WriteUInt16BigEndian(requestPayload.AsSpan(3, 2), (ushort)nameBytes.Length);
            nameBytes.CopyTo(requestPayload, 5);
            credential.Span.CopyTo(requestPayload.AsSpan(5 + nameBytes.Length));
            await _transport.SendAsync(requestPayload, cancellationToken).ConfigureAwait(false);

            byte[]? responseData = await _transport.ReceiveAsync(cancellationToken).ConfigureAwait(false);

            if (responseData is { Length: >= 2 }
                && (MessageType)responseData[0] == MessageType.Error)
            {
                var errorCode = (RegistrationErrorCode)responseData[1];
                throw new RegistrationRefusedException(errorCode);
            }

            if (responseData is null
                || responseData.Length != 18
                || (MessageType)responseData[0] != MessageType.RegistrationComplete)
            {
                throw new InvalidOperationException("Failed to register with the hub.");
            }

            Id = new Guid(responseData.AsSpan(1, 16));
            Name = clientName;
            NegotiatedProtocolVersion = responseData[17];
            _logger.LogInformation(
                "Connected to hub with id {ClientId} on protocol version {ProtocolVersion}",
                Id,
                NegotiatedProtocolVersion);

            var cts = new CancellationTokenSource();
            _cts = cts;

            // Mark connected before starting the loop: if the hub has already buffered a
            // disconnect, the loop can run synchronously to termination, and its teardown
            // only fires when it observes the Connected state.
            lock (_stateLock)
            {
                _state = ConnectionState.Connected;
            }

            Task loopTask = ReceiveLoopAsync(cts.Token);

            lock (_stateLock)
            {
                // The loop may have run to completion synchronously (a buffered disconnect), and a
                // Disconnected handler may have reconnected from within it, replacing _cts. Only
                // record the loop task while this connection is still the current one, so a stale
                // synchronous loop never clobbers a newer connection established during teardown.
                if (ReferenceEquals(_cts, cts))
                {
                    _receiveLoopTask = loopTask;
                }
            }
        }
        catch (Exception exception)
        {
            await CleanUpAsync().ConfigureAwait(false);

            lock (_stateLock)
            {
                _state = ConnectionState.Disconnected;
            }

            if (exception is RegistrationRefusedException or InvalidOperationException)
            {
                _logger.LogWarning(exception, "Failed to connect to hub");
            }
            else
            {
                _logger.LogError(exception, "Failed to connect to hub");
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        ITransport? transport;
        CancellationTokenSource? cts;
        Task? receiveLoopTask;

        lock (_stateLock)
        {
            if (_state is not ConnectionState.Connected)
            {
                // A teardown is already under way. If the receive loop started it — the connection
                // dropped remotely at the same moment the application asked to disconnect — it is
                // about to raise Disconnected for a disconnect the application requested. Claim the
                // teardown so it stays silent, matching what would have happened had this call won
                // the race instead. Setting the flag here is atomic with the teardown's own read of
                // it, because that read shares this lock with the move to Disconnected: once the
                // state is Disconnected the decision has already been taken and there is nothing
                // left to claim.
                if (_state is ConnectionState.Disconnecting)
                {
                    _localDisconnectRequested = true;
                }

                return;
            }

            _state = ConnectionState.Disconnecting;
            transport = _transport;
            cts = _cts;
            receiveLoopTask = _receiveLoopTask;
        }

        try
        {
            byte[] disconnectPayload = [(byte)MessageType.Disconnect];
            await transport!.SendAsync(disconnectPayload, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or OperationCanceledException)
        {
            // Best-effort disconnect notification; the transport may already be closed.
        }

        if (cts is not null)
        {
            await cts.CancelAsync().ConfigureAwait(false);
        }

        // If DisconnectAsync was invoked from within the receive loop (for example, from a
        // MessageReceived handler), awaiting the loop's own task here would deadlock. We have
        // already signalled cancellation, so the loop unwinds on its own; skip the await.
        if (receiveLoopTask is not null && !_inReceiveLoop.Value)
        {
            try
            {
                await receiveLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // CancellationToken triggered.
            }
        }

        await CleanUpAsync().ConfigureAwait(false);

        lock (_stateLock)
        {
            Id = Guid.Empty;
            Name = string.Empty;
            NegotiatedProtocolVersion = 0;
            _state = ConnectionState.Disconnected;
        }
    }

    /// <inheritdoc/>
    public Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        CancellationToken cancellationToken = default)
    {
        return SendAsync(recipientId, message, MessageHeaders.Empty, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        MessageHeaders headers,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(headers);
        ThrowIfReservedHeaderKeyPresent(headers);

        await SendCoreAsync(recipientId, message, headers, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds and sends a direct message frame, with or without a header block. Shared by the public
    /// <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, MessageHeaders, CancellationToken)"/> and by
    /// <see cref="RequestAsync"/>/<see cref="ReplyAsync"/>, which construct
    /// <see cref="RequestReplyHeaderKeys"/> headers themselves and so must bypass the public overload's
    /// <see cref="ThrowIfReservedHeaderKeyPresent"/> guard rather than trip over their own headers.
    /// </summary>
    private async Task SendCoreAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        ITransport transport;

        lock (_stateLock)
        {
            if (_state is not ConnectionState.Connected)
            {
                throw new InvalidOperationException("Not connected to a hub.");
            }

            transport = _transport!;
        }

        byte[] payload;
        if (headers.Count == 0)
        {
            // No header block is written at all when there is nothing to carry, so this remains
            // byte-for-byte identical to the frame a header-unaware peer already understands.
            payload = new byte[1 + 16 + message.Length];
            payload[0] = (byte)MessageType.SendMessage;
            recipientId.TryWriteBytes(payload.AsSpan(1));
            message.CopyTo(payload.AsMemory(17));
        }
        else
        {
            RequireHeaderEnvelopeSupport(NegotiatedProtocolVersion);

            int headerLength = HeaderEnvelope.GetEncodedLength(headers);
            payload = new byte[1 + 16 + 2 + headerLength + message.Length];
            payload[0] = (byte)MessageType.SendMessageWithHeaders;
            recipientId.TryWriteBytes(payload.AsSpan(1));
            BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(17, 2), (ushort)headerLength);
            HeaderEnvelope.Write(headers, payload.AsSpan(19, headerLength));
            message.CopyTo(payload.AsMemory(19 + headerLength));
        }

        await SendWithPolicyAsync(transport, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Guards against an application header colliding with one of <see cref="RequestReplyHeaderKeys"/>.
    /// Without this, a message that happened to carry a header literally named <c>mesh.reply</c> with
    /// value <c>"1"</c> would be silently intercepted by the receive loop as if it were a reply to some
    /// request and never raised through <see cref="MessageReceived"/> at all.
    /// </summary>
    private static void ThrowIfReservedHeaderKeyPresent(MessageHeaders headers)
    {
        if (headers.ContainsKey(RequestReplyHeaderKeys.CorrelationId)
            || headers.ContainsKey(RequestReplyHeaderKeys.Reply))
        {
            throw new ArgumentException(
                $"The header keys '{RequestReplyHeaderKeys.CorrelationId}' and '{RequestReplyHeaderKeys.Reply}' "
                + "are reserved for the request/response helper (RequestAsync/ReplyAsync) and cannot be set "
                + "directly.",
                nameof(headers));
        }
    }

    /// <inheritdoc/>
    public async Task BroadcastAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default)
    {
        ITransport transport;

        lock (_stateLock)
        {
            if (_state is not ConnectionState.Connected)
            {
                throw new InvalidOperationException("Not connected to a hub.");
            }

            transport = _transport!;
        }

        var payload = new byte[1 + message.Length];
        payload[0] = (byte)MessageType.BroadcastMessage;
        message.CopyTo(payload.AsMemory(1));

        await SendWithPolicyAsync(transport, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task JoinGroupAsync(string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupName);

        ITransport transport = GetConnectedTransport();

        // Record the membership before the frame goes out, not after. The hub may refuse the join, and
        // its refusal can arrive and be handled by the receive loop before this method resumes — an add
        // afterwards would then reinstate the very group the refusal had just removed. Both checks above
        // run before the record is made, so the only thing left to undo is a send that failed.
        bool recorded;
        lock (_groupMembershipLock)
        {
            recorded = _joinedGroups.Add(groupName);
        }

        try
        {
            await SendGroupMembershipAsync(transport, MessageType.JoinGroup, groupName, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            // The frame never reached the hub, so no membership was created; take the record back — but
            // only if this call is what recorded it. A join of a group already joined, or one racing a
            // concurrent join of the same name, must not roll back the record its predecessor owns: the
            // group would then be missing from JoinedGroups while the client is still in it on the hub,
            // and the reconnector, which restores from that snapshot, would silently not restore it.
            if (recorded)
            {
                lock (_groupMembershipLock)
                {
                    _joinedGroups.Remove(groupName);
                }
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public async Task LeaveGroupAsync(string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupName);

        ITransport transport = GetConnectedTransport();

        await SendGroupMembershipAsync(transport, MessageType.LeaveGroup, groupName, cancellationToken)
            .ConfigureAwait(false);

        lock (_groupMembershipLock)
        {
            _joinedGroups.Remove(groupName);
        }
    }

    /// <inheritdoc/>
    public Task SendToGroupAsync(
        string groupName,
        ReadOnlyMemory<byte> message,
        CancellationToken cancellationToken = default)
    {
        return SendToGroupAsync(groupName, message, MessageHeaders.Empty, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task SendToGroupAsync(
        string groupName,
        ReadOnlyMemory<byte> message,
        MessageHeaders headers,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        ArgumentNullException.ThrowIfNull(headers);

        ITransport transport = GetConnectedTransport();

        byte[] nameBytes = Encoding.UTF8.GetBytes(groupName);
        if (nameBytes.Length > ushort.MaxValue)
        {
            throw new ArgumentException("The group name is too long.", nameof(groupName));
        }

        byte[] payload;
        if (headers.Count == 0)
        {
            // No header block is written at all when there is nothing to carry, so this remains
            // byte-for-byte identical to the frame a header-unaware peer already understands.
            payload = new byte[1 + 2 + nameBytes.Length + message.Length];
            payload[0] = (byte)MessageType.GroupMessage;
            BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(1, 2), (ushort)nameBytes.Length);
            nameBytes.CopyTo(payload, 3);
            message.CopyTo(payload.AsMemory(3 + nameBytes.Length));
        }
        else
        {
            RequireHeaderEnvelopeSupport(NegotiatedProtocolVersion);

            int headerLength = HeaderEnvelope.GetEncodedLength(headers);
            int headerLengthOffset = 3 + nameBytes.Length;
            payload = new byte[headerLengthOffset + 2 + headerLength + message.Length];
            payload[0] = (byte)MessageType.GroupMessageWithHeaders;
            BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(1, 2), (ushort)nameBytes.Length);
            nameBytes.CopyTo(payload, 3);
            BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(headerLengthOffset, 2), (ushort)headerLength);
            HeaderEnvelope.Write(headers, payload.AsSpan(headerLengthOffset + 2, headerLength));
            message.CopyTo(payload.AsMemory(headerLengthOffset + 2 + headerLength));
        }

        await SendWithPolicyAsync(transport, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Guards a header-bearing send against a connection that negotiated a protocol version predating
    /// the header envelope, so headers the caller supplied are never silently dropped on the wire.
    /// </summary>
    private static void RequireHeaderEnvelopeSupport(byte negotiatedProtocolVersion)
    {
        if (negotiatedProtocolVersion < Protocol.HeaderEnvelopeMinVersion)
        {
            throw new NotSupportedException(
                $"Message headers require a negotiated protocol version of at least "
                + $"{Protocol.HeaderEnvelopeMinVersion}; this connection negotiated version "
                + $"{negotiatedProtocolVersion}.");
        }
    }

    /// <summary>
    /// Sends a payload over the transport, applying the configured send timeout and transient-failure
    /// retry policy. Defaults leave a single attempt with no timeout, preserving fire-and-forget sends.
    /// </summary>
    private async Task SendWithPolicyAsync(
        ITransport transport,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await SendOnceAsync(transport, payload, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (
                attempt < _maxSendAttempts
                && !cancellationToken.IsCancellationRequested
                && ex is IOException or SocketException)
            {
                // Only a transient transport I/O failure is retried, and only for a transport that does
                // not partially transmit a message before failing (the ITransport "send a complete
                // message" contract). A timeout is deliberately not retried: cancelling a stalled write
                // may leave a stream framing partly written, so it surfaces to the caller instead. Logic
                // errors, cancellation and a closed connection (ObjectDisposedException) also propagate.
                _logger.LogDebug(
                    ex,
                    "Transient failure sending to the hub on attempt {Attempt} of {MaxAttempts}; retrying",
                    attempt,
                    _maxSendAttempts);
                await Task.Delay(_sendRetryDelay * attempt, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task SendOnceAsync(
        ITransport transport,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        if (_sendTimeout is not { } timeout)
        {
            await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);
            return;
        }

        // Bound the send by cancelling it, not by abandoning the wait: cancelling releases the
        // transport's write path and any pooled buffer so a stalled send cannot block the connection.
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);

        try
        {
            await transport.SendAsync(payload, timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"The send did not complete within {timeout}.");
        }
    }

    private static async Task SendGroupMembershipAsync(
        ITransport transport,
        MessageType type,
        string groupName,
        CancellationToken cancellationToken)
    {
        byte[] nameBytes = Encoding.UTF8.GetBytes(groupName);
        var payload = new byte[1 + nameBytes.Length];
        payload[0] = (byte)type;
        nameBytes.CopyTo(payload, 1);

        await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    private ITransport GetConnectedTransport()
    {
        lock (_stateLock)
        {
            if (_state is not ConnectionState.Connected)
            {
                throw new InvalidOperationException("Not connected to a hub.");
            }

            return _transport!;
        }
    }

    /// <inheritdoc/>
    public async Task<Guid?> GetClientIdByNameAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        ITransport transport;

        lock (_stateLock)
        {
            if (_state is not ConnectionState.Connected)
            {
                throw new InvalidOperationException("Not connected to a hub.");
            }

            transport = _transport!;
        }

        await _lookupLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // _lookupLock serialises lookups, so a plain increment is sufficient.
            int correlationId = unchecked(_lookupCorrelationId++);
            var completion = new TaskCompletionSource<Guid?>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingLookup = new PendingLookup(correlationId, completion);

            byte[] nameBytes = Encoding.UTF8.GetBytes(name);
            var payload = new byte[1 + 4 + nameBytes.Length];
            payload[0] = (byte)MessageType.ClientLookupRequest;
            BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(1, 4), correlationId);
            nameBytes.CopyTo(payload, 5);
            await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);

            return await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _pendingLookup = null;

            try
            {
                _lookupLock.Release();
            }
            catch (ObjectDisposedException)
            {
                // The semaphore was disposed during a concurrent DisposeAsync call.
            }
        }
    }

    /// <inheritdoc/>
    public async Task<ReadOnlyMemory<byte>> RequestAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "The request timeout must be positive.");
        }

        long correlationId = Interlocked.Increment(ref _requestCorrelationId);
        var completion = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);

        // The expected responder is recorded alongside the completion source so a reply is only ever
        // accepted from the client this request was actually addressed to. Without this, any other
        // client connected to the same hub could forge a DeliverMessageWithHeaders frame guessing (or
        // brute-forcing) this correlation id and resolve the request with attacker-controlled bytes.
        _pendingRequests[correlationId] = new PendingRequest(recipientId, completion);

        try
        {
            var headers = new MessageHeaders(
            [
                new KeyValuePair<string, string>(
                    RequestReplyHeaderKeys.CorrelationId,
                    correlationId.ToString(CultureInfo.InvariantCulture)),
            ]);

            await SendCoreAsync(recipientId, message, headers, cancellationToken).ConfigureAwait(false);

            // Bound the wait by cancelling it, matching the pattern SendOnceAsync uses for its own
            // timeout: cancelling releases the wait rather than abandoning it, so a request that never
            // gets a reply does not leak a continuation waiting on the completion source forever.
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(timeout);

            try
            {
                return await completion.Task.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException($"The request did not receive a reply within {timeout}.");
            }
        }
        finally
        {
            // Whether the reply arrived, the call timed out, or it was cancelled, this correlation id is
            // no longer awaited — a late reply for it is discarded by TryCompletePendingRequest rather
            // than resolving a future request that happens to reuse the id.
            _pendingRequests.TryRemove(correlationId, out _);
        }
    }

    /// <inheritdoc/>
    public Task ReplyAsync(
        MessageReceivedEventArgs request,
        ReadOnlyMemory<byte> message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.CorrelationId is not { } correlationId)
        {
            throw new InvalidOperationException(
                "The supplied message was not a request and cannot be replied to.");
        }

        var headers = new MessageHeaders(
        [
            new KeyValuePair<string, string>(
                RequestReplyHeaderKeys.CorrelationId, correlationId.ToString(CultureInfo.InvariantCulture)),
            new KeyValuePair<string, string>(RequestReplyHeaderKeys.Reply, "1"),
        ]);

        return SendCoreAsync(request.SenderId, message, headers, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
        _lookupLock.Dispose();
    }

    private async Task CleanUpAsync()
    {
        ITransport? transport;
        CancellationTokenSource? cts;

        lock (_stateLock)
        {
            transport = _transport;
            cts = _cts;
            _transport = null;
            _cts = null;
            _receiveLoopTask = null;
        }

        lock (_groupMembershipLock)
        {
            _joinedGroups.Clear();
        }

        if (transport is not null)
        {
            await transport.DisposeAsync().ConfigureAwait(false);
        }

        cts?.Dispose();
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        // Mark this execution flow as the receive loop so a DisconnectAsync call made from a
        // handler invoked below does not deadlock by awaiting this very loop.
        _inReceiveLoop.Value = true;

        ITransport transport;

        lock (_stateLock)
        {
            transport = _transport ?? throw new InvalidOperationException("Transport is not initialised.");
        }

        // Tracks why the loop ended, used when the termination is remote (not a local
        // DisconnectAsync) to report a reason on the Disconnected event. Defaults to a lost
        // connection; only an explicit hub disconnect message changes it.
        var reason = DisconnectReason.ConnectionLost;

        // One linked source and one activity counter for the whole loop. The idle monitor compares
        // the counter between ticks and cancels this source if no frame arrives within the timeout,
        // so the read below never allocates a CancellationTokenSource or arms a timer per frame.
        using var idleCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        long activitySequence = 0;
        Task? idleMonitorTask = _idleTimeout is { } idleTimeout ? MonitorIdleAsync(idleTimeout) : null;

        async Task MonitorIdleAsync(TimeSpan timeout)
        {
            using var timer = new PeriodicTimer(timeout);
            long lastSeen = Volatile.Read(ref activitySequence);
            try
            {
                while (await timer.WaitForNextTickAsync(idleCts.Token).ConfigureAwait(false))
                {
                    long current = Volatile.Read(ref activitySequence);
                    if (current != lastSeen)
                    {
                        // A frame arrived during the interval; the connection is alive.
                        lastSeen = current;
                        continue;
                    }

                    _logger.LogWarning(
                        "No frame received from the hub within {Timeout}; treating the connection as lost",
                        timeout);
                    await idleCts.CancelAsync().ConfigureAwait(false);
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                // The receive loop is tearing down; stop monitoring.
            }
        }

        try
        {
            while (!idleCts.Token.IsCancellationRequested)
            {
                byte[]? data = await transport.ReceiveAsync(idleCts.Token).ConfigureAwait(false);

                if (data is null)
                {
                    break;
                }

                // Any received frame proves the connection is alive; the idle monitor observes this.
                Interlocked.Increment(ref activitySequence);

                if (data.Length == 0)
                {
                    // Empty frames carry no opcode; ignore rather than indexing data[0].
                    continue;
                }

                if (data.Length >= 17
                    && (MessageType)data[0] == MessageType.DeliverMessage)
                {
                    var senderId = new Guid(data.AsSpan(1, 16));
                    ReadOnlyMemory<byte> messageData = data.AsMemory(17);

                    try
                    {
                        MessageReceived?.Invoke(this, new MessageReceivedEventArgs
                        {
                            SenderId = senderId,
                            Data = messageData,
                        });
                    }
                    catch (Exception ex)
                    {
                        // A throwing subscriber must not tear down the receive loop and
                        // silently halt all further delivery. This is a callback boundary.
                        _logger.LogError(ex, "A MessageReceived handler threw an exception");
                    }
                }
                else if (data.Length >= 19
                    && (MessageType)data[0] == MessageType.DeliverGroupMessage)
                {
                    var senderId = new Guid(data.AsSpan(1, 16));
                    int nameLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(17, 2));

                    if (data.Length >= 19 + nameLength)
                    {
                        string groupName = Encoding.UTF8.GetString(data.AsSpan(19, nameLength));
                        ReadOnlyMemory<byte> messageData = data.AsMemory(19 + nameLength);

                        try
                        {
                            GroupMessageReceived?.Invoke(this, new GroupMessageReceivedEventArgs
                            {
                                SenderId = senderId,
                                GroupName = groupName,
                                Data = messageData,
                            });
                        }
                        catch (Exception ex)
                        {
                            // Callback boundary — a throwing subscriber must not halt the loop.
                            _logger.LogError(ex, "A GroupMessageReceived handler threw an exception");
                        }
                    }
                }
                else if (data.Length >= 19
                    && (MessageType)data[0] == MessageType.DeliverMessageWithHeaders)
                {
                    var senderId = new Guid(data.AsSpan(1, 16));
                    int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(17, 2));

                    if (data.Length >= 19 + headerBlockLength)
                    {
                        MessageHeaders? headers = TryReadHeaderBlock(data.AsSpan(19), headerBlockLength, senderId);
                        if (headers is not null)
                        {
                            ReadOnlyMemory<byte> messageData = data.AsMemory(19 + headerBlockLength);

                            if (!TryCompletePendingRequest(senderId, headers, messageData))
                            {
                                try
                                {
                                    MessageReceived?.Invoke(this, new MessageReceivedEventArgs
                                    {
                                        SenderId = senderId,
                                        Data = messageData,
                                        Headers = headers,
                                        CorrelationId = TryGetRequestCorrelationId(headers),
                                    });
                                }
                                catch (Exception ex)
                                {
                                    // A throwing subscriber must not tear down the receive loop and
                                    // silently halt all further delivery. This is a callback boundary.
                                    _logger.LogError(ex, "A MessageReceived handler threw an exception");
                                }
                            }
                        }
                    }
                }
                else if (data.Length >= 21
                    && (MessageType)data[0] == MessageType.DeliverGroupMessageWithHeaders)
                {
                    var senderId = new Guid(data.AsSpan(1, 16));
                    int nameLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(17, 2));
                    int headerLengthOffset = 19 + nameLength;

                    if (data.Length >= headerLengthOffset + 2)
                    {
                        int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(
                            data.AsSpan(headerLengthOffset, 2));
                        int bodyOffset = headerLengthOffset + 2 + headerBlockLength;

                        if (data.Length >= bodyOffset)
                        {
                            string groupName = Encoding.UTF8.GetString(data.AsSpan(19, nameLength));
                            MessageHeaders? headers = TryReadHeaderBlock(
                                data.AsSpan(headerLengthOffset + 2), headerBlockLength, senderId);

                            if (headers is not null)
                            {
                                ReadOnlyMemory<byte> messageData = data.AsMemory(bodyOffset);

                                try
                                {
                                    GroupMessageReceived?.Invoke(this, new GroupMessageReceivedEventArgs
                                    {
                                        SenderId = senderId,
                                        GroupName = groupName,
                                        Data = messageData,
                                        Headers = headers,
                                    });
                                }
                                catch (Exception ex)
                                {
                                    // Callback boundary — a throwing subscriber must not halt the loop.
                                    _logger.LogError(ex, "A GroupMessageReceived handler threw an exception");
                                }
                            }
                        }
                    }
                }
                else if (data.Length > 1
                    && (MessageType)data[0] == MessageType.GroupJoinRefused)
                {
                    // The hub declined the join, so this client is not a member however the join was
                    // issued — an application call or the reconnector restoring membership. Drop the
                    // optimistic record first, so JoinedGroups stops claiming a membership that does not
                    // exist and a later disconnect does not hand the group to the reconnector to restore.
                    string groupName = Encoding.UTF8.GetString(data.AsSpan(1));

                    lock (_groupMembershipLock)
                    {
                        _joinedGroups.Remove(groupName);
                    }

                    _logger.LogWarning("The hub refused membership of group {GroupName}", groupName);

                    try
                    {
                        GroupJoinRefused?.Invoke(this, new GroupJoinRefusedEventArgs { GroupName = groupName });
                    }
                    catch (Exception ex)
                    {
                        // Callback boundary — a throwing subscriber must not halt the loop.
                        _logger.LogError(ex, "A GroupJoinRefused handler threw an exception");
                    }
                }
                else if (data.Length >= 6
                    && (MessageType)data[0] == MessageType.ClientLookupResponse)
                {
                    int correlationId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(1, 4));
                    PendingLookup? pending = _pendingLookup;

                    if (pending is null || pending.CorrelationId != correlationId)
                    {
                        // Stale or unsolicited response (e.g. from a cancelled lookup); discard.
                        _logger.LogDebug(
                            "Discarding lookup response with unmatched correlation id {CorrelationId}",
                            correlationId);
                    }
                    else if (data[5] == 0x01 && data.Length >= 22)
                    {
                        pending.Completion.TrySetResult(new Guid(data.AsSpan(6, 16)));
                    }
                    else
                    {
                        pending.Completion.TrySetResult(null);
                    }
                }
                else if ((MessageType)data[0] == MessageType.Ping)
                {
                    // The hub is probing liveness; reply so it knows we are still here. Best-effort:
                    // if the send fails the connection is already gone and the loop will terminate.
                    try
                    {
                        await transport.SendAsync(
                            new[] { (byte)MessageType.Pong }, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is IOException or ObjectDisposedException)
                    {
                        // Connection is gone; the next receive will end the loop.
                    }
                }
                else if ((MessageType)data[0] == MessageType.Disconnect)
                {
                    _logger.LogInformation("Hub sent disconnect");
                    reason = DisconnectReason.RemoteDisconnect;
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Receive loop cancelled");
        }
        catch (IOException ex)
        {
            _logger.LogWarning(ex, "Receive loop terminated due to transport error");
        }
        catch (ObjectDisposedException ex)
        {
            _logger.LogDebug(ex, "Receive loop terminated: transport disposed");
        }
        finally
        {
            // Stop the idle monitor and let it unwind before tearing the connection down.
            if (idleMonitorTask is not null)
            {
                await idleCts.CancelAsync().ConfigureAwait(false);
                try
                {
                    await idleMonitorTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected once the loop's cancellation token is triggered.
                }
            }

            // The receive loop is the only thing that completes a pending lookup. If it
            // terminates for any reason before the response arrives, fault the waiter so
            // callers using a default (non-cancellable) token are not left hanging — and
            // so the held _lookupLock is released, unblocking subsequent lookups.
            _pendingLookup?.Completion.TrySetException(
                new InvalidOperationException("The connection was closed before the lookup completed."));

            // Likewise the only thing that completes a pending RequestAsync call: fault every
            // still-outstanding request so a caller awaiting with a non-cancellable token is not left
            // hanging, then clear the table so nothing here is mistaken for a match on the next connection.
            foreach (KeyValuePair<long, PendingRequest> pending in _pendingRequests)
            {
                pending.Value.Completion.TrySetException(
                    new InvalidOperationException("The connection was closed before a reply arrived."));
            }

            _pendingRequests.Clear();

            await HandleReceiveLoopTerminationAsync(reason).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Tears the connection down and raises <see cref="Disconnected"/> when the receive loop
    /// ends for a remote reason. If a local <see cref="DisconnectAsync"/> already moved the
    /// client out of the connected state, that call owns cleanup and no event is raised.
    /// A <see cref="DisconnectAsync"/> arriving while this teardown is in flight also suppresses
    /// the event, so a local disconnect racing a remote drop behaves the same whichever wins. One
    /// arriving after the disconnected state has been published is too late: the decision to raise
    /// is taken in the same locked block that publishes it.
    /// </summary>
    private async Task HandleReceiveLoopTerminationAsync(DisconnectReason reason)
    {
        lock (_stateLock)
        {
            // A local DisconnectAsync sets Disconnecting before cancelling the loop; if the
            // state is anything other than Connected, the teardown is already being handled
            // (or has happened) elsewhere and the application initiated it, so stay silent.
            if (_state is not ConnectionState.Connected)
            {
                return;
            }

            _state = ConnectionState.Disconnecting;
        }

        // Capture group membership before CleanUpAsync clears it, so the Disconnected event can
        // report the groups the client was in and a handler can restore them after reconnecting.
        string[] joinedGroups;
        lock (_groupMembershipLock)
        {
            joinedGroups = _joinedGroups.ToArray();
        }

        await CleanUpAsync().ConfigureAwait(false);

        bool raiseDisconnected;

        lock (_stateLock)
        {
            Id = Guid.Empty;
            Name = string.Empty;
            NegotiatedProtocolVersion = 0;
            _state = ConnectionState.Disconnected;

            // Take the decision to raise under the same lock that publishes the disconnected state,
            // so a DisconnectAsync racing this teardown either claims it before this point or finds
            // the client already disconnected and has nothing to claim.
            raiseDisconnected = !_localDisconnectRequested;
        }

        if (!raiseDisconnected)
        {
            _logger.LogDebug(
                "Suppressing Disconnected: the application requested this disconnect while the connection was being torn down");
            return;
        }

        try
        {
            Disconnected?.Invoke(this, new DisconnectedEventArgs { Reason = reason, JoinedGroups = joinedGroups });
        }
        catch (Exception ex)
        {
            // A throwing subscriber must not fault the receive loop task. Callback boundary.
            _logger.LogError(ex, "A Disconnected handler threw an exception");
        }
    }

    /// <summary>
    /// Decodes a header block, returning <see langword="null"/> instead of throwing if it is internally
    /// malformed.
    /// </summary>
    /// <remarks>
    /// The hub relays a header block byte-for-byte without decoding it, so a peer sending a
    /// well-formed outer frame with an internally corrupt header block reaches this decoder unfiltered.
    /// A single bad frame must not tear down the receive loop and disconnect an otherwise healthy
    /// connection — the same reasoning already applied to a throwing event handler at every call site
    /// below applies here too, just one step earlier.
    /// </remarks>
    private MessageHeaders? TryReadHeaderBlock(ReadOnlySpan<byte> source, int blockLength, Guid senderId)
    {
        try
        {
            return HeaderEnvelope.Read(source, blockLength);
        }
        catch (FormatException ex)
        {
            _logger.LogWarning(
                ex, "Discarding a message from {SenderId} with a malformed header block", senderId);
            return null;
        }
    }

    /// <summary>
    /// Completes a pending <see cref="RequestAsync"/> call if <paramref name="headers"/> mark this frame
    /// as a reply, so it is resolved internally rather than surfaced through <see cref="MessageReceived"/>.
    /// </summary>
    /// <param name="senderId">
    /// The actual sender of the frame, as stamped by the hub. Checked against the pending request's
    /// recorded <see cref="PendingRequest.ExpectedResponderId"/> before completion, so a reply cannot be
    /// forged by a client other than the one the request was addressed to.
    /// </param>
    /// <param name="headers">The frame's decoded headers.</param>
    /// <param name="messageData">The frame's body, used as the reply payload if this frame is a match.</param>
    /// <returns>
    /// <see langword="true"/> if the frame was a reply (whether or not it matched a still-pending
    /// request from the expected responder), so the caller must not raise it as an ordinary message.
    /// </returns>
    private bool TryCompletePendingRequest(Guid senderId, MessageHeaders headers, ReadOnlyMemory<byte> messageData)
    {
        if (!headers.TryGetValue(RequestReplyHeaderKeys.Reply, out string? replyFlag)
            || !string.Equals(replyFlag, "1", StringComparison.Ordinal))
        {
            return false;
        }

        if (!headers.TryGetValue(RequestReplyHeaderKeys.CorrelationId, out string? correlationText)
            || !long.TryParse(
                correlationText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long correlationId))
        {
            _logger.LogWarning("Discarding a reply frame with a missing or malformed correlation id");
            return true;
        }

        if (!_pendingRequests.TryGetValue(correlationId, out PendingRequest? pending))
        {
            // The request this reply answers has already timed out, been cancelled, or never existed
            // on this connection; discard rather than misrouting it to an unrelated later request.
            _logger.LogDebug(
                "Discarding a reply with an unmatched or expired correlation id {CorrelationId}",
                correlationId);
        }
        else if (pending.ExpectedResponderId != senderId)
        {
            // This frame claims to answer a request addressed elsewhere. Left in place rather than
            // removed, so a forged reply cannot be used to strand the real request — the genuine
            // responder's reply can still arrive and complete it.
            _logger.LogWarning(
                "Discarding a reply for correlation id {CorrelationId} from {SenderId}, which does not "
                + "match the expected responder {ExpectedResponderId}",
                correlationId,
                senderId,
                pending.ExpectedResponderId);
        }
        else if (_pendingRequests.TryRemove(new KeyValuePair<long, PendingRequest>(correlationId, pending)))
        {
            // The compare-remove above only succeeds against the exact instance just matched, so a
            // concurrent RequestAsync that has already claimed this id for a new call (having removed
            // and replaced the entry itself) cannot have its fresh request stolen by a stale reply.
            pending.Completion.TrySetResult(messageData);
        }

        return true;
    }

    /// <summary>
    /// Reads the request correlation id from an incoming (non-reply) frame's headers, for exposure on
    /// <see cref="MessageReceivedEventArgs.CorrelationId"/>.
    /// </summary>
    private static long? TryGetRequestCorrelationId(MessageHeaders headers)
    {
        if (headers.TryGetValue(RequestReplyHeaderKeys.CorrelationId, out string? correlationText)
            && long.TryParse(
                correlationText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long correlationId))
        {
            return correlationId;
        }

        return null;
    }

    private enum ConnectionState
    {
        Disconnected,
        Connecting,
        Connected,
        Disconnecting,
    }

    private sealed record PendingLookup(int CorrelationId, TaskCompletionSource<Guid?> Completion);

    /// <summary>
    /// A <see cref="RequestAsync"/> call awaiting a reply. <see cref="ExpectedResponderId"/> is checked
    /// against the actual sender of an incoming reply frame before <see cref="Completion"/> is resolved,
    /// so a client other than the one this request was addressed to cannot forge a reply and resolve it.
    /// </summary>
    private sealed record PendingRequest(Guid ExpectedResponderId, TaskCompletionSource<ReadOnlyMemory<byte>> Completion);
}
