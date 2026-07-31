using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net.Sockets;
using System.Text;
using AdamSalisbury.Meshworx.Diagnostics;
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

    // Single-slot pending client-attribute query, the same shape as _pendingLookup/_lookupLock above but
    // kept separate since it answers a different request type with a different reply shape.
    private readonly SemaphoreSlim _findClientsLock = new(1, 1);
    private PendingFindClients? _pendingFindClients;
    private int _findClientsCorrelationId;

    // Concurrent, unlike the single-slot lookup above: multiple RequestAsync calls may be in flight
    // together, each tracked independently by its own correlation id until its reply arrives, it times
    // out, or the connection tears down.
    private readonly ConcurrentDictionary<long, PendingRequest> _pendingRequests = new();
    private long _requestCorrelationId;

    // As _pendingRequests above, but for outstanding SendAsync(..., DeliveryOptions.RequireAck(...))
    // calls awaiting the recipient's acknowledgement rather than a reply payload.
    private readonly ConcurrentDictionary<long, PendingAck> _pendingAcks = new();
    private long _ackCorrelationId;

    // The resumption token the hub last issued, and the name it was issued to. Deliberately *not*
    // cleared when the connection ends — surviving the disconnect is the entire point of it, since it is
    // what the next ConnectAsync presents to reclaim this identity. It is cleared only when it is spent,
    // when the hub refuses it, or when connecting under a different name, for which it is meaningless.
    private byte[]? _sessionToken;
    private string? _sessionTokenName;

    // Completed by the receive loop when the hub answers a resume attempt: true for
    // SessionResumed, false for SessionResumeRefused. Guarded by _stateLock.
    private TaskCompletionSource<bool>? _pendingResume;

    // How long ConnectAsync waits for the hub to answer a resume attempt before giving up and keeping
    // the fresh identity it was just assigned. Short deliberately: the hub answers from the same receive
    // loop that just registered this client, so anything beyond a round trip means it is not going to.
    private static readonly TimeSpan SessionResumeTimeout = TimeSpan.FromSeconds(10);

    private readonly Lock _groupMembershipLock = new();
    private readonly HashSet<string> _joinedGroups = new(StringComparer.Ordinal);

    private readonly Lock _topicSubscriptionLock = new();
    private readonly HashSet<string> _subscribedTopics = new(StringComparer.Ordinal);

    private static readonly TimeSpan DefaultSendRetryDelay = TimeSpan.FromMilliseconds(100);

    // How much payload one chunk carries. A frame must fit the transport's 1 MiB cap once the message
    // type, recipient id, header-length prefix and the header block itself are added, so the budget is
    // the cap less a reserve for all of that. The three chunk headers cost about 110 bytes at their
    // longest (a 36-character GUID plus two four-digit numbers, with their keys); 4 KiB leaves room for
    // an application's own headers to travel on a chunked send as well, which they must, since
    // SendLargeAsync copies the caller's headers onto every chunk. A caller whose headers exceed that
    // reserve gets the same ArgumentException from the framer that any oversized frame would produce,
    // rather than a silently truncated transfer.
    private const int ChunkFrameOverheadReserve = 4 * 1024;

    private const int MaxChunkBodySize =
        Transport.Framing.StreamFramer.MaxPayloadSize - ChunkFrameOverheadReserve;

    private readonly TimeSpan? _idleTimeout;
    private readonly TimeSpan? _sendTimeout;
    private readonly int _maxSendAttempts;
    private readonly TimeSpan _sendRetryDelay;

    // Holds the chunks of every part-received large message until each is whole. Only ever touched from
    // the receive loop, which processes one frame at a time, so it needs no lock of its own.
    private readonly ChunkReassembler _reassembler;

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
    /// <param name="maxReassemblyBytes">
    /// The ceiling on memory held across every part-received chunked message at once. A chunk that
    /// would take this client past the ceiling is dropped and its transfer abandoned, so a peer that
    /// starts transfers and never finishes them costs a bounded, reclaimable amount. Defaults to
    /// 64 MiB.
    /// </param>
    /// <param name="chunkTransferTimeout">
    /// How long an incomplete chunked transfer may sit without a further chunk before it is discarded
    /// and its memory reclaimed. Defaults to one minute.
    /// </param>
    /// <param name="timeProvider">
    /// The clock used to age out incomplete chunked transfers. Defaults to
    /// <see cref="TimeProvider.System"/>; supply one to control time in a test.
    /// </param>
    public MeshClient(
        ILogger<MeshClient> logger,
        TimeSpan? idleTimeout = null,
        TimeSpan? sendTimeout = null,
        int maxSendAttempts = 1,
        TimeSpan? sendRetryDelay = null,
        int? maxReassemblyBytes = null,
        TimeSpan? chunkTransferTimeout = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(logger);

        if (maxReassemblyBytes is { } reassemblyBytes && reassemblyBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxReassemblyBytes), "The maximum reassembly bytes must be positive.");
        }

        if (chunkTransferTimeout is { } chunkTimeout && chunkTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(chunkTransferTimeout), "The chunk transfer timeout must be positive.");
        }

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
        _reassembler = new ChunkReassembler(maxReassemblyBytes, chunkTransferTimeout, timeProvider);
    }

    /// <inheritdoc/>
    public Guid Id { get; private set; }

    /// <inheritdoc/>
    public string Name { get; private set; } = string.Empty;

    /// <inheritdoc/>
    public byte NegotiatedProtocolVersion { get; private set; }

    /// <inheritdoc/>
    public bool SessionResumed { get; private set; }

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
    public IReadOnlyCollection<string> SubscribedTopics
    {
        get
        {
            lock (_topicSubscriptionLock)
            {
                return _subscribedTopics.ToArray();
            }
        }
    }

    /// <inheritdoc/>
    public event EventHandler<MessageReceivedEventArgs>? MessageReceived;

    /// <inheritdoc/>
    public event EventHandler<GroupMessageReceivedEventArgs>? GroupMessageReceived;

    /// <inheritdoc/>
    public event EventHandler<TopicMessageReceivedEventArgs>? TopicMessageReceived;

    /// <inheritdoc/>
    public event EventHandler<PresenceChangedEventArgs>? PresenceChanged;

    /// <inheritdoc/>
    public event EventHandler<GroupJoinRefusedEventArgs>? GroupJoinRefused;

    /// <inheritdoc/>
    public event EventHandler<DisconnectedEventArgs>? Disconnected;

    /// <inheritdoc/>
    public event EventHandler<SendRejectedEventArgs>? SendRejected;

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
            // Captured before registration replaces it: the token that reclaims the *previous* session is
            // the one to present, and the reply to this registration carries a new one that overwrites it.
            // A token issued to a different name is meaningless here, so it is not carried across.
            byte[]? resumptionToken = string.Equals(_sessionTokenName, clientName, StringComparison.Ordinal)
                ? _sessionToken
                : null;

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
                || responseData.Length < 18
                || (MessageType)responseData[0] != MessageType.RegistrationComplete)
            {
                throw new InvalidOperationException("Failed to register with the hub.");
            }

            Id = new Guid(responseData.AsSpan(1, 16));
            Name = clientName;
            NegotiatedProtocolVersion = responseData[17];
            SessionResumed = false;

            // A hub with session resumption enabled appends the token that reclaims this identity. The
            // reply is exactly 18 bytes without one, which is every reply from a hub built before
            // version 6 and every reply on a connection negotiated below it — so the trailing fields are
            // read only when they are actually there.
            ReadSessionToken(responseData, clientName);
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

            // Attempted only once the receive loop is running, because the hub's answer is not the next
            // frame on the wire: registration may already have queued messages held for this name while
            // it was away, and those arrive first. Handling the reply in the loop rather than with a
            // second blocking read is what makes the interleaving a non-event.
            if (resumptionToken is not null
                && NegotiatedProtocolVersion >= Protocol.SessionResumptionMinVersion)
            {
                await TryResumeSessionAsync(resumptionToken, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Reads the resumption token a version-6 hub appends to its registration reply, and remembers which
    /// name it belongs to.
    /// </summary>
    /// <remarks>
    /// A reply with no token — the 18-byte form — clears whatever was held rather than leaving it: a hub
    /// that has stopped issuing tokens, or one that has just refused this client's own, must not leave
    /// the client trying to spend a credential that no longer means anything.
    /// </remarks>
    private void ReadSessionToken(byte[] responseData, string clientName)
    {
        if (responseData.Length < 20)
        {
            _sessionToken = null;
            _sessionTokenName = null;
            return;
        }

        int tokenLength = BinaryPrimitives.ReadUInt16BigEndian(responseData.AsSpan(18, 2));
        if (tokenLength == 0 || responseData.Length < 20 + tokenLength)
        {
            _sessionToken = null;
            _sessionTokenName = null;
            return;
        }

        _sessionToken = responseData.AsSpan(20, tokenLength).ToArray();
        _sessionTokenName = clientName;
    }

    /// <summary>
    /// Presents a resumption token to the hub and waits for it to accept or refuse, leaving the client on
    /// the identity it just registered with if anything at all goes wrong.
    /// </summary>
    /// <remarks>
    /// Every failure here — a refusal, a timeout, a hub that does not recognise the opcode, a transport
    /// error on the way out — resolves to the same outcome: the connection is up, on the fresh identity,
    /// and <see cref="SessionResumed"/> stays <see langword="false"/>. Resumption is an optimisation over
    /// reconnecting, never a precondition for it, so it must never be able to fail a connect that has
    /// already succeeded.
    /// </remarks>
    private async Task TryResumeSessionAsync(byte[] resumptionToken, CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        lock (_stateLock)
        {
            _pendingResume = completion;
        }

        try
        {
            var payload = new byte[1 + resumptionToken.Length];
            payload[0] = (byte)MessageType.ResumeSession;
            resumptionToken.CopyTo(payload, 1);

            await _transport!.SendAsync(payload, cancellationToken).ConfigureAwait(false);

            bool resumed = await completion.Task
                .WaitAsync(SessionResumeTimeout, cancellationToken)
                .ConfigureAwait(false);

            if (resumed)
            {
                _logger.LogInformation("Resumed previous session as {ClientId}", Id);
            }
            else
            {
                _logger.LogInformation(
                    "The hub refused session resumption; continuing as {ClientId}", Id);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            || !cancellationToken.IsCancellationRequested)
        {
            // Includes the timeout. Swallowed rather than rethrown: the connection itself is up and
            // usable, and failing it here would turn an optimisation into a reason not to connect at
            // all. The token registration just issued is left in place — it belongs to *this* identity,
            // not the one that was refused, so the next reconnect still has something to present.
            _logger.LogDebug(ex, "Session resumption did not complete; continuing as {ClientId}", Id);
        }
        finally
        {
            lock (_stateLock)
            {
                if (ReferenceEquals(_pendingResume, completion))
                {
                    _pendingResume = null;
                }
            }
        }
    }

    /// <summary>
    /// Completes a pending resume attempt, if one is outstanding.
    /// </summary>
    private void CompletePendingResume(bool resumed)
    {
        TaskCompletionSource<bool>? completion;

        lock (_stateLock)
        {
            completion = _pendingResume;
            _pendingResume = null;
        }

        completion?.TrySetResult(resumed);
    }

    /// <summary>
    /// Repopulates <see cref="_joinedGroups"/> from the group block a <see cref="Protocol.SessionResumedGroupsMinVersion"/>
    /// or later hub appends to a <see cref="MessageType.SessionResumed"/> reply, so <see cref="JoinedGroups"/>
    /// reflects the memberships the hub actually restored rather than staying empty from the
    /// <see cref="CleanUpAsync"/> clear the preceding disconnect left behind (issue #109).
    /// </summary>
    /// <remarks>
    /// A negotiated version below <see cref="Protocol.SessionResumedGroupsMinVersion"/>, or a reply too
    /// short to carry the block at all, leaves <see cref="_joinedGroups"/> untouched: the hub restored the
    /// memberships either way, this client-side record just cannot learn what they were from the reply
    /// itself in that case, exactly as it could not before this method existed.
    /// </remarks>
    /// <param name="data">The full <see cref="MessageType.SessionResumed"/> frame.</param>
    /// <param name="groupsOffset">The offset immediately after the resumption token, where the group block begins.</param>
    private void RestoreJoinedGroupsFromResumedReply(byte[] data, int groupsOffset)
    {
        if (NegotiatedProtocolVersion < Protocol.SessionResumedGroupsMinVersion
            || data.Length < groupsOffset + 2)
        {
            return;
        }

        int groupCount = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(groupsOffset, 2));
        int offset = groupsOffset + 2;
        var restoredGroups = new List<string>(groupCount);

        for (int i = 0; i < groupCount; i++)
        {
            if (data.Length < offset + 2)
            {
                // Truncated block: something upstream mangled the frame. Leave the membership record as
                // it was rather than acting on a partial read.
                return;
            }

            int nameLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(offset, 2));
            offset += 2;

            if (data.Length < offset + nameLength)
            {
                return;
            }

            restoredGroups.Add(Encoding.UTF8.GetString(data.AsSpan(offset, nameLength)));
            offset += nameLength;
        }

        lock (_groupMembershipLock)
        {
            _joinedGroups.Clear();

            foreach (string groupName in restoredGroups)
            {
                _joinedGroups.Add(groupName);
            }
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
            SessionResumed = false;
            _state = ConnectionState.Disconnected;

            // Part-assembled transfers cannot be completed by a different connection: a sender's chunk
            // ids are only meaningful within the session that issued them, so holding them past
            // disconnect would keep memory for a completion that can never arrive.
            _reassembler.Clear();
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

    /// <inheritdoc/>
    public async Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        DeliveryOptions options,
        CancellationToken cancellationToken = default)
    {
        if (!options.RequireAcknowledgement)
        {
            if (!options.AwaitCapacity && options.Priority == MessagePriority.Normal)
            {
                await SendAsync(recipientId, message, cancellationToken).ConfigureAwait(false);
                return;
            }

            List<KeyValuePair<string, string>> plainHeaderEntries = [];

            if (options.AwaitCapacity)
            {
                plainHeaderEntries.Add(new KeyValuePair<string, string>(BackpressureHeaderKeys.AwaitCapacity, "1"));
            }

            if (options.Priority != MessagePriority.Normal)
            {
                plainHeaderEntries.Add(new KeyValuePair<string, string>(
                    MessagePriorityHeaderKeys.Priority, MessagePriorityHeaderKeys.ToHeaderValue(options.Priority)));
            }

            var plainHeaders = new MessageHeaders(plainHeaderEntries);

            await SendCoreAsync(recipientId, message, plainHeaders, cancellationToken).ConfigureAwait(false);
            return;
        }

        long ackId = Interlocked.Increment(ref _ackCorrelationId);
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // As RequestAsync's PendingRequest: the expected acknowledger is recorded so a forged
        // acknowledgement from a client other than the one this message was addressed to cannot
        // convince the sender delivery succeeded when it did not.
        _pendingAcks[ackId] = new PendingAck(recipientId, completion);

        try
        {
            List<KeyValuePair<string, string>> headerEntries =
            [
                new(
                    DeliveryAcknowledgementHeaderKeys.CorrelationId,
                    ackId.ToString(CultureInfo.InvariantCulture)),
                new(DeliveryAcknowledgementHeaderKeys.Request, "1"),
            ];

            if (options.AwaitCapacity)
            {
                headerEntries.Add(new KeyValuePair<string, string>(BackpressureHeaderKeys.AwaitCapacity, "1"));
            }

            if (options.Priority != MessagePriority.Normal)
            {
                headerEntries.Add(new KeyValuePair<string, string>(
                    MessagePriorityHeaderKeys.Priority, MessagePriorityHeaderKeys.ToHeaderValue(options.Priority)));
            }

            var headers = new MessageHeaders(headerEntries);

            await SendCoreAsync(recipientId, message, headers, cancellationToken).ConfigureAwait(false);

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(options.AcknowledgementTimeout!.Value);

            try
            {
                await completion.Task.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException(
                    $"No delivery acknowledgement was received within {options.AcknowledgementTimeout}.");
            }
        }
        finally
        {
            // Whether the acknowledgement arrived, the call timed out, or it was cancelled, this id is
            // no longer awaited — a late acknowledgement for it is discarded rather than resolving a
            // future send that happens to reuse it.
            _pendingAcks.TryRemove(ackId, out _);
        }
    }

    /// <inheritdoc/>
    public async Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default)
    {
        if (timeToLive <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeToLive), "The time-to-live must be positive.");
        }

        long expiresAtUnixMilliseconds = DateTimeOffset.UtcNow.Add(timeToLive).ToUnixTimeMilliseconds();
        var headers = new MessageHeaders(
        [
            new KeyValuePair<string, string>(
                MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds,
                expiresAtUnixMilliseconds.ToString(CultureInfo.InvariantCulture)),
        ]);

        await SendCoreAsync(recipientId, message, headers, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds and sends a direct message frame, with or without a header block. Shared by the public
    /// <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, MessageHeaders, CancellationToken)"/> and by
    /// <see cref="RequestAsync(Guid, ReadOnlyMemory{byte}, TimeSpan, CancellationToken)"/>,
    /// <see cref="ReplyAsync(MessageReceivedEventArgs, ReadOnlyMemory{byte}, CancellationToken)"/>,
    /// <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, DeliveryOptions, CancellationToken)"/>,
    /// <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, TimeSpan, CancellationToken)"/> and the
    /// acknowledgement send in the receive loop, all of which construct
    /// <see cref="RequestReplyHeaderKeys"/>, <see cref="DeliveryAcknowledgementHeaderKeys"/> or
    /// <see cref="MessageExpiryHeaderKeys"/> headers themselves and so must bypass the public overload's
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

        using Activity? activity = MeshworxActivitySource.Instance.StartActivity(
            MeshworxActivitySource.SendActivityName, ActivityKind.Producer);

        if (activity is not null)
        {
            activity.SetTag("meshworx.recipient_id", recipientId);
            activity.SetTag("meshworx.message_size", message.Length);
        }

        headers = WithTraceContext(headers, activity);

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

    // Every header key a built-in helper (request/response, delivery acknowledgement, time-to-live, and
    // any future one following the same pattern) reserves for its own use. Kept as a single list rather
    // than a growing chain of ContainsKey checks, so ThrowIfReservedHeaderKeyPresent stays a fixed shape
    // as more helpers are added.
    private static readonly string[] ReservedHeaderKeys =
    [
        RequestReplyHeaderKeys.CorrelationId,
        RequestReplyHeaderKeys.Reply,
        DeliveryAcknowledgementHeaderKeys.CorrelationId,
        DeliveryAcknowledgementHeaderKeys.Request,
        DeliveryAcknowledgementHeaderKeys.Ack,
        MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds,
        BackpressureHeaderKeys.AwaitCapacity,
        MessagePriorityHeaderKeys.Priority,

        // Reserved for the same reason as the rest: these are written from the ambient Activity on
        // every traced send, so an application setting them by hand would have its value silently
        // replaced — or, worse, kept on a send that happened not to be traced, putting a stale trace id
        // on a message that belongs to a different operation entirely.
        TraceContextHeaderKeys.TraceParent,
        TraceContextHeaderKeys.TraceState,

        // Reserved because the receive loop acts on these before a message is ever raised: a header
        // literally named mesh.chunk.id/index/count would be read by TryReadChunkHeaders as real
        // reassembly metadata, and the message carrying it would be silently absorbed into the
        // reassembler — and, if a chunk count and index happened to be internally consistent, held
        // against a logical message that never arrives the rest of, until the transfer timeout frees it.
        ChunkHeaderKeys.Id,
        ChunkHeaderKeys.Index,
        ChunkHeaderKeys.Count,
    ];

    /// <summary>
    /// Guards against an application header colliding with one of <see cref="ReservedHeaderKeys"/>.
    /// Without this, a message that happened to carry a header literally named, for example,
    /// <c>mesh.reply</c> with value <c>"1"</c> would be silently intercepted by the receive loop as if
    /// it were answering a request, and never raised through <see cref="MessageReceived"/> at all.
    /// </summary>
    private static void ThrowIfReservedHeaderKeyPresent(MessageHeaders headers)
    {
        foreach (string reservedKey in ReservedHeaderKeys)
        {
            if (headers.ContainsKey(reservedKey))
            {
                throw new ArgumentException(
                    $"The header key '{reservedKey}' is reserved for a built-in helper (request/response, "
                    + "delivery acknowledgement, time-to-live, backpressure, or priority) and cannot be set "
                    + "directly.",
                    nameof(headers));
            }
        }
    }

    /// <inheritdoc/>
    /// <inheritdoc/>
    public async Task SendLargeAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        MessageHeaders? headers = null,
        CancellationToken cancellationToken = default)
    {
        headers ??= MessageHeaders.Empty;
        ThrowIfReservedHeaderKeyPresent(headers);

        // Explicit rather than degrading, unlike trace context: a caller reaching for this method is
        // asking to send something that cannot go any other way, so a peer that cannot receive it is a
        // failed send rather than a quiet loss of an optional extra.
        RequireHeaderEnvelopeSupport(NegotiatedProtocolVersion);

        int chunkCount = (message.Length + MaxChunkBodySize - 1) / MaxChunkBodySize;

        // A zero-length payload is still one chunk. Sending none would complete no transfer at the far
        // end, so an empty large-send would silently never arrive.
        chunkCount = Math.Max(chunkCount, 1);

        if (chunkCount > ChunkHeaderKeys.MaxChunksPerMessage)
        {
            throw new ArgumentException(
                $"The message is too large to chunk: it needs {chunkCount} chunks, and the maximum is "
                + $"{ChunkHeaderKeys.MaxChunksPerMessage}.",
                nameof(message));
        }

        var chunkId = Guid.NewGuid();

        for (int index = 0; index < chunkCount; index++)
        {
            int offset = index * MaxChunkBodySize;
            int length = Math.Min(MaxChunkBodySize, message.Length - offset);

            var chunkHeaders = new Dictionary<string, string>(headers.Count + 3, StringComparer.Ordinal);

            foreach (KeyValuePair<string, string> header in headers)
            {
                chunkHeaders[header.Key] = header.Value;
            }

            chunkHeaders[ChunkHeaderKeys.Id] = chunkId.ToString("D", CultureInfo.InvariantCulture);
            chunkHeaders[ChunkHeaderKeys.Index] = index.ToString(CultureInfo.InvariantCulture);
            chunkHeaders[ChunkHeaderKeys.Count] = chunkCount.ToString(CultureInfo.InvariantCulture);

            await SendCoreAsync(
                    recipientId,
                    message.Slice(offset, length),
                    MessageHeaders.FromOwnedDictionary(chunkHeaders),
                    cancellationToken)
                .ConfigureAwait(false);
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

        using Activity? activity = MeshworxActivitySource.Instance.StartActivity(
            MeshworxActivitySource.SendActivityName, ActivityKind.Producer);

        if (activity is not null)
        {
            activity.SetTag("meshworx.group_name", groupName);
            activity.SetTag("meshworx.message_size", message.Length);
        }

        headers = WithTraceContext(headers, activity);

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

    /// <inheritdoc/>
    public Task SendToGroupAsync(
        string groupName,
        ReadOnlyMemory<byte> message,
        MessagePriority priority,
        CancellationToken cancellationToken = default)
    {
        if (priority == MessagePriority.Normal)
        {
            // No header block is written at all — byte-for-byte identical to the headerless overload.
            return SendToGroupAsync(groupName, message, cancellationToken);
        }

        var headers = new MessageHeaders(
        [
            new KeyValuePair<string, string>(MessagePriorityHeaderKeys.Priority, MessagePriorityHeaderKeys.ToHeaderValue(priority)),
        ]);

        return SendToGroupAsync(groupName, message, headers, cancellationToken);
    }

    /// <inheritdoc/>
    public Task SendToGroupAsync(
        string groupName,
        ReadOnlyMemory<byte> message,
        bool retain,
        CancellationToken cancellationToken = default)
    {
        if (!retain)
        {
            // No header block is written at all — byte-for-byte identical to the headerless overload.
            return SendToGroupAsync(groupName, message, cancellationToken);
        }

        var headers = new MessageHeaders(
        [
            new KeyValuePair<string, string>(RetainHeaderKeys.Retain, "1"),
        ]);

        return SendToGroupAsync(groupName, message, headers, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task SubscribeAsync(string pattern, CancellationToken cancellationToken = default)
    {
        TopicSubscriptionTrie.ValidatePattern(pattern);

        ITransport transport = GetConnectedTransport();
        RequireTopicPubSubSupport(NegotiatedProtocolVersion);

        // Recorded before the frame goes out, mirroring JoinGroupAsync — there is no authorisation hook
        // for a subscription that could refuse it after the fact, but recording first keeps the two
        // methods symmetric and means a concurrent Unsubscribe of the same pattern can never race a
        // record made after this method's own send.
        bool recorded;
        lock (_topicSubscriptionLock)
        {
            recorded = _subscribedTopics.Add(pattern);
        }

        try
        {
            await SendTopicSubscriptionAsync(transport, MessageType.SubscribeTopic, pattern, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            if (recorded)
            {
                lock (_topicSubscriptionLock)
                {
                    _subscribedTopics.Remove(pattern);
                }
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public async Task UnsubscribeAsync(string pattern, CancellationToken cancellationToken = default)
    {
        TopicSubscriptionTrie.ValidatePattern(pattern);

        ITransport transport = GetConnectedTransport();
        RequireTopicPubSubSupport(NegotiatedProtocolVersion);

        await SendTopicSubscriptionAsync(transport, MessageType.UnsubscribeTopic, pattern, cancellationToken)
            .ConfigureAwait(false);

        lock (_topicSubscriptionLock)
        {
            _subscribedTopics.Remove(pattern);
        }
    }

    /// <inheritdoc/>
    public Task PublishAsync(
        string topic, ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default)
    {
        return PublishAsync(topic, message, MessageHeaders.Empty, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task PublishAsync(
        string topic,
        ReadOnlyMemory<byte> message,
        MessageHeaders headers,
        CancellationToken cancellationToken = default)
    {
        TopicSubscriptionTrie.ValidateTopic(topic);
        ArgumentNullException.ThrowIfNull(headers);

        ITransport transport = GetConnectedTransport();
        RequireTopicPubSubSupport(NegotiatedProtocolVersion);

        using Activity? activity = MeshworxActivitySource.Instance.StartActivity(
            MeshworxActivitySource.SendActivityName, ActivityKind.Producer);

        if (activity is not null)
        {
            activity.SetTag("meshworx.topic", topic);
            activity.SetTag("meshworx.message_size", message.Length);
        }

        headers = WithTraceContext(headers, activity);

        byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
        if (topicBytes.Length > ushort.MaxValue)
        {
            throw new ArgumentException("The topic is too long.", nameof(topic));
        }

        byte[] payload;
        if (headers.Count == 0)
        {
            // No header block is written at all when there is nothing to carry, mirroring
            // SendToGroupAsync's own headerless fast path.
            payload = new byte[1 + 2 + topicBytes.Length + message.Length];
            payload[0] = (byte)MessageType.PublishTopicMessage;
            BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(1, 2), (ushort)topicBytes.Length);
            topicBytes.CopyTo(payload, 3);
            message.CopyTo(payload.AsMemory(3 + topicBytes.Length));
        }
        else
        {
            RequireHeaderEnvelopeSupport(NegotiatedProtocolVersion);

            int headerLength = HeaderEnvelope.GetEncodedLength(headers);
            int headerLengthOffset = 3 + topicBytes.Length;
            payload = new byte[headerLengthOffset + 2 + headerLength + message.Length];
            payload[0] = (byte)MessageType.PublishTopicMessageWithHeaders;
            BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(1, 2), (ushort)topicBytes.Length);
            topicBytes.CopyTo(payload, 3);
            BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(headerLengthOffset, 2), (ushort)headerLength);
            HeaderEnvelope.Write(headers, payload.AsSpan(headerLengthOffset + 2, headerLength));
            message.CopyTo(payload.AsMemory(headerLengthOffset + 2 + headerLength));
        }

        await SendWithPolicyAsync(transport, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task PublishAsync(
        string topic,
        ReadOnlyMemory<byte> message,
        bool retain,
        CancellationToken cancellationToken = default)
    {
        if (!retain)
        {
            // No header block is written at all — byte-for-byte identical to the headerless overload.
            return PublishAsync(topic, message, cancellationToken);
        }

        var headers = new MessageHeaders(
        [
            new KeyValuePair<string, string>(RetainHeaderKeys.Retain, "1"),
        ]);

        return PublishAsync(topic, message, headers, cancellationToken);
    }

    private static async Task SendTopicSubscriptionAsync(
        ITransport transport,
        MessageType type,
        string pattern,
        CancellationToken cancellationToken)
    {
        byte[] patternBytes = Encoding.UTF8.GetBytes(pattern);
        var payload = new byte[1 + patternBytes.Length];
        payload[0] = (byte)type;
        patternBytes.CopyTo(payload, 1);

        await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Guards a header-bearing send against a connection that negotiated a protocol version predating
    /// the header envelope, so headers the caller supplied are never silently dropped on the wire.
    /// </summary>
    /// <summary>
    /// Starts the consumer span for a received message, continuing the sender's trace when the message
    /// carries one.
    /// </summary>
    /// <remarks>
    /// Returns <see langword="null"/> when nothing is listening, which is the ordinary case and costs
    /// nothing. A message with no trace context still gets a span when a listener is attached — it
    /// simply starts a new trace rather than continuing one, which is the honest representation of a
    /// message that arrived from an untraced sender.
    /// </remarks>
    private static Activity? StartReceiveActivity(MessageHeaders headers, Guid senderId)
    {
        Activity? activity = TraceContextHeaderKeys.TryExtractTraceContext(headers, out ActivityContext parent)
            ? MeshworxActivitySource.Instance.StartActivity(
                MeshworxActivitySource.ReceiveActivityName, ActivityKind.Consumer, parent)
            : MeshworxActivitySource.Instance.StartActivity(
                MeshworxActivitySource.ReceiveActivityName, ActivityKind.Consumer);

        activity?.SetTag("meshworx.sender_id", senderId);
        return activity;
    }

    /// <summary>
    /// Returns the caller's headers with W3C trace context added, when there is context to propagate
    /// and this connection can carry it.
    /// </summary>
    /// <remarks>
    /// The context comes from this library's own send span when a listener created one, and otherwise
    /// from the ambient <see cref="Activity.Current"/> — a message sent inside an application's existing
    /// span should join that trace whether or not anyone is listening to <em>this</em> library
    /// specifically. When neither exists, which is the case for every send in a process with no tracing
    /// at all, this returns the caller's headers untouched and the frame is byte-for-byte what it was
    /// before tracing existed.
    /// <para>
    /// The version gate is the important part. Adding a header to a previously header-free send turns it
    /// into a header-bearing frame, and
    /// <see cref="RequireHeaderEnvelopeSupport(byte)"/> throws for a connection that negotiated below
    /// <see cref="Protocol.HeaderEnvelopeMinVersion"/>. Without this check, merely attaching a tracing
    /// listener would start throwing on sends to an older peer that worked perfectly a moment earlier —
    /// observability breaking delivery, which is precisely backwards. Tracing degrades instead: the
    /// context is dropped, the message goes out exactly as it always did.
    /// </para>
    /// <para>
    /// The caller's <see cref="MessageHeaders"/> is immutable and routinely reused across sends, so the
    /// trace context is added to a copy rather than written into it.
    /// </para>
    /// </remarks>
    private MessageHeaders WithTraceContext(MessageHeaders headers, Activity? activity)
    {
        if (NegotiatedProtocolVersion < Protocol.HeaderEnvelopeMinVersion)
        {
            return headers;
        }

        if (!TraceContextHeaderKeys.TryGetTraceContext(
                activity ?? Activity.Current, out string traceParent, out string? traceState))
        {
            return headers;
        }

        var merged = new Dictionary<string, string>(headers.Count + 2, StringComparer.Ordinal);

        foreach (KeyValuePair<string, string> header in headers)
        {
            merged[header.Key] = header.Value;
        }

        merged[TraceContextHeaderKeys.TraceParent] = traceParent;

        if (!string.IsNullOrEmpty(traceState))
        {
            merged[TraceContextHeaderKeys.TraceState] = traceState;
        }

        return MessageHeaders.FromOwnedDictionary(merged);
    }

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
    /// Guards every topic pub/sub call against a connection that negotiated a protocol version predating
    /// the feature, so a client built with topic support talking to an older hub fails fast and audibly
    /// rather than having its subscribe or publish frame silently go unrecognised by a peer that has never
    /// heard of the opcode.
    /// </summary>
    private static void RequireTopicPubSubSupport(byte negotiatedProtocolVersion)
    {
        if (negotiatedProtocolVersion < Protocol.TopicPubSubMinVersion)
        {
            throw new NotSupportedException(
                $"Topic pub/sub requires a negotiated protocol version of at least "
                + $"{Protocol.TopicPubSubMinVersion}; this connection negotiated version "
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
    public async Task UpdateAttributesAsync(
        IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(attributes);
        ValidateAttributes(attributes);

        ITransport transport = GetConnectedTransport();
        RequireClientAttributesSupport(NegotiatedProtocolVersion);

        var headers = new MessageHeaders(attributes);
        int blockLength = HeaderEnvelope.GetEncodedLength(headers);
        var payload = new byte[1 + blockLength];
        payload[0] = (byte)MessageType.SetClientAttributes;
        HeaderEnvelope.Write(headers, payload.AsSpan(1, blockLength));

        await SendWithPolicyAsync(transport, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Validates an attribute bag against the same bounds the hub enforces, so a caller learns
    /// immediately and locally that a bag is too large rather than having it silently dropped by the hub.
    /// </summary>
    private static void ValidateAttributes(IReadOnlyDictionary<string, string> attributes)
    {
        if (attributes.Count > Protocol.MaxClientAttributeCount)
        {
            throw new ArgumentException(
                $"An attribute bag cannot hold more than {Protocol.MaxClientAttributeCount} entries.",
                nameof(attributes));
        }

        foreach (KeyValuePair<string, string> attribute in attributes)
        {
            if (Encoding.UTF8.GetByteCount(attribute.Key) > Protocol.MaxClientAttributeKeyLength)
            {
                throw new ArgumentException(
                    $"Attribute key '{attribute.Key}' exceeds the maximum length of "
                    + $"{Protocol.MaxClientAttributeKeyLength} UTF-8 bytes.",
                    nameof(attributes));
            }

            if (Encoding.UTF8.GetByteCount(attribute.Value) > Protocol.MaxClientAttributeValueLength)
            {
                throw new ArgumentException(
                    $"The value for attribute key '{attribute.Key}' exceeds the maximum length of "
                    + $"{Protocol.MaxClientAttributeValueLength} UTF-8 bytes.",
                    nameof(attributes));
            }
        }
    }

    private static void RequireClientAttributesSupport(byte negotiatedProtocolVersion)
    {
        if (negotiatedProtocolVersion < Protocol.ClientAttributesMinVersion)
        {
            throw new NotSupportedException(
                $"Client attributes require a negotiated protocol version of at least "
                + $"{Protocol.ClientAttributesMinVersion}; this connection negotiated version "
                + $"{negotiatedProtocolVersion}.");
        }
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<ClientDescriptor>> FindClientsAsync(
        AttributeQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        ITransport transport = GetConnectedTransport();
        RequireClientAttributesSupport(NegotiatedProtocolVersion);

        await _findClientsLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // _findClientsLock serialises queries, so a plain increment is sufficient.
            int correlationId = unchecked(_findClientsCorrelationId++);
            var completion = new TaskCompletionSource<IReadOnlyList<ClientDescriptor>>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingFindClients = new PendingFindClients(correlationId, completion);

            var criteria = new MessageHeaders(query);
            int blockLength = HeaderEnvelope.GetEncodedLength(criteria);
            var payload = new byte[1 + 4 + blockLength];
            payload[0] = (byte)MessageType.FindClientsRequest;
            BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(1, 4), correlationId);
            HeaderEnvelope.Write(criteria, payload.AsSpan(5, blockLength));
            await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);

            return await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _pendingFindClients = null;

            try
            {
                _findClientsLock.Release();
            }
            catch (ObjectDisposedException)
            {
                // The semaphore was disposed during a concurrent DisposeAsync call.
            }
        }
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<ClientDescriptor>> GetClientsAsync(CancellationToken cancellationToken = default)
    {
        return FindClientsAsync(new AttributeQuery([]), cancellationToken);
    }

    /// <inheritdoc/>
    public async Task SubscribePresenceAsync(CancellationToken cancellationToken = default)
    {
        ITransport transport = GetConnectedTransport();
        RequirePresenceSupport(NegotiatedProtocolVersion);

        byte[] payload = [(byte)MessageType.SubscribePresence];
        await SendWithPolicyAsync(transport, payload, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task UnsubscribePresenceAsync(CancellationToken cancellationToken = default)
    {
        ITransport transport = GetConnectedTransport();
        RequirePresenceSupport(NegotiatedProtocolVersion);

        byte[] payload = [(byte)MessageType.UnsubscribePresence];
        await SendWithPolicyAsync(transport, payload, cancellationToken).ConfigureAwait(false);
    }

    private static void RequirePresenceSupport(byte negotiatedProtocolVersion)
    {
        if (negotiatedProtocolVersion < Protocol.PresenceMinVersion)
        {
            throw new NotSupportedException(
                $"Presence requires a negotiated protocol version of at least "
                + $"{Protocol.PresenceMinVersion}; this connection negotiated version "
                + $"{negotiatedProtocolVersion}.");
        }
    }

    /// <inheritdoc/>
    public Task<ReadOnlyMemory<byte>> RequestAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        return RequestAsync(recipientId, message, timeout, MessageHeaders.Empty, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<ReadOnlyMemory<byte>> RequestAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        TimeSpan timeout,
        MessageHeaders headers,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(headers);
        ThrowIfReservedHeaderKeyPresent(headers);

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
            MessageHeaders requestHeaders = WithReservedHeaders(
                headers,
                new KeyValuePair<string, string>(
                    RequestReplyHeaderKeys.CorrelationId,
                    correlationId.ToString(CultureInfo.InvariantCulture)));

            await SendCoreAsync(recipientId, message, requestHeaders, cancellationToken)
                .ConfigureAwait(false);

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
        return ReplyAsync(request, message, MessageHeaders.Empty, cancellationToken);
    }

    /// <inheritdoc/>
    public Task ReplyAsync(
        MessageReceivedEventArgs request,
        ReadOnlyMemory<byte> message,
        MessageHeaders headers,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(headers);
        ThrowIfReservedHeaderKeyPresent(headers);

        if (request.CorrelationId is not { } correlationId)
        {
            throw new InvalidOperationException(
                "The supplied message was not a request and cannot be replied to.");
        }

        MessageHeaders replyHeaders = WithReservedHeaders(
            headers,
            new KeyValuePair<string, string>(
                RequestReplyHeaderKeys.CorrelationId,
                correlationId.ToString(CultureInfo.InvariantCulture)),
            new KeyValuePair<string, string>(RequestReplyHeaderKeys.Reply, "1"));

        return SendCoreAsync(request.SenderId, message, replyHeaders, cancellationToken);
    }

    /// <summary>
    /// Returns the caller's headers with a built-in helper's own headers added, without mutating the
    /// original.
    /// </summary>
    /// <remarks>
    /// A caller's <see cref="MessageHeaders"/> is immutable and may be reused across many sends, so the
    /// reserved keys are added to a copy — the same shape <see cref="WithTraceContext"/> uses for the
    /// same reason. The caller's own entries cannot collide with the added ones:
    /// <see cref="ThrowIfReservedHeaderKeyPresent"/> has already rejected any that would.
    /// </remarks>
    private static MessageHeaders WithReservedHeaders(
        MessageHeaders headers, params KeyValuePair<string, string>[] reserved)
    {
        if (headers.Count == 0)
        {
            return new MessageHeaders(reserved);
        }

        var merged = new Dictionary<string, string>(headers.Count + reserved.Length, StringComparer.Ordinal);

        foreach (KeyValuePair<string, string> header in headers)
        {
            merged[header.Key] = header.Value;
        }

        foreach (KeyValuePair<string, string> header in reserved)
        {
            merged[header.Key] = header.Value;
        }

        return MessageHeaders.FromOwnedDictionary(merged);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
        _lookupLock.Dispose();
        _findClientsLock.Dispose();
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

        lock (_topicSubscriptionLock)
        {
            _subscribedTopics.Clear();
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

                            // A chunk is not a message yet. It is absorbed here and raises nothing
                            // until its last sibling arrives, at which point the reassembled whole is
                            // raised once — so a subscriber sees large messages exactly as it sees
                            // small ones, never a partial one.
                            if (ChunkHeaderKeys.TryReadChunkHeaders(
                                    headers, out Guid chunkId, out int chunkIndex, out int chunkCount))
                            {
                                if (_reassembler.TryAddChunk(
                                        senderId,
                                        chunkId,
                                        chunkIndex,
                                        chunkCount,
                                        messageData,
                                        out byte[]? reassembled))
                                {
                                    messageData = reassembled;

                                    // The chunk keys are per-chunk bookkeeping, meaningless once
                                    // reassembly is done — a subscriber must see exactly the headers the
                                    // sender passed to SendLargeAsync, not the internal reassembly
                                    // metadata every individual chunk carried.
                                    headers = ChunkHeaderKeys.WithoutChunkHeaders(headers);
                                }
                                else
                                {
                                    continue;
                                }
                            }

                            if (!TryCompletePendingAck(senderId, headers)
                                && !TryCompletePendingRequest(senderId, headers, messageData)
                                && !IsExpired(headers, senderId))
                            {
                                // The consumer span covers the handler, not just the frame's arrival:
                                // what a trace is being read to answer is how long the application took
                                // to deal with the message, and that is the subscriber's work below.
                                using (Activity? receiveActivity = StartReceiveActivity(headers, senderId))
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
                                        receiveActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                                    }
                                }

                                // The acknowledgement is sent once the message has been handed to the
                                // application (the event above has been raised, successfully or not),
                                // matching the "delivered-to-application receipt" contract — not merely
                                // that the frame arrived on the wire. Fired and forgotten rather than
                                // awaited: a slow or blocked write back to the peer (write-side
                                // backpressure, a stalled socket) must not head-of-line-block this
                                // connection's own inbound frame processing — including its Ping/Pong
                                // keepalive — behind it. TrySendAcknowledgementAsync handles every
                                // failure internally, so nothing here needs to observe the result.
                                _ = TrySendAcknowledgementAsync(senderId, headers, cancellationToken);
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

                            if (headers is not null && !IsExpired(headers, senderId))
                            {
                                ReadOnlyMemory<byte> messageData = data.AsMemory(bodyOffset);

                                using (Activity? receiveActivity = StartReceiveActivity(headers, senderId))
                                {
                                    receiveActivity?.SetTag("meshworx.group_name", groupName);

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
                                        receiveActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                                    }
                                }
                            }
                        }
                    }
                }
                else if (data.Length >= 19
                    && (MessageType)data[0] == MessageType.DeliverTopicMessage)
                {
                    var senderId = new Guid(data.AsSpan(1, 16));
                    int topicLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(17, 2));

                    if (data.Length >= 19 + topicLength)
                    {
                        string topic = Encoding.UTF8.GetString(data.AsSpan(19, topicLength));
                        ReadOnlyMemory<byte> messageData = data.AsMemory(19 + topicLength);

                        try
                        {
                            TopicMessageReceived?.Invoke(this, new TopicMessageReceivedEventArgs
                            {
                                SenderId = senderId,
                                Topic = topic,
                                Data = messageData,
                            });
                        }
                        catch (Exception ex)
                        {
                            // Callback boundary — a throwing subscriber must not halt the loop.
                            _logger.LogError(ex, "A TopicMessageReceived handler threw an exception");
                        }
                    }
                }
                else if (data.Length >= 21
                    && (MessageType)data[0] == MessageType.DeliverTopicMessageWithHeaders)
                {
                    var senderId = new Guid(data.AsSpan(1, 16));
                    int topicLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(17, 2));
                    int headerLengthOffset = 19 + topicLength;

                    if (data.Length >= headerLengthOffset + 2)
                    {
                        int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(
                            data.AsSpan(headerLengthOffset, 2));
                        int bodyOffset = headerLengthOffset + 2 + headerBlockLength;

                        if (data.Length >= bodyOffset)
                        {
                            string topic = Encoding.UTF8.GetString(data.AsSpan(19, topicLength));
                            MessageHeaders? headers = TryReadHeaderBlock(
                                data.AsSpan(headerLengthOffset + 2), headerBlockLength, senderId);

                            if (headers is not null && !IsExpired(headers, senderId))
                            {
                                ReadOnlyMemory<byte> messageData = data.AsMemory(bodyOffset);

                                using (Activity? receiveActivity = StartReceiveActivity(headers, senderId))
                                {
                                    receiveActivity?.SetTag("meshworx.topic", topic);

                                    try
                                    {
                                        TopicMessageReceived?.Invoke(this, new TopicMessageReceivedEventArgs
                                        {
                                            SenderId = senderId,
                                            Topic = topic,
                                            Data = messageData,
                                            Headers = headers,
                                        });
                                    }
                                    catch (Exception ex)
                                    {
                                        // Callback boundary — a throwing subscriber must not halt the loop.
                                        _logger.LogError(ex, "A TopicMessageReceived handler threw an exception");
                                        receiveActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                                    }
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
                else if (data.Length >= 7
                    && (MessageType)data[0] == MessageType.FindClientsResponse)
                {
                    int correlationId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(1, 4));
                    PendingFindClients? pending = _pendingFindClients;

                    if (pending is null || pending.CorrelationId != correlationId)
                    {
                        // Stale or unsolicited response (e.g. from a cancelled query); discard.
                        _logger.LogDebug(
                            "Discarding find-clients response with unmatched correlation id {CorrelationId}",
                            correlationId);
                    }
                    else if (TryReadClientDescriptors(data.AsSpan(5), out List<ClientDescriptor>? results))
                    {
                        pending.Completion.TrySetResult(results);
                    }
                    else
                    {
                        _logger.LogWarning("Discarding a malformed find-clients response");
                    }
                }
                else if (data.Length >= 20
                    && (MessageType)data[0] == MessageType.PresenceChanged)
                {
                    var changeType = (PresenceChangeType)data[1];
                    var presenceClientId = new Guid(data.AsSpan(2, 16));
                    int nameLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(18, 2));

                    if (data.Length >= 20 + nameLength
                        && changeType is PresenceChangeType.Joined or PresenceChangeType.Left)
                    {
                        string presenceClientName = Encoding.UTF8.GetString(data.AsSpan(20, nameLength));

                        try
                        {
                            PresenceChanged?.Invoke(this, new PresenceChangedEventArgs
                            {
                                ClientId = presenceClientId,
                                ClientName = presenceClientName,
                                ChangeType = changeType,
                            });
                        }
                        catch (Exception ex)
                        {
                            // Callback boundary — a throwing subscriber must not halt the loop.
                            _logger.LogError(ex, "A PresenceChanged handler threw an exception");
                        }
                    }
                }
                else if (data.Length >= 17
                    && (MessageType)data[0] == MessageType.QueueSaturated)
                {
                    var saturatedRecipientId = new Guid(data.AsSpan(1, 16));

                    try
                    {
                        SendRejected?.Invoke(
                            this, new SendRejectedEventArgs { RecipientId = saturatedRecipientId });
                    }
                    catch (Exception ex)
                    {
                        // Callback boundary — a throwing subscriber must not halt the loop.
                        _logger.LogError(ex, "A SendRejected handler threw an exception");
                    }
                }
                else if (data.Length >= 19
                    && (MessageType)data[0] == MessageType.SessionResumed)
                {
                    var resumedId = new Guid(data.AsSpan(1, 16));
                    int tokenLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(17, 2));

                    if (data.Length >= 19 + tokenLength && tokenLength > 0)
                    {
                        lock (_stateLock)
                        {
                            Id = resumedId;
                            SessionResumed = true;
                            _sessionToken = data.AsSpan(19, tokenLength).ToArray();
                            _sessionTokenName = Name;
                        }

                        RestoreJoinedGroupsFromResumedReply(data, 19 + tokenLength);
                        CompletePendingResume(true);
                    }
                }
                else if ((MessageType)data[0] == MessageType.SessionResumeRefused)
                {
                    // The old identity is gone for good — expired, already reclaimed, or never this
                    // hub's to give. The token held now is not that one: registration replaced it with a
                    // fresh token for the identity this connection *does* have, so there is nothing to
                    // clear and the next reconnect has something valid to present.
                    CompletePendingResume(false);
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

            // Likewise the only thing that completes a pending FindClientsAsync query.
            _pendingFindClients?.Completion.TrySetException(
                new InvalidOperationException("The connection was closed before the query completed."));

            // Likewise the only thing that completes a pending RequestAsync call: fault every
            // still-outstanding request so a caller awaiting with a non-cancellable token is not left
            // hanging, then clear the table so nothing here is mistaken for a match on the next connection.
            foreach (KeyValuePair<long, PendingRequest> pending in _pendingRequests)
            {
                pending.Value.Completion.TrySetException(
                    new InvalidOperationException("The connection was closed before a reply arrived."));
            }

            _pendingRequests.Clear();

            // Likewise the only thing that completes a pending SendAsync(..., DeliveryOptions.RequireAck)
            // call.
            foreach (KeyValuePair<long, PendingAck> pendingAck in _pendingAcks)
            {
                pendingAck.Value.Completion.TrySetException(
                    new InvalidOperationException("The connection was closed before an acknowledgement arrived."));
            }

            _pendingAcks.Clear();

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
            SessionResumed = false;
            _state = ConnectionState.Disconnected;

            // Part-assembled transfers cannot be completed by a different connection: a sender's chunk
            // ids are only meaningful within the session that issued them, so holding them past
            // disconnect would keep memory for a completion that can never arrive.
            _reassembler.Clear();

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
    /// Parses a <see cref="MessageType.FindClientsResponse"/> body:
    /// <c>[resultCount(2)][for each: id(16)][nameLength(2)][name]]</c>.
    /// </summary>
    /// <param name="body">The frame content immediately after the correlation id.</param>
    /// <param name="results">
    /// The parsed descriptors, or <see langword="null"/> if the body was not well formed.
    /// </param>
    /// <returns>
    /// <see langword="true"/> and the parsed descriptors if the body is well formed; otherwise
    /// <see langword="false"/>, leaving <paramref name="results"/> <see langword="null"/>.
    /// </returns>
    private static bool TryReadClientDescriptors(
        ReadOnlySpan<byte> body, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out List<ClientDescriptor>? results)
    {
        results = null;

        if (body.Length < 2)
        {
            return false;
        }

        int count = BinaryPrimitives.ReadUInt16BigEndian(body[..2]);
        int offset = 2;
        var parsed = new List<ClientDescriptor>(count);

        for (int i = 0; i < count; i++)
        {
            if (offset + 16 + 2 > body.Length)
            {
                return false;
            }

            var id = new Guid(body.Slice(offset, 16));
            offset += 16;

            int nameLength = BinaryPrimitives.ReadUInt16BigEndian(body.Slice(offset, 2));
            offset += 2;

            if (offset + nameLength > body.Length)
            {
                return false;
            }

            string name = Encoding.UTF8.GetString(body.Slice(offset, nameLength));
            offset += nameLength;

            parsed.Add(new ClientDescriptor(id, name));
        }

        results = parsed;
        return true;
    }

    /// <summary>
    /// Determines whether an incoming direct or group message has already passed the expiry its sender
    /// attached via <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, TimeSpan, CancellationToken)"/>, so
    /// it can be dropped before being handed to the application rather than delivered stale.
    /// </summary>
    /// <remarks>
    /// Absent, unparseable or out-of-range expiry data means "does not expire" — identical to a message
    /// with no time-to-live at all — mirroring <see cref="TryReadHeaderBlock"/>'s own tolerant treatment
    /// of a malformed header block: a bad expiry value is not treated as a reason to fail delivery, and
    /// critically must never itself throw and crash the receive loop over one hostile or malformed frame.
    /// See <see cref="MessageExpiryHeaderKeys.TryParseExpiry"/>.
    /// </remarks>
    private bool IsExpired(MessageHeaders headers, Guid senderId)
    {
        if (!headers.TryGetValue(MessageExpiryHeaderKeys.ExpiresAtUnixMilliseconds, out string? value)
            || !MessageExpiryHeaderKeys.TryParseExpiry(value, out DateTimeOffset expiry))
        {
            return false;
        }

        if (DateTimeOffset.UtcNow <= expiry)
        {
            return false;
        }

        _logger.LogDebug("Discarding an expired message from {SenderId}", senderId);
        return true;
    }

    /// <summary>
    /// Completes a pending <see cref="RequestAsync(Guid, ReadOnlyMemory{byte}, TimeSpan, CancellationToken)"/> call if
    /// <paramref name="headers"/> mark this frame
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
    /// Completes a pending <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, DeliveryOptions, CancellationToken)"/>
    /// call if <paramref name="headers"/> mark this frame as a delivery acknowledgement, so it is
    /// resolved internally rather than surfaced through <see cref="MessageReceived"/>.
    /// </summary>
    /// <param name="senderId">
    /// The actual sender of the frame, as stamped by the hub. Checked against the pending
    /// acknowledgement's recorded <see cref="PendingAck.ExpectedAcknowledgerId"/> before completion, so
    /// an acknowledgement cannot be forged by a client other than the one the message was addressed to.
    /// </param>
    /// <param name="headers">The frame's decoded headers.</param>
    /// <returns>
    /// <see langword="true"/> if the frame was an acknowledgement (whether or not it matched a
    /// still-pending send from the expected acknowledger), so the caller must not raise it as an
    /// ordinary message.
    /// </returns>
    private bool TryCompletePendingAck(Guid senderId, MessageHeaders headers)
    {
        if (!headers.TryGetValue(DeliveryAcknowledgementHeaderKeys.Ack, out string? ackFlag)
            || !string.Equals(ackFlag, "1", StringComparison.Ordinal))
        {
            return false;
        }

        if (!headers.TryGetValue(DeliveryAcknowledgementHeaderKeys.CorrelationId, out string? correlationText)
            || !long.TryParse(
                correlationText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long correlationId))
        {
            _logger.LogWarning("Discarding an acknowledgement frame with a missing or malformed correlation id");
            return true;
        }

        if (!_pendingAcks.TryGetValue(correlationId, out PendingAck? pending))
        {
            // The send this acknowledges has already timed out, been cancelled, or never existed on
            // this connection; discard rather than misrouting it to an unrelated later send.
            _logger.LogDebug(
                "Discarding an acknowledgement with an unmatched or expired correlation id {CorrelationId}",
                correlationId);
        }
        else if (pending.ExpectedAcknowledgerId != senderId)
        {
            // This frame claims to acknowledge a message addressed elsewhere. Left in place rather than
            // removed, so a forged acknowledgement cannot be used to strand the real send — the
            // genuinely addressed recipient's acknowledgement can still arrive and complete it.
            _logger.LogWarning(
                "Discarding an acknowledgement for correlation id {CorrelationId} from {SenderId}, which "
                + "does not match the expected recipient {ExpectedAcknowledgerId}",
                correlationId,
                senderId,
                pending.ExpectedAcknowledgerId);
        }
        else if (_pendingAcks.TryRemove(new KeyValuePair<long, PendingAck>(correlationId, pending)))
        {
            // As TryCompletePendingRequest's compare-remove: only succeeds against the exact instance
            // just matched, so a forged acknowledgement cannot steal a slot a concurrent send has
            // already claimed for itself.
            pending.Completion.TrySetResult();
        }

        return true;
    }

    /// <summary>
    /// Sends a delivery acknowledgement back to the sender if <paramref name="headers"/> requested one,
    /// once the message has been handed to the application.
    /// </summary>
    /// <remarks>
    /// Called fire-and-forget from <see cref="ReceiveLoopAsync"/> — nothing awaits this task's
    /// completion or observes its exception, so every failure must be handled inside it. The catch
    /// below is deliberately broad (a callback/detached-task boundary, matching the pattern already used
    /// for a throwing <see cref="MessageReceived"/> subscriber): a transport-specific exception
    /// (<see cref="System.Net.WebSockets.WebSocketException"/> included), a send timeout, or
    /// cancellation from the connection tearing down must all be swallowed here rather than becoming an
    /// unobserved task exception or, had this still been awaited inline, tearing down the receive loop
    /// over what is always a best-effort courtesy send.
    /// </remarks>
    private async Task TrySendAcknowledgementAsync(
        Guid senderId, MessageHeaders headers, CancellationToken cancellationToken)
    {
        if (!headers.TryGetValue(DeliveryAcknowledgementHeaderKeys.Request, out string? requestFlag)
            || !string.Equals(requestFlag, "1", StringComparison.Ordinal))
        {
            return;
        }

        if (!headers.TryGetValue(DeliveryAcknowledgementHeaderKeys.CorrelationId, out string? correlationText))
        {
            _logger.LogWarning("Discarding an acknowledgement request with a missing correlation id");
            return;
        }

        var ackHeaders = new MessageHeaders(
        [
            new KeyValuePair<string, string>(DeliveryAcknowledgementHeaderKeys.CorrelationId, correlationText),
            new KeyValuePair<string, string>(DeliveryAcknowledgementHeaderKeys.Ack, "1"),
        ]);

        try
        {
            await SendCoreAsync(senderId, ReadOnlyMemory<byte>.Empty, ackHeaders, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Best-effort: if the connection is already gone or tearing down, the send timed out, or
            // some transport-specific fault occurred, there is nothing further to do here — the
            // sender's own pending acknowledgement wait will simply time out on its side.
            _logger.LogDebug(ex, "Failed to send a delivery acknowledgement to {SenderId}", senderId);
        }
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

    private sealed record PendingFindClients(
        int CorrelationId, TaskCompletionSource<IReadOnlyList<ClientDescriptor>> Completion);

    /// <summary>
    /// A <see cref="RequestAsync(Guid, ReadOnlyMemory{byte}, TimeSpan, CancellationToken)"/> call awaiting a reply. <see cref="ExpectedResponderId"/> is checked
    /// against the actual sender of an incoming reply frame before <see cref="Completion"/> is resolved,
    /// so a client other than the one this request was addressed to cannot forge a reply and resolve it.
    /// </summary>
    private sealed record PendingRequest(Guid ExpectedResponderId, TaskCompletionSource<ReadOnlyMemory<byte>> Completion);

    /// <summary>
    /// A <c>SendAsync(..., DeliveryOptions.RequireAck(...))</c> call awaiting acknowledgement.
    /// <see cref="ExpectedAcknowledgerId"/> is checked against the actual sender of an incoming
    /// acknowledgement frame before <see cref="Completion"/> is resolved, so a client other than the one
    /// the message was addressed to cannot forge an acknowledgement and resolve it.
    /// </summary>
    private sealed record PendingAck(Guid ExpectedAcknowledgerId, TaskCompletionSource Completion);
}
