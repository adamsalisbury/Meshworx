using System.Buffers.Binary;
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
    private ITransport? _transport;
    private CancellationTokenSource? _cts;
    private Task? _receiveLoopTask;

    // Single-slot pending lookup, serialised by _lookupLock. Each request carries a
    // correlation id echoed by the hub; the receive loop only completes the pending
    // lookup when the ids match, so a response from a cancelled request cannot resolve
    // a subsequent lookup with a stale result.
    private PendingLookup? _pendingLookup;
    private int _lookupCorrelationId;

    private readonly TimeSpan? _idleTimeout;

    /// <param name="logger">The logger used to record client activity.</param>
    /// <param name="idleTimeout">
    /// The maximum time the client will wait without receiving any frame from the hub before treating
    /// the connection as lost and raising <see cref="Disconnected"/>. Set this above the hub's heartbeat
    /// interval so the hub's pings keep the connection alive. Defaults to <see langword="null"/> (no timeout).
    /// </param>
    public MeshClient(ILogger<MeshClient> logger, TimeSpan? idleTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(logger);

        if (idleTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(idleTimeout), "The idle timeout must be positive.");
        }

        _logger = logger;
        _idleTimeout = idleTimeout;
    }

    /// <inheritdoc/>
    public Guid Id { get; private set; }

    /// <inheritdoc/>
    public string Name { get; private set; } = string.Empty;

    /// <inheritdoc/>
    public event EventHandler<MessageReceivedEventArgs>? MessageReceived;

    /// <inheritdoc/>
    public event EventHandler<DisconnectedEventArgs>? Disconnected;

    /// <inheritdoc/>
    public async Task ConnectAsync(ITransport transport, string clientName, CancellationToken cancellationToken = default)
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
            _transport = transport;
        }

        try
        {
            byte[] nameBytes = Encoding.UTF8.GetBytes(clientName);
            var requestPayload = new byte[2 + nameBytes.Length];
            requestPayload[0] = (byte)MessageType.RegistrationRequest;
            requestPayload[1] = Protocol.Version;
            nameBytes.CopyTo(requestPayload, 2);
            await _transport.SendAsync(requestPayload, cancellationToken).ConfigureAwait(false);

            byte[]? responseData = await _transport.ReceiveAsync(cancellationToken).ConfigureAwait(false);

            if (responseData is { Length: >= 2 }
                && (MessageType)responseData[0] == MessageType.Error)
            {
                var errorCode = (RegistrationErrorCode)responseData[1];
                throw new RegistrationRefusedException(errorCode);
            }

            if (responseData is null
                || responseData.Length != 17
                || (MessageType)responseData[0] != MessageType.RegistrationComplete)
            {
                throw new InvalidOperationException("Failed to register with the hub.");
            }

            Id = new Guid(responseData.AsSpan(1, 16));
            Name = clientName;
            _logger.LogInformation("Connected to hub with id {ClientId}", Id);

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
            _state = ConnectionState.Disconnected;
        }
    }

    /// <inheritdoc/>
    public async Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        CancellationToken cancellationToken = default)
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

        var payload = new byte[1 + 16 + message.Length];
        payload[0] = (byte)MessageType.SendMessage;
        recipientId.TryWriteBytes(payload.AsSpan(1));
        message.CopyTo(payload.AsMemory(17));

        await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);
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

        await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task JoinGroupAsync(string groupName, CancellationToken cancellationToken = default)
    {
        return SendGroupMembershipAsync(MessageType.JoinGroup, groupName, cancellationToken);
    }

    /// <inheritdoc/>
    public Task LeaveGroupAsync(string groupName, CancellationToken cancellationToken = default)
    {
        return SendGroupMembershipAsync(MessageType.LeaveGroup, groupName, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task SendToGroupAsync(
        string groupName,
        ReadOnlyMemory<byte> message,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupName);

        ITransport transport = GetConnectedTransport();

        byte[] nameBytes = Encoding.UTF8.GetBytes(groupName);
        if (nameBytes.Length > ushort.MaxValue)
        {
            throw new ArgumentException("The group name is too long.", nameof(groupName));
        }

        var payload = new byte[1 + 2 + nameBytes.Length + message.Length];
        payload[0] = (byte)MessageType.GroupMessage;
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(1, 2), (ushort)nameBytes.Length);
        nameBytes.CopyTo(payload, 3);
        message.CopyTo(payload.AsMemory(3 + nameBytes.Length));

        await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    private async Task SendGroupMembershipAsync(
        MessageType type,
        string groupName,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupName);

        ITransport transport = GetConnectedTransport();

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

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                byte[]? data;

                if (_idleTimeout is null)
                {
                    data = await transport.ReceiveAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    using var readCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    readCts.CancelAfter(_idleTimeout.Value);
                    try
                    {
                        data = await transport.ReceiveAsync(readCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                        when (readCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                    {
                        _logger.LogWarning(
                            "No frame received from the hub within {Timeout}; treating the connection as lost",
                            _idleTimeout);
                        reason = DisconnectReason.ConnectionLost;
                        break;
                    }
                }

                if (data is null)
                {
                    break;
                }

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
            // The receive loop is the only thing that completes a pending lookup. If it
            // terminates for any reason before the response arrives, fault the waiter so
            // callers using a default (non-cancellable) token are not left hanging — and
            // so the held _lookupLock is released, unblocking subsequent lookups.
            _pendingLookup?.Completion.TrySetException(
                new InvalidOperationException("The connection was closed before the lookup completed."));

            await HandleReceiveLoopTerminationAsync(reason).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Tears the connection down and raises <see cref="Disconnected"/> when the receive loop
    /// ends for a remote reason. If a local <see cref="DisconnectAsync"/> already moved the
    /// client out of the connected state, that call owns cleanup and no event is raised.
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

        await CleanUpAsync().ConfigureAwait(false);

        lock (_stateLock)
        {
            Id = Guid.Empty;
            Name = string.Empty;
            _state = ConnectionState.Disconnected;
        }

        try
        {
            Disconnected?.Invoke(this, new DisconnectedEventArgs { Reason = reason });
        }
        catch (Exception ex)
        {
            // A throwing subscriber must not fault the receive loop task. Callback boundary.
            _logger.LogError(ex, "A Disconnected handler threw an exception");
        }
    }

    private enum ConnectionState
    {
        Disconnected,
        Connecting,
        Connected,
        Disconnecting,
    }

    private sealed record PendingLookup(int CorrelationId, TaskCompletionSource<Guid?> Completion);
}
