using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;

namespace AdamSalisbury.Meshworx;

public sealed class MeshHub : IMeshHub, IAsyncDisposable
{
    private static readonly TimeSpan DefaultRegistrationTimeout = TimeSpan.FromSeconds(10);

    private readonly ILogger<MeshHub> _logger;
    private readonly ITransportListener _listener;
    private readonly TimeSpan _registrationTimeout;
    private readonly int _maxClients;
    private readonly TimeSpan? _heartbeatInterval;
    private readonly int _maxMissedHeartbeats;
    private readonly ConcurrentDictionary<Guid, ClientConnection> _clients = new();
    private readonly ConcurrentDictionary<string, Guid> _clientNames = new();
    private readonly ConcurrentDictionary<Task, byte> _handlerTasks = new();

    // Group membership is mutated far less often than messages are routed, so a single lock guarding
    // plain collections is simpler and safe. Each connection also tracks the groups it joined so it
    // can be removed from all of them on disconnect.
    private readonly Lock _groupsLock = new();
    private readonly Dictionary<string, HashSet<Guid>> _groups = new(StringComparer.Ordinal);
    private CancellationTokenSource? _cts;
    private Task? _acceptLoopTask;

    /// <param name="logger">The logger used to record hub activity.</param>
    /// <param name="listener">The transport listener that accepts incoming client connections.</param>
    /// <param name="registrationTimeout">
    /// The maximum time a newly accepted connection is given to complete registration before it is
    /// dropped. Guards against connections that accept but never register. Defaults to 10 seconds.
    /// </param>
    /// <param name="maxClients">
    /// The maximum number of clients that may be registered at once. Further registration attempts are
    /// refused with <see cref="RegistrationErrorCode.HubAtCapacity"/>. Defaults to unlimited.
    /// </param>
    /// <param name="heartbeatInterval">
    /// How long a registered client may be idle before the hub probes it with a ping. A client that
    /// fails to send any frame across <paramref name="maxMissedHeartbeats"/> consecutive intervals is
    /// evicted, detecting half-open connections. Defaults to <see langword="null"/> (disabled).
    /// </param>
    /// <param name="maxMissedHeartbeats">
    /// The number of consecutive idle intervals a client may go without sending any frame before it is
    /// evicted. Only used when <paramref name="heartbeatInterval"/> is set. Defaults to 2.
    /// </param>
    public MeshHub(
        ILogger<MeshHub> logger,
        ITransportListener listener,
        TimeSpan? registrationTimeout = null,
        int? maxClients = null,
        TimeSpan? heartbeatInterval = null,
        int maxMissedHeartbeats = 2)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(listener);

        if (registrationTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(registrationTimeout), "The registration timeout must be positive.");
        }

        if (maxClients is { } max && max <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxClients), "The maximum client count must be positive.");
        }

        if (heartbeatInterval is { } interval && interval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(heartbeatInterval), "The heartbeat interval must be positive.");
        }

        if (maxMissedHeartbeats < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxMissedHeartbeats), "The maximum missed heartbeats must be at least one.");
        }

        _logger = logger;
        _listener = listener;
        _registrationTimeout = registrationTimeout ?? DefaultRegistrationTimeout;
        _maxClients = maxClients ?? int.MaxValue;
        _heartbeatInterval = heartbeatInterval;
        _maxMissedHeartbeats = maxMissedHeartbeats;
    }

    /// <inheritdoc/>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_cts is not null)
        {
            throw new InvalidOperationException("The hub is already running.");
        }

        await _listener.StartAsync(cancellationToken).ConfigureAwait(false);
        _cts = new CancellationTokenSource();
        _acceptLoopTask = AcceptLoopAsync(_cts.Token);
    }

    /// <inheritdoc/>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_cts is null)
        {
            return;
        }

        byte[] disconnectPayload = [(byte)MessageType.Disconnect];
        foreach (ClientConnection client in _clients.Values)
        {
            try
            {
                await client.Transport.SendAsync(disconnectPayload, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException or OperationCanceledException)
            {
                // Best-effort disconnect notification; the client may already be gone.
            }
        }

        await _cts.CancelAsync().ConfigureAwait(false);

        if (_acceptLoopTask is not null)
        {
            try
            {
                await _acceptLoopTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when the cancellation token is triggered during shutdown.
            }
        }

        // Wait for all handler tasks to complete — each handler disposes its own client
        // connection in its finally block, so no separate disposal loop is needed.
        try
        {
            await Task.WhenAll(_handlerTasks.Keys).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Handler task exceptions are already logged individually via ContinueWith.
            // This catch prevents WhenAll from propagating during shutdown.
        }

        _handlerTasks.Clear();
        _clientNames.Clear();
        _clients.Clear();

        lock (_groupsLock)
        {
            _groups.Clear();
        }

        _cts.Dispose();
        _cts = null;
        _acceptLoopTask = null;
    }

    /// <inheritdoc/>
    public event EventHandler<ClientConnectionEventArgs>? ClientConnected;

    /// <inheritdoc/>
    public event EventHandler<ClientConnectionEventArgs>? ClientDisconnected;

    /// <inheritdoc/>
    public int ConnectedClientCount => _clients.Count;

    /// <inheritdoc/>
    public bool IsClientRegistered(Guid clientId)
    {
        return _clients.ContainsKey(clientId);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        await _listener.DisposeAsync().ConfigureAwait(false);
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            ITransport transport;
            try
            {
                transport = await _listener.AcceptAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (Exception ex)
            {
                // A single connection failing to be accepted — a peer resetting the moment it
                // connects, or a transient socket error — must not tear down the accept loop and
                // stop the hub serving every future client. Log and keep listening. This is the
                // background service's top-level loop, so catching broadly here is intentional.
                _logger.LogWarning(ex, "Failed to accept an incoming connection; continuing to listen");
                continue;
            }

            var handlerTask = HandleClientAsync(transport, cancellationToken);
            _handlerTasks.TryAdd(handlerTask, 0);
            _ = handlerTask.ContinueWith(
                t =>
                {
                    _handlerTasks.TryRemove(t, out _);
                    if (t.IsFaulted)
                    {
                        _logger.LogError(t.Exception, "Unhandled exception in client handler");
                    }
                },
                TaskScheduler.Default);
        }
    }

    private async Task HandleClientAsync(ITransport transport, CancellationToken cancellationToken)
    {
        var clientId = Guid.NewGuid();
        ClientConnection? connection = null;
        Task? sendLoopTask = null;
        CancellationTokenSource? clientCts = null;

        try
        {
            byte[]? registrationData;
            using (var registrationCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                registrationCts.CancelAfter(_registrationTimeout);
                try
                {
                    registrationData = await transport.ReceiveAsync(registrationCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                    when (registrationCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    _logger.LogDebug(
                        "Client {ClientId} did not complete registration within {Timeout}; dropping connection",
                        clientId,
                        _registrationTimeout);
                    return;
                }
            }

            if (registrationData is null
                || registrationData.Length < 3
                || (MessageType)registrationData[0] != MessageType.RegistrationRequest)
            {
                return;
            }

            if (registrationData[1] != Protocol.Version)
            {
                byte[] versionError =
                    [(byte)MessageType.Error, (byte)RegistrationErrorCode.UnsupportedProtocolVersion];
                await transport.SendAsync(versionError, cancellationToken).ConfigureAwait(false);
                return;
            }

            string clientName = Encoding.UTF8.GetString(registrationData.AsSpan(2));

            if (clientName.Length > Protocol.MaxClientNameLength)
            {
                byte[] nameTooLongError =
                    [(byte)MessageType.Error, (byte)RegistrationErrorCode.ClientNameTooLong];
                await transport.SendAsync(nameTooLongError, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (_clients.Count >= _maxClients)
            {
                byte[] capacityError = [(byte)MessageType.Error, (byte)RegistrationErrorCode.HubAtCapacity];
                await transport.SendAsync(capacityError, cancellationToken).ConfigureAwait(false);
                _logger.LogWarning(
                    "Refusing client {ClientId}: hub at capacity ({MaxClients} clients)", clientId, _maxClients);
                return;
            }

            if (!_clientNames.TryAdd(clientName, clientId))
            {
                byte[] errorPayload = [(byte)MessageType.Error, (byte)RegistrationErrorCode.DuplicateClientName];
                await transport.SendAsync(errorPayload, cancellationToken).ConfigureAwait(false);
                return;
            }

            connection = new ClientConnection(clientId, clientName, transport);
            _clients.TryAdd(clientId, connection);

            var responsePayload = new byte[17];
            responsePayload[0] = (byte)MessageType.RegistrationComplete;
            clientId.TryWriteBytes(responsePayload.AsSpan(1));
            await transport.SendAsync(responsePayload, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation("Client {ClientId} ({ClientName}) connected", clientId, clientName);
            RaiseClientEvent(ClientConnected, clientId, clientName, nameof(ClientConnected));

            clientCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            sendLoopTask = SendLoopAsync(connection, clientCts);

            int missedHeartbeats = 0;

            while (!clientCts.Token.IsCancellationRequested)
            {
                byte[]? data;

                if (_heartbeatInterval is null)
                {
                    data = await transport.ReceiveAsync(clientCts.Token).ConfigureAwait(false);
                }
                else
                {
                    using var readCts = CancellationTokenSource.CreateLinkedTokenSource(clientCts.Token);
                    readCts.CancelAfter(_heartbeatInterval.Value);
                    try
                    {
                        data = await transport.ReceiveAsync(readCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                        when (readCts.IsCancellationRequested && !clientCts.Token.IsCancellationRequested)
                    {
                        missedHeartbeats++;
                        if (missedHeartbeats > _maxMissedHeartbeats)
                        {
                            _logger.LogInformation(
                                "Client {ClientId} did not respond to {Missed} heartbeats; evicting",
                                clientId,
                                _maxMissedHeartbeats);
                            break;
                        }

                        // Probe liveness via the outbound queue so the ping serialises with any
                        // other queued frames. A live client replies with a Pong (or any frame),
                        // resetting the counter below.
                        connection.OutboundQueue.Writer.TryWrite([(byte)MessageType.Ping]);
                        continue;
                    }
                }

                if (data is null)
                {
                    break;
                }

                // Any received frame proves the client is alive.
                missedHeartbeats = 0;

                if (data.Length == 0)
                {
                    // Empty frames carry no opcode; ignore rather than indexing data[0].
                    continue;
                }

                if (data.Length >= 17
                    && (MessageType)data[0] == MessageType.SendMessage)
                {
                    var recipientId = new Guid(data.AsSpan(1, 16));
                    ReadOnlyMemory<byte> messageData = data.AsMemory(17);

                    RouteMessage(clientId, recipientId, messageData);
                }
                else if ((MessageType)data[0] == MessageType.BroadcastMessage)
                {
                    BroadcastMessage(clientId, data.AsMemory(1));
                }
                else if ((MessageType)data[0] == MessageType.JoinGroup)
                {
                    string groupName = Encoding.UTF8.GetString(data.AsSpan(1));
                    JoinGroup(connection, groupName);
                }
                else if ((MessageType)data[0] == MessageType.LeaveGroup)
                {
                    string groupName = Encoding.UTF8.GetString(data.AsSpan(1));
                    LeaveGroup(connection, groupName);
                }
                else if (data.Length >= 3
                    && (MessageType)data[0] == MessageType.GroupMessage)
                {
                    int nameLength = BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(1, 2));
                    if (data.Length >= 3 + nameLength)
                    {
                        string groupName = Encoding.UTF8.GetString(data.AsSpan(3, nameLength));
                        SendToGroup(clientId, groupName, data.AsMemory(3 + nameLength));
                    }
                }
                else if (data.Length >= 5
                    && (MessageType)data[0] == MessageType.ClientLookupRequest)
                {
                    int correlationId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(1, 4));
                    string lookupName = Encoding.UTF8.GetString(data.AsSpan(5));

                    byte[] lookupResponse;
                    if (_clientNames.TryGetValue(lookupName, out Guid foundId)
                        && _clients.TryGetValue(foundId, out ClientConnection? found))
                    {
                        lookupResponse = new byte[22];
                        lookupResponse[0] = (byte)MessageType.ClientLookupResponse;
                        BinaryPrimitives.WriteInt32BigEndian(lookupResponse.AsSpan(1, 4), correlationId);
                        lookupResponse[5] = 0x01;
                        found.Id.TryWriteBytes(lookupResponse.AsSpan(6));
                    }
                    else
                    {
                        lookupResponse = new byte[6];
                        lookupResponse[0] = (byte)MessageType.ClientLookupResponse;
                        BinaryPrimitives.WriteInt32BigEndian(lookupResponse.AsSpan(1, 4), correlationId);
                        lookupResponse[5] = 0x00;
                    }

                    await transport.SendAsync(lookupResponse, clientCts.Token).ConfigureAwait(false);
                }
                else if ((MessageType)data[0] == MessageType.Pong)
                {
                    // Liveness reply to a heartbeat ping; the counter was already reset above.
                }
                else if ((MessageType)data[0] == MessageType.Disconnect)
                {
                    _logger.LogDebug("Client {ClientId} sent disconnect", clientId);
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when the cancellation token is triggered during shutdown.
        }
        catch (IOException ex)
        {
            _logger.LogWarning(ex, "Client {ClientId} transport error", clientId);
        }
        finally
        {
            connection?.OutboundQueue.Writer.TryComplete();

            if (clientCts is not null)
            {
                await clientCts.CancelAsync().ConfigureAwait(false);
            }

            if (sendLoopTask is not null)
            {
                try
                {
                    await sendLoopTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected during shutdown.
                }
            }

            clientCts?.Dispose();

            if (connection is not null)
            {
                RemoveFromAllGroups(connection);
                _clientNames.TryRemove(connection.Name, out _);
                _clients.TryRemove(clientId, out _);
                await connection.DisposeAsync().ConfigureAwait(false);
                _logger.LogInformation("Client {ClientId} disconnected", clientId);
                RaiseClientEvent(ClientDisconnected, connection.Id, connection.Name, nameof(ClientDisconnected));
            }
            else
            {
                await transport.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private void RaiseClientEvent(
        EventHandler<ClientConnectionEventArgs>? handler,
        Guid clientId,
        string clientName,
        string eventName)
    {
        if (handler is null)
        {
            return;
        }

        try
        {
            handler(this, new ClientConnectionEventArgs { ClientId = clientId, ClientName = clientName });
        }
        catch (Exception ex)
        {
            // A throwing subscriber must not fault the client handler task. Callback boundary.
            _logger.LogError(ex, "A {EventName} handler threw an exception", eventName);
        }
    }

    private async Task SendLoopAsync(ClientConnection connection, CancellationTokenSource clientCts)
    {
        try
        {
            await foreach (byte[] payload in connection.OutboundQueue.Reader
                .ReadAllAsync(clientCts.Token).ConfigureAwait(false))
            {
                await connection.Transport.SendAsync(payload, clientCts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException)
        {
            _logger.LogWarning(
                ex,
                "Send loop for client {ClientId} terminated due to transport error",
                connection.Id);
            await clientCts.CancelAsync().ConfigureAwait(false);
        }
    }

    private void RouteMessage(
        Guid senderId,
        Guid recipientId,
        ReadOnlyMemory<byte> messageData)
    {
        if (!_clients.TryGetValue(recipientId, out ClientConnection? recipient))
        {
            _logger.LogDebug(
                "Message from {SenderId} dropped: recipient {RecipientId} not found",
                senderId,
                recipientId);
            return;
        }

        var deliveryPayload = new byte[1 + 16 + messageData.Length];
        deliveryPayload[0] = (byte)MessageType.DeliverMessage;
        senderId.TryWriteBytes(deliveryPayload.AsSpan(1));
        messageData.CopyTo(deliveryPayload.AsMemory(17));

        if (!recipient.OutboundQueue.Writer.TryWrite(deliveryPayload))
        {
            _logger.LogWarning(
                "Outbound queue for {RecipientId} is full, message from {SenderId} dropped",
                recipientId,
                senderId);
        }
    }

    private void BroadcastMessage(Guid senderId, ReadOnlyMemory<byte> messageData)
    {
        // Build the delivery frame once and share it across every recipient's queue. The send
        // loops only read the array, so concurrent reads of this never-mutated buffer are safe.
        var deliveryPayload = new byte[1 + 16 + messageData.Length];
        deliveryPayload[0] = (byte)MessageType.DeliverMessage;
        senderId.TryWriteBytes(deliveryPayload.AsSpan(1));
        messageData.CopyTo(deliveryPayload.AsMemory(17));

        foreach (KeyValuePair<Guid, ClientConnection> entry in _clients)
        {
            if (entry.Key == senderId)
            {
                // A broadcast is not echoed back to its sender.
                continue;
            }

            if (!entry.Value.OutboundQueue.Writer.TryWrite(deliveryPayload))
            {
                _logger.LogWarning(
                    "Outbound queue for {RecipientId} is full, broadcast from {SenderId} dropped",
                    entry.Key,
                    senderId);
            }
        }
    }

    private void JoinGroup(ClientConnection connection, string groupName)
    {
        if (string.IsNullOrEmpty(groupName))
        {
            return;
        }

        lock (_groupsLock)
        {
            if (!_groups.TryGetValue(groupName, out HashSet<Guid>? members))
            {
                members = [];
                _groups[groupName] = members;
            }

            members.Add(connection.Id);
            connection.Groups.Add(groupName);
        }

        _logger.LogDebug("Client {ClientId} joined group {GroupName}", connection.Id, groupName);
    }

    private void LeaveGroup(ClientConnection connection, string groupName)
    {
        if (string.IsNullOrEmpty(groupName))
        {
            return;
        }

        lock (_groupsLock)
        {
            RemoveMemberFromGroup(connection.Id, groupName);
            connection.Groups.Remove(groupName);
        }

        _logger.LogDebug("Client {ClientId} left group {GroupName}", connection.Id, groupName);
    }

    private void RemoveFromAllGroups(ClientConnection connection)
    {
        lock (_groupsLock)
        {
            foreach (string groupName in connection.Groups)
            {
                RemoveMemberFromGroup(connection.Id, groupName);
            }

            connection.Groups.Clear();
        }
    }

    // Must be called while holding _groupsLock.
    private void RemoveMemberFromGroup(Guid clientId, string groupName)
    {
        if (_groups.TryGetValue(groupName, out HashSet<Guid>? members)
            && members.Remove(clientId)
            && members.Count == 0)
        {
            _groups.Remove(groupName);
        }
    }

    private void SendToGroup(Guid senderId, string groupName, ReadOnlyMemory<byte> messageData)
    {
        Guid[] recipients;
        lock (_groupsLock)
        {
            if (!_groups.TryGetValue(groupName, out HashSet<Guid>? members) || members.Count == 0)
            {
                return;
            }

            // Snapshot the membership so the queues can be written without holding the lock.
            recipients = members.Count == 1 && members.Contains(senderId)
                ? []
                : members.Where(id => id != senderId).ToArray();
        }

        if (recipients.Length == 0)
        {
            return;
        }

        // One shared, never-mutated delivery frame across every recipient's queue (see BroadcastMessage).
        var deliveryPayload = new byte[1 + 16 + messageData.Length];
        deliveryPayload[0] = (byte)MessageType.DeliverMessage;
        senderId.TryWriteBytes(deliveryPayload.AsSpan(1));
        messageData.CopyTo(deliveryPayload.AsMemory(17));

        foreach (Guid recipientId in recipients)
        {
            if (_clients.TryGetValue(recipientId, out ClientConnection? recipient)
                && !recipient.OutboundQueue.Writer.TryWrite(deliveryPayload))
            {
                _logger.LogWarning(
                    "Outbound queue for {RecipientId} is full, group message from {SenderId} dropped",
                    recipientId,
                    senderId);
            }
        }
    }

    private sealed class ClientConnection(Guid id, string name, ITransport transport) : IAsyncDisposable
    {
        private const int OutboundQueueCapacity = 1024;
        private int _disposed;

        public Guid Id { get; } = id;
        public string Name { get; } = name;
        public ITransport Transport { get; } = transport;

        /// <summary>
        /// The set of groups this client has joined. Only accessed under the hub's groups lock.
        /// </summary>
        public HashSet<string> Groups { get; } = new(StringComparer.Ordinal);

        public Channel<byte[]> OutboundQueue { get; } = Channel.CreateBounded<byte[]>(
            new BoundedChannelOptions(OutboundQueueCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
            });

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                OutboundQueue.Writer.TryComplete();
                await Transport.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
