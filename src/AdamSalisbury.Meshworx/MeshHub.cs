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
    private static readonly TimeSpan DefaultGroupAuthorisationTimeout = TimeSpan.FromSeconds(10);
    private const int DefaultMaxConcurrentAuthentications = 64;

    private readonly ILogger<MeshHub> _logger;
    private readonly ITransportListener _listener;
    private readonly TimeSpan _registrationTimeout;
    private readonly int _maxClients;
    private readonly TimeSpan? _heartbeatInterval;
    private readonly int _maxMissedHeartbeats;
    private readonly ClientAuthenticator? _authenticator;
    private readonly GroupAuthoriser? _groupAuthoriser;
    private readonly TimeSpan _groupAuthorisationTimeout;

    // Caps how many integrator authenticator callbacks may run concurrently. Null when no authenticator
    // is configured, since there is then no pre-authentication work to bound.
    private readonly SemaphoreSlim? _authenticationSlots;

    private readonly ConcurrentDictionary<Guid, ClientConnection> _clients = new();
    private readonly ConcurrentDictionary<string, Guid> _clientNames = new();
    private readonly ConcurrentDictionary<Task, byte> _handlerTasks = new();

    // How many client slots are currently claimed. This, rather than _clients.Count, is what maxClients
    // is enforced against: a slot is claimed by a single atomic operation before the client is put into
    // the registries and given back when its handler ends. Testing the client count and adding
    // afterwards let concurrent registrations all read the same count and all admit, so the cap could be
    // overshot by as many clients as happened to be registering at once. Every claim is owned by exactly
    // one client handler and released in that handler's finally, so a shutdown that clears the registries
    // deliberately does not reset this — the handlers still running own the outstanding claims and give
    // them back themselves. A hub stopped while a handler is still unwinding therefore reports no
    // connected clients while briefly still holding its slot, which is the safe way round.
    private int _reservedClientSlots;

    // Each group is guarded by its own lock, so traffic to distinct groups routes in parallel and
    // only mutation of the same group contends. A group is created on first join and removed once
    // empty. Each connection also tracks the groups it joined so it can be removed from all of them
    // on disconnect; that set is only ever touched by the connection's own receive loop (and its
    // teardown, which runs after the loop ends), so it needs no additional lock.
    private readonly ConcurrentDictionary<string, Group> _groups = new(StringComparer.Ordinal);

    // Guards every lifecycle field below. Starting, stopping and disposing can each be called from a
    // different thread, so each of them takes the state it needs in one critical section and then works
    // only from locals: reading a field twice is what let a concurrent stop null the token source
    // between a check and the dereference that followed it. Nothing that blocks or awaits is done while
    // holding it.
    private readonly Lock _stateLock = new();

    private CancellationTokenSource? _cts;
    private Task? _acceptLoopTask;

    // The call to StopAsync that finds the hub running takes ownership of the shutdown and stores it
    // here; every concurrent call awaits that same task rather than running a second shutdown over state
    // the first has already taken. Cleared once the shutdown finishes, so the hub can be started again.
    // Read and written only under the lock.
    private Task? _stopTask;

    // The first call to DisposeAsync stores its teardown here; every later or concurrent call awaits
    // that same task. Read and written only under the lock.
    private Task? _disposeTask;

    // Set while a start is between claiming the hub and publishing its accept loop. It holds the running
    // slot across the listener start without exposing a token source that a concurrent stop could take
    // ownership of before there is anything to stop.
    private bool _starting;

    // Set the instant disposal begins, before any teardown starts, so a start racing a disposal cannot
    // bring the hub back up on a listener that is being torn down.
    private bool _disposed;

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
    /// How long a registered client may be idle before the hub probes it with a ping — unless
    /// <paramref name="maxMissedHeartbeats"/> is 1, in which case the first idle interval evicts the
    /// client rather than probing it. A client that fails to send any frame across
    /// <paramref name="maxMissedHeartbeats"/> consecutive intervals is evicted, detecting half-open
    /// connections. Defaults to <see langword="null"/> (disabled).
    /// </param>
    /// <param name="maxMissedHeartbeats">
    /// The number of consecutive idle intervals that causes a client to be evicted. A client that sends
    /// no frame across this many consecutive intervals is dropped; any frame it sends resets the count.
    /// The hub pings the client on each idle interval before the last, so it is probed
    /// <paramref name="maxMissedHeartbeats"/> minus one times before eviction — a value of 1 evicts on
    /// the first idle interval without probing at all. Only used when
    /// <paramref name="heartbeatInterval"/> is set. Defaults to 2.
    /// </param>
    /// <param name="authenticator">
    /// An optional callback invoked for each registration to decide whether the client may join, given
    /// its name and the opaque credential it supplied. Returning <see langword="false"/> refuses the
    /// client with <see cref="RegistrationErrorCode.AuthenticationFailed"/>. When <see langword="null"/>
    /// (the default) the hub performs no authentication and admits any peer that completes the
    /// handshake — in that case the hub must only be exposed to a trusted network.
    /// </param>
    /// <param name="maxConcurrentAuthentications">
    /// The maximum number of <paramref name="authenticator"/> callbacks that may run at once. The
    /// authenticator runs on unauthenticated input, so this bounds the work an unauthenticated peer can
    /// cause by connecting. A connection that cannot obtain a slot within
    /// <paramref name="registrationTimeout"/> is refused with
    /// <see cref="RegistrationErrorCode.AuthenticationFailed"/>. Defaults to 64. Ignored when
    /// <paramref name="authenticator"/> is <see langword="null"/>.
    /// </param>
    /// <param name="groupAuthoriser">
    /// An optional callback invoked for each group join to decide whether the client may become a member,
    /// given its registered identity and the group name. Returning <see langword="false"/> refuses the
    /// join and tells the client so. When <see langword="null"/> (the default) the hub authorises no
    /// joins and any client may join any group — groups are then a routing convenience, not an isolation
    /// boundary. Sending to a group always requires membership of it, with or without this callback.
    /// </param>
    /// <param name="groupAuthorisationTimeout">
    /// The maximum time a <paramref name="groupAuthoriser"/> callback is given to decide before the join
    /// is refused. Bounds a hanging integrator callback, which would otherwise stall the calling client's
    /// receive loop — and hold its client slot — indefinitely. Defaults to 10 seconds. Ignored when
    /// <paramref name="groupAuthoriser"/> is <see langword="null"/>.
    /// </param>
    public MeshHub(
        ILogger<MeshHub> logger,
        ITransportListener listener,
        TimeSpan? registrationTimeout = null,
        int? maxClients = null,
        TimeSpan? heartbeatInterval = null,
        int maxMissedHeartbeats = 2,
        ClientAuthenticator? authenticator = null,
        int? maxConcurrentAuthentications = null,
        GroupAuthoriser? groupAuthoriser = null,
        TimeSpan? groupAuthorisationTimeout = null)
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

        if (maxConcurrentAuthentications is { } maxAuthentications && maxAuthentications <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConcurrentAuthentications),
                "The maximum concurrent authentication count must be positive.");
        }

        if (groupAuthorisationTimeout is { } authorisationTimeout && authorisationTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(groupAuthorisationTimeout), "The group authorisation timeout must be positive.");
        }

        _logger = logger;
        _listener = listener;
        _registrationTimeout = registrationTimeout ?? DefaultRegistrationTimeout;
        _maxClients = maxClients ?? int.MaxValue;
        _heartbeatInterval = heartbeatInterval;
        _maxMissedHeartbeats = maxMissedHeartbeats;
        _authenticator = authenticator;
        _groupAuthoriser = groupAuthoriser;
        _groupAuthorisationTimeout = groupAuthorisationTimeout ?? DefaultGroupAuthorisationTimeout;

        if (authenticator is not null)
        {
            int slots = maxConcurrentAuthentications ?? DefaultMaxConcurrentAuthentications;
            _authenticationSlots = new SemaphoreSlim(slots, slots);
        }

        // At a maximum of one missed heartbeat there is no interval left in which a ping could be
        // answered, so the hub evicts on the first idle interval without probing. A client that only
        // receives — and so sends nothing of its own — is then dropped every interval. That is a
        // legitimate choice if clients are expected to send continuously, but it is far more often a
        // misconfiguration, and a silent one, so say so once at construction.
        if (heartbeatInterval is not null && maxMissedHeartbeats == 1)
        {
            _logger.LogWarning(
                "Heartbeats are enabled with maxMissedHeartbeats set to 1, so clients are evicted on "
                + "their first idle interval and are never probed with a ping. Clients that do not send "
                + "frames of their own will be evicted every interval; use 2 or more to probe liveness.");
        }
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">The hub has been disposed.</exception>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_stateLock)
        {
            // A disposed hub must stay disposed. Without this a start racing a disposal would begin
            // listening on a transport that is being torn down, and the teardown would then leave a
            // running accept loop behind that nothing owns.
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_cts is not null || _stopTask is not null || _starting)
            {
                throw new InvalidOperationException("The hub is already running.");
            }

            // Claim the running slot with a flag rather than by publishing the token source early. A
            // second concurrent start is refused here, but a concurrent stop cannot take a token source
            // whose accept loop does not exist yet — which would abandon a listener that had just been
            // bound, on a hub that then reported itself stopped.
            _starting = true;
        }

        var cts = new CancellationTokenSource();

        try
        {
            await _listener.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Release the claim, so a hub whose listener failed to start is startable again rather than
            // permanently reporting itself as already running. Nothing else has seen this token source,
            // so disposing it here cannot race a stop.
            lock (_stateLock)
            {
                _starting = false;
            }

            cts.Dispose();
            throw;
        }

        lock (_stateLock)
        {
            _starting = false;

            // A disposal may have run to completion while the listener was starting. Its teardown has
            // already disposed the listener, so publishing an accept loop here would run it against a
            // closed listener for as long as it took to notice.
            if (_disposed)
            {
                cts.Dispose();
                throw new ObjectDisposedException(GetType().FullName);
            }

            // Publish the token source and the accept loop together, so no stop can ever see one without
            // the other. Creating the loop holds the lock across the synchronous head of
            // ITransportListener.AcceptAsync; both listeners in this library reach their first await in a
            // lock acquisition and a field read, and only another lifecycle call could contend.
            _cts = cts;
            _acceptLoopTask = AcceptLoopAsync(cts.Token);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Safe to call more than once, and from more than one thread at a time. The call that finds the hub
    /// running takes ownership of its state and performs the shutdown; every concurrent call awaits that
    /// same shutdown, so each of them returns only once the hub has actually stopped, and none of them
    /// notifies the clients a second time or disposes the token source twice. A call made when the hub is
    /// not running returns immediately.
    /// </remarks>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task shutdown;

        lock (_stateLock)
        {
            if (_stopTask is null)
            {
                CancellationTokenSource? cts = _cts;
                if (cts is null)
                {
                    return Task.CompletedTask;
                }

                // Hand the state to the shutdown and clear the fields in the same critical section, so no
                // other caller can see a half-stopped hub or tear the same state down twice.
                Task? acceptLoopTask = _acceptLoopTask;
                _cts = null;
                _acceptLoopTask = null;

                _stopTask = StopCoreAsync(cts, acceptLoopTask, cancellationToken);
            }

            shutdown = _stopTask;
        }

        // A caller that joined someone else's shutdown still honours its own cancellation token. Giving
        // up on the wait does not cancel the shutdown itself, which belongs to the caller that started it.
        return cancellationToken.CanBeCanceled ? shutdown.WaitAsync(cancellationToken) : shutdown;
    }

    /// <summary>
    /// Performs the one and only shutdown of a running hub, working from the state handed to it so that it
    /// cannot race another caller over the fields.
    /// </summary>
    private async Task StopCoreAsync(
        CancellationTokenSource cts, Task? acceptLoopTask, CancellationToken cancellationToken)
    {
        // Nothing of the shutdown may run on the caller's stack while it still holds the state lock, and
        // the first thing below is transport I/O. The TCP listener can reason its teardown's synchronous
        // head safe instead; here there is an arbitrary ITransport in the way, so yield.
        await Task.Yield();

        try
        {
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
        }
        finally
        {
            // The notification above is best-effort, but the shutdown proper is not: run it even if a
            // transport failed in a way the filter above does not cover. Skipping it would leave the
            // accept loop running and the token source undisposed on a hub that now reports itself
            // stopped, which no later call could put right.
            await ShutDownAsync(cts, acceptLoopTask, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Cancels the hub's work, waits for the accept loop and every client handler to finish, and clears the
    /// registries.
    /// </summary>
    private async Task ShutDownAsync(
        CancellationTokenSource cts, Task? acceptLoopTask, CancellationToken cancellationToken)
    {
        try
        {
            await cts.CancelAsync().ConfigureAwait(false);

            if (acceptLoopTask is not null)
            {
                try
                {
                    await acceptLoopTask.WaitAsync(cancellationToken).ConfigureAwait(false);
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
            _groups.Clear();

            cts.Dispose();
        }
        finally
        {
            // Release the shutdown claim whatever happened, so a hub can be started again once it has
            // stopped — and so a shutdown that failed part way leaves the hub stopped rather than wedged
            // as permanently stopping.
            lock (_stateLock)
            {
                _stopTask = null;
            }
        }
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
    /// <remarks>
    /// Safe to call more than once, and from more than one thread at a time. The first call performs the
    /// teardown; every other call awaits that same teardown, so each of them returns only once the hub has
    /// stopped and the listener is closed. A disposed hub cannot be started again.
    /// </remarks>
    public ValueTask DisposeAsync()
    {
        Task disposal;

        lock (_stateLock)
        {
            // Mark the hub disposed before any teardown begins, so a start racing this call is refused
            // rather than racing the listener's disposal.
            _disposed = true;
            _disposeTask ??= DisposeCoreAsync();
            disposal = _disposeTask;
        }

        return new ValueTask(disposal);
    }

    /// <summary>
    /// Performs the one and only teardown of the hub.
    /// </summary>
    private async Task DisposeCoreAsync()
    {
        // Started from inside the state lock, and the shutdown it awaits takes that same lock. Yield first
        // so none of it runs on the disposing thread while the lock is still held.
        await Task.Yield();

        await StopAsync().ConfigureAwait(false);
        await _listener.DisposeAsync().ConfigureAwait(false);
        _authenticationSlots?.Dispose();
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
        Task? heartbeatMonitorTask = null;
        CancellationTokenSource? clientCts = null;
        bool slotReserved = false;

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

            // Registration frame: [type][version][name length (2, big-endian)][name][credential].
            if (registrationData is null
                || registrationData.Length < 2
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

            if (registrationData.Length < 4)
            {
                // Too short to carry the 2-byte name length; malformed.
                return;
            }

            int registrationNameLength = BinaryPrimitives.ReadUInt16BigEndian(registrationData.AsSpan(2, 2));
            if (registrationNameLength == 0 || registrationData.Length < 4 + registrationNameLength)
            {
                // Malformed frame: the name is empty, or the declared name runs past the payload. An
                // empty name is refused here rather than admitted, because it would otherwise reserve
                // the empty string in the name registry. No in-box client can produce one.
                return;
            }

            string clientName = Encoding.UTF8.GetString(registrationData.AsSpan(4, registrationNameLength));

            if (clientName.Length > Protocol.MaxClientNameLength)
            {
                byte[] nameTooLongError =
                    [(byte)MessageType.Error, (byte)RegistrationErrorCode.ClientNameTooLong];
                await transport.SendAsync(nameTooLongError, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (_authenticator is not null)
            {
                // Refuse an already-full hub before running the integrator's authenticator, so a
                // connection flood cannot drive authentication work — or hold handler tasks on a slow
                // authenticator — once there is nothing left to admit it to. This is only an early-out:
                // the slot itself is claimed below, after authentication returns, so that a peer which
                // never authenticates cannot hold capacity away from one that would.
                if (Volatile.Read(ref _reservedClientSlots) >= _maxClients)
                {
                    await RefuseAtCapacityAsync(transport, clientId, cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (!await AuthenticateAsync(
                        clientId, clientName, registrationData, registrationNameLength, cancellationToken)
                    .ConfigureAwait(false))
                {
                    byte[] authError = [(byte)MessageType.Error, (byte)RegistrationErrorCode.AuthenticationFailed];
                    await transport.SendAsync(authError, cancellationToken).ConfigureAwait(false);
                    return;
                }
            }

            // Claim a slot rather than testing the client count and adding to the registry afterwards.
            // Any number of registrations can read the same count, all pass a test against it, and all
            // then add — so the count-based check made maxClients a soft cap that a burst could overshoot
            // by the size of the burst. The claim is one atomic operation, so exactly one of any number
            // of concurrent registrations takes the last slot and the rest are refused here.
            if (!TryReserveClientSlot())
            {
                await RefuseAtCapacityAsync(transport, clientId, cancellationToken).ConfigureAwait(false);
                return;
            }

            slotReserved = true;

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

            // A single monitor per connection probes liveness off a PeriodicTimer, so the receive
            // loop below reads against one long-lived token with no per-frame CancellationTokenSource
            // or timer-queue churn. The monitor is only started when heartbeats are configured.
            if (_heartbeatInterval is { } heartbeatInterval)
            {
                heartbeatMonitorTask = MonitorHeartbeatAsync(connection, clientCts, heartbeatInterval, clientId);
            }

            while (!clientCts.Token.IsCancellationRequested)
            {
                byte[]? data = await transport.ReceiveAsync(clientCts.Token).ConfigureAwait(false);

                if (data is null)
                {
                    break;
                }

                // Any received frame proves the client is alive; the heartbeat monitor observes this.
                connection.RecordActivity();

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
                    // Pass the original name bytes through as well, so a refusal can echo them
                    // rather than re-encode the string they were just decoded from.
                    await JoinGroupAsync(connection, groupName, data.AsMemory(1), clientCts.Token)
                        .ConfigureAwait(false);
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
                        // Pass the original name bytes straight through so SendToGroup does not
                        // re-encode the string it was just decoded from.
                        SendToGroup(
                            clientId,
                            groupName,
                            data.AsMemory(3, nameLength),
                            data.AsMemory(3 + nameLength));
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
                    // Liveness reply to a heartbeat ping; RecordActivity above already noted it.
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

            if (heartbeatMonitorTask is not null)
            {
                try
                {
                    await heartbeatMonitorTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected once the client's cancellation token is triggered.
                }
            }

            clientCts?.Dispose();

            if (connection is not null)
            {
                RemoveFromAllGroups(connection);
                _clientNames.TryRemove(connection.Name, out _);
                _clients.TryRemove(clientId, out _);
            }

            // Give the slot back on every path that claimed one — a client that was admitted and has now
            // disconnected, and one that claimed a slot but was then refused for a duplicate name.
            // Released the moment the client is out of the registries, and deliberately before the
            // transport is disposed: a transport that blocks on close would otherwise hold a slot for as
            // long as it hangs, and it is the hub's registries rather than the socket that maxClients
            // accounts for.
            if (slotReserved)
            {
                ReleaseClientSlot();
            }

            if (connection is not null)
            {
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

    /// <summary>
    /// Claims one of the hub's client slots if one is free.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if a slot was claimed, in which case the caller owns it and must give it
    /// back with <see cref="ReleaseClientSlot"/>; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Internal rather than private so a test can put the hub into the state a concurrent registration
    /// produces — a slot taken, but the client not yet in the registry — which is precisely the window a
    /// check against the observable client count cannot see.
    /// </remarks>
    internal bool TryReserveClientSlot()
    {
        // Compare-and-swap rather than increment-then-test-and-undo. An increment that overshoots the cap
        // is visible to every other registration until it is undone, so a burst that all overshot and all
        // backed out would refuse clients for slots that were never really taken. Here a claim is only
        // ever made from a count that was still under the cap at the instant the claim was made.
        int claimed = Volatile.Read(ref _reservedClientSlots);

        while (claimed < _maxClients)
        {
            int observed = Interlocked.CompareExchange(ref _reservedClientSlots, claimed + 1, claimed);
            if (observed == claimed)
            {
                return true;
            }

            claimed = observed;
        }

        return false;
    }

    /// <summary>
    /// Gives back a client slot claimed by <see cref="TryReserveClientSlot"/>.
    /// </summary>
    /// <remarks>
    /// Internal for the same reason as <see cref="TryReserveClientSlot"/>. Must be called exactly once
    /// per successful claim.
    /// </remarks>
    internal void ReleaseClientSlot()
    {
        Interlocked.Decrement(ref _reservedClientSlots);
    }

    /// <summary>
    /// Tells a registering client the hub is full and records why it was refused.
    /// </summary>
    private async Task RefuseAtCapacityAsync(
        ITransport transport, Guid clientId, CancellationToken cancellationToken)
    {
        byte[] capacityError = [(byte)MessageType.Error, (byte)RegistrationErrorCode.HubAtCapacity];
        await transport.SendAsync(capacityError, cancellationToken).ConfigureAwait(false);
        _logger.LogWarning(
            "Refusing client {ClientId}: hub at capacity ({MaxClients} clients)", clientId, _maxClients);
    }

    private async Task<bool> AuthenticateAsync(
        Guid clientId,
        string clientName,
        byte[] registrationData,
        int nameLength,
        CancellationToken cancellationToken)
    {
        // The authenticator runs on unauthenticated input, once per accepted connection, so an
        // unauthenticated peer can drive it simply by connecting. Bound how many may run at once,
        // otherwise a connection flood turns a deliberately expensive credential check into a
        // denial of service. Waiting is bounded too: a connection that cannot get a slot within the
        // registration timeout is refused as at-capacity rather than held indefinitely.
        if (!await _authenticationSlots!.WaitAsync(_registrationTimeout, cancellationToken).ConfigureAwait(false))
        {
            _logger.LogWarning(
                "Refusing client {ClientId} ({ClientName}): no authentication slot became available within {Timeout}",
                clientId,
                clientName,
                _registrationTimeout);
            return false;
        }

        try
        {
            // Copy the credential out of the registration frame so the context does not alias the larger
            // inbound buffer, which is safer if a caller retains it beyond the call.
            byte[] credential = registrationData.AsSpan(4 + nameLength).ToArray();
            var context = new RegistrationContext { ClientName = clientName, Credential = credential };

            bool authenticated;
            try
            {
                // Bound the authenticator by the registration timeout so a slow or hanging integrator
                // callback cannot hold the handler task (and its connection) open indefinitely. WaitAsync
                // abandons the wait even if the callback ignores the cancellation token.
                authenticated = await _authenticator!(context, cancellationToken)
                    .AsTask()
                    .WaitAsync(_registrationTimeout, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning(
                    "Authenticator did not complete within {Timeout} for client {ClientName}; refusing registration",
                    _registrationTimeout,
                    clientName);
                return false;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // The cancellation came from inside the callback, not from hub shutdown — an HTTP call to
                // an identity provider timing out is the common case. Treat it as a refusal, so the client
                // gets AuthenticationFailed and the reason is logged, rather than letting it unwind to the
                // handler's shutdown catch and drop the connection silently. Callback boundary.
                _logger.LogWarning(
                    "Authenticator was cancelled for client {ClientName}; refusing registration", clientName);
                return false;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A throwing authenticator must refuse the client, not fault the handler. Callback boundary.
                _logger.LogError(
                    ex, "Authenticator threw for client {ClientName}; refusing registration", clientName);
                return false;
            }

            if (!authenticated)
            {
                _logger.LogWarning(
                    "Refusing client {ClientId} ({ClientName}): authentication failed", clientId, clientName);
            }

            return authenticated;
        }
        finally
        {
            _authenticationSlots.Release();
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

    // How many bytes of already-queued frames the send loop will coalesce into a single write before
    // flushing. Small enough to bound the rented buffer, large enough to absorb fan-out bursts.
    private const int SendCoalesceByteBudget = 64 * 1024;

    private async Task SendLoopAsync(ClientConnection connection, CancellationTokenSource clientCts)
    {
        // Reused across the connection's lifetime so coalescing adds no per-frame allocation.
        var batch = new List<ReadOnlyMemory<byte>>();

        try
        {
            await foreach (byte[] payload in connection.OutboundQueue.Reader
                .ReadAllAsync(clientCts.Token).ConfigureAwait(false))
            {
                batch.Add(payload);
                long batchBytes = payload.Length;

                // Drain whatever is already queued so a fan-out burst becomes one write. TryRead never
                // blocks, so a lone frame is sent immediately with no added latency; only frames already
                // waiting are batched, and only up to the byte budget so the write stays bounded.
                while (batchBytes < SendCoalesceByteBudget
                    && connection.OutboundQueue.Reader.TryRead(out byte[]? next))
                {
                    batch.Add(next);
                    batchBytes += next.Length;
                }

                if (batch.Count == 1)
                {
                    await connection.Transport.SendAsync(batch[0], clientCts.Token).ConfigureAwait(false);
                }
                else if (connection.Transport is IBatchSendTransport batchTransport)
                {
                    await batchTransport.SendAsync(batch, clientCts.Token).ConfigureAwait(false);
                }
                else
                {
                    // A transport without batching support still gets every frame, just one at a time.
                    foreach (ReadOnlyMemory<byte> queuedFrame in batch)
                    {
                        await connection.Transport.SendAsync(queuedFrame, clientCts.Token).ConfigureAwait(false);
                    }
                }

                batch.Clear();
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

    private async Task MonitorHeartbeatAsync(
        ClientConnection connection,
        CancellationTokenSource clientCts,
        TimeSpan interval,
        Guid clientId)
    {
        // One timer per connection, reused for the connection's whole lifetime. Between ticks the
        // receive loop bumps ActivitySequence for every frame; an unchanged sequence across a tick
        // means the client sent nothing during that interval, so it is probed and eventually evicted.
        using var timer = new PeriodicTimer(interval);
        long lastSeenActivity = connection.ActivitySequence;
        int missedHeartbeats = 0;

        try
        {
            while (await timer.WaitForNextTickAsync(clientCts.Token).ConfigureAwait(false))
            {
                long currentActivity = connection.ActivitySequence;
                if (currentActivity != lastSeenActivity)
                {
                    // A frame arrived during the interval; the client is alive.
                    lastSeenActivity = currentActivity;
                    missedHeartbeats = 0;
                    continue;
                }

                // The client sent nothing across this interval. Evicting on the _maxMissedHeartbeats'th
                // consecutive silent interval — not the one after it — is what the documented contract
                // promises, so the comparison must be inclusive.
                missedHeartbeats++;
                if (missedHeartbeats >= _maxMissedHeartbeats)
                {
                    _logger.LogInformation(
                        "Client {ClientId} was idle across {Missed} consecutive heartbeat intervals; evicting",
                        clientId,
                        missedHeartbeats);
                    await clientCts.CancelAsync().ConfigureAwait(false);
                    return;
                }

                // Probe liveness via the outbound queue so the ping serialises with any other queued
                // frames. A live client replies with a Pong (or any frame), resetting the counter.
                connection.OutboundQueue.Writer.TryWrite([(byte)MessageType.Ping]);
            }
        }
        catch (OperationCanceledException)
        {
            // The connection's cancellation token was triggered; stop monitoring.
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

    /// <summary>
    /// Admits a client to a group once the configured <see cref="GroupAuthoriser"/> has allowed it.
    /// </summary>
    /// <remarks>
    /// Every join goes through here, including the re-joins a client sends after reconnecting, so an
    /// authorisation decision cannot be carried across a connection or bypassed by a restore: a
    /// reconnected client is a new client id that must be authorised again on its own merits.
    /// </remarks>
    private async Task JoinGroupAsync(
        ClientConnection connection,
        string groupName,
        ReadOnlyMemory<byte> groupNameBytes,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(groupName))
        {
            return;
        }

        if (_groupAuthoriser is not null
            && !await AuthoriseGroupJoinAsync(connection, groupName, cancellationToken).ConfigureAwait(false))
        {
            RefuseGroupJoin(connection, groupName, groupNameBytes);
            return;
        }

        AddToGroup(connection, groupName);
    }

    /// <summary>
    /// Asks the configured group authoriser whether a client may join a group, failing closed on every
    /// outcome that is not an explicit approval.
    /// </summary>
    private async Task<bool> AuthoriseGroupJoinAsync(
        ClientConnection connection, string groupName, CancellationToken cancellationToken)
    {
        var context = new GroupJoinContext
        {
            ClientId = connection.Id,
            ClientName = connection.Name,
            GroupName = groupName,
        };

        try
        {
            // Unlike the registration authenticator, this callback runs on input from an already-admitted
            // client and is driven from that client's own receive loop, which reads nothing further from
            // it until this returns. So there is no semaphore here: the registration authenticator has one
            // because it runs on unauthenticated input, where any peer that reaches the port can drive it,
            // and that is not the position this callback is in.
            //
            // What the wait below bounds is this hub's willingness to wait, not the callback's execution.
            // A callback that outruns the timeout is abandoned and goes on running, so a client that keeps
            // asking after each refusal can leave invocations piling up behind it. Across clients the
            // ceiling is the connected client count, which is maxClients only if one was configured. An
            // authoriser that holds a resource per call must therefore bound its own concurrency; this is
            // documented on the delegate and in the README rather than guessed at with a limit here.
            ValueTask<bool> pending = _groupAuthoriser!(context, cancellationToken);

            // A decision taken synchronously — a lookup against a policy table is the common case — needs
            // no task to bound, and joins recur across a connection's life rather than happening once at
            // registration, so the fast path is worth keeping allocation-free. Anything else, including an
            // already-faulted result, goes through the bounded wait below and is handled by the filters.
            bool authorised = pending.IsCompletedSuccessfully
                ? pending.Result
                : await pending
                    .AsTask()
                    .WaitAsync(_groupAuthorisationTimeout, cancellationToken)
                    .ConfigureAwait(false);

            if (!authorised)
            {
                _logger.LogWarning(
                    "Refusing client {ClientId} ({ClientName}) membership of group {GroupName}: not authorised",
                    connection.Id,
                    connection.Name,
                    groupName);
            }

            return authorised;
        }
        catch (TimeoutException)
        {
            _logger.LogWarning(
                "The group authoriser did not complete within {Timeout} for client {ClientName} joining "
                + "group {GroupName}; refusing the join",
                _groupAuthorisationTimeout,
                connection.Name,
                groupName);
            return false;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // The cancellation came from inside the callback rather than from the client disconnecting or
            // the hub shutting down — a lookup against an external policy store timing out is the common
            // case. Refuse the join and say why, rather than letting it unwind into the receive loop's
            // shutdown catch and drop a live connection. Callback boundary.
            _logger.LogWarning(
                "The group authoriser was cancelled for client {ClientName} joining group {GroupName}; "
                + "refusing the join",
                connection.Name,
                groupName);
            return false;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A throwing authoriser must refuse the join, not fault the client handler. Callback boundary.
            _logger.LogError(
                ex,
                "The group authoriser threw for client {ClientName} joining group {GroupName}; refusing the join",
                connection.Name,
                groupName);
            return false;
        }
    }

    /// <summary>
    /// Tells a client its group join was refused, so it does not go on believing it is a member of a
    /// group it will receive nothing from and may not send to.
    /// </summary>
    private void RefuseGroupJoin(
        ClientConnection connection, string groupName, ReadOnlyMemory<byte> groupNameBytes)
    {
        // Echo the name bytes the client sent rather than re-encoding the string they were decoded from.
        // Re-encoding is not size-preserving: every byte that is not valid UTF-8 decodes to U+FFFD and
        // encodes back as three bytes, so a name of invalid bytes would triple. Group names are not
        // length-capped, so the refusal could then exceed the transport's maximum payload and throw on
        // send — which faults the send loop, and a faulted send loop is awaited during this connection's
        // teardown, abandoning the rest of it including the release of the client's slot. Echoing keeps
        // the refusal no larger than the frame that provoked it, which the transport already bounded.
        var refusal = new byte[1 + groupNameBytes.Length];
        refusal[0] = (byte)MessageType.GroupJoinRefused;
        groupNameBytes.Span.CopyTo(refusal.AsSpan(1));

        if (!connection.OutboundQueue.Writer.TryWrite(refusal))
        {
            _logger.LogWarning(
                "Outbound queue for {ClientId} is full, group join refusal for {GroupName} dropped",
                connection.Id,
                groupName);
        }
    }

    private void AddToGroup(ClientConnection connection, string groupName)
    {
        while (true)
        {
            Group group = _groups.GetOrAdd(groupName, static _ => new Group());
            lock (group.Lock)
            {
                if (group.Removed)
                {
                    // The group was emptied and removed between GetOrAdd and acquiring its lock;
                    // retry so a live instance is used rather than resurrecting a dead one.
                    continue;
                }

                group.Members.Add(connection.Id);
                connection.Groups.Add(groupName);
            }

            _logger.LogDebug("Client {ClientId} joined group {GroupName}", connection.Id, groupName);
            return;
        }
    }

    private void LeaveGroup(ClientConnection connection, string groupName)
    {
        if (string.IsNullOrEmpty(groupName))
        {
            return;
        }

        RemoveMemberFromGroup(connection.Id, groupName);
        connection.Groups.Remove(groupName);

        _logger.LogDebug("Client {ClientId} left group {GroupName}", connection.Id, groupName);
    }

    private void RemoveFromAllGroups(ClientConnection connection)
    {
        foreach (string groupName in connection.Groups)
        {
            RemoveMemberFromGroup(connection.Id, groupName);
        }

        connection.Groups.Clear();
    }

    private void RemoveMemberFromGroup(Guid clientId, string groupName)
    {
        if (!_groups.TryGetValue(groupName, out Group? group))
        {
            return;
        }

        lock (group.Lock)
        {
            if (group.Members.Remove(clientId) && group.Members.Count == 0)
            {
                // Last member left: mark the group removed under its lock and take it out of the
                // dictionary only if this exact instance is still mapped, so a group another thread
                // created under the same name is never dropped.
                group.Removed = true;
                _groups.TryRemove(new KeyValuePair<string, Group>(groupName, group));
            }
        }
    }

    private void SendToGroup(
        Guid senderId,
        string groupName,
        ReadOnlyMemory<byte> groupNameBytes,
        ReadOnlyMemory<byte> messageData)
    {
        if (!_groups.TryGetValue(groupName, out Group? group))
        {
            return;
        }

        // Sending to a group is a member's privilege. Membership is what a join grants — and what the
        // group authoriser decides — so a client that never joined, or was refused, must not be able to
        // inject a frame that reaches every member carrying its id. The test is a set lookup against the
        // live membership inside the lock, not a scan of the snapshot afterwards, so a sender removed
        // from the group cannot slip a message through the gap. Null means not a member: taking that
        // branch outside the lock keeps the logging call out of the critical section.
        Guid[]? recipients = null;
        lock (group.Lock)
        {
            if (group.Members.Contains(senderId))
            {
                // Snapshot membership with a plain CopyTo — no LINQ closure or enumerator in the
                // critical section — so the queues can be written without holding the lock. The sender
                // is filtered out during delivery below rather than inside the lock.
                recipients = new Guid[group.Members.Count];
                group.Members.CopyTo(recipients);
            }
        }

        if (recipients is null)
        {
            _logger.LogDebug(
                "Group message from {SenderId} to group {GroupName} dropped: the sender is not a member",
                senderId,
                groupName);
            return;
        }

        if (recipients.Length == 1 && recipients[0] == senderId)
        {
            // The sender is the only member; nothing to deliver and no frame to build.
            return;
        }

        // One shared, never-mutated delivery frame across every recipient's queue (see
        // BroadcastMessage). The frame carries the group name so recipients know its origin. The
        // name bytes are copied straight from the inbound frame rather than re-encoding the string.
        int nameLength = groupNameBytes.Length;
        var deliveryPayload = new byte[1 + 16 + 2 + nameLength + messageData.Length];
        deliveryPayload[0] = (byte)MessageType.DeliverGroupMessage;
        senderId.TryWriteBytes(deliveryPayload.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(deliveryPayload.AsSpan(17, 2), (ushort)nameLength);
        groupNameBytes.Span.CopyTo(deliveryPayload.AsSpan(19));
        messageData.CopyTo(deliveryPayload.AsMemory(19 + nameLength));

        foreach (Guid recipientId in recipients)
        {
            if (recipientId == senderId)
            {
                // A group message is not echoed back to its sender.
                continue;
            }

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

    // A single group's membership, guarded by its own lock. Removed is set true under Lock when the
    // group is taken out of _groups so a concurrent join that already fetched this instance retries
    // against a fresh one rather than resurrecting a dead group.
    private sealed class Group
    {
        public Lock Lock { get; } = new();
        public HashSet<Guid> Members { get; } = new();
        public bool Removed { get; set; }
    }

    private sealed class ClientConnection(Guid id, string name, ITransport transport) : IAsyncDisposable
    {
        private const int OutboundQueueCapacity = 1024;
        private int _disposed;
        private long _activitySequence;

        public Guid Id { get; } = id;
        public string Name { get; } = name;
        public ITransport Transport { get; } = transport;

        /// <summary>
        /// A monotonically increasing counter bumped once for every frame received from the client.
        /// The heartbeat monitor compares it between ticks to detect an idle connection without
        /// arming a timer per received frame.
        /// </summary>
        public long ActivitySequence => Volatile.Read(ref _activitySequence);

        public void RecordActivity()
        {
            Interlocked.Increment(ref _activitySequence);
        }

        /// <summary>
        /// The set of groups this client has joined. Only ever touched by this connection's own
        /// receive loop and its teardown (which runs after the loop ends), so it needs no lock.
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
