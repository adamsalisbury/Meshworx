using System.Threading.Channels;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AdamSalisbury.Meshworx;

/// <summary>
/// Keeps a <see cref="IMeshClient"/> connected to a hub, transparently re-establishing the connection
/// when it is lost. The managed client is exposed through <see cref="Client"/> for sending and
/// receiving; this type only owns the connection lifecycle.
/// </summary>
/// <remarks>
/// The initial connection is attempted once by <see cref="StartAsync"/>, which throws if it fails. After
/// a successful start, an unexpected disconnect triggers reconnection: each attempt is bounded by the
/// connect timeout and retried after the retry delay until it succeeds or the reconnector is disposed.
/// <para>
/// By default the reconnector re-joins the groups the client belonged to before the drop, so group
/// messages resume without any application involvement. Pass <c>restoreGroupMembership: false</c> to the
/// constructor to take full manual control and restore membership yourself in a <see cref="Reconnected"/>
/// handler. In-flight messages are never re-sent — that remains the application's responsibility.
/// </para>
/// </remarks>
public sealed class MeshClientReconnector : IAsyncDisposable
{
    private static readonly TimeSpan DefaultRetryDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DefaultConnectTimeout = TimeSpan.FromSeconds(10);

    private readonly string _clientName;
    private readonly ReadOnlyMemory<byte> _credential;
    private readonly Func<CancellationToken, Task<ITransport>> _transportFactory;
    private readonly TimeSpan _retryDelay;
    private readonly TimeSpan _connectTimeout;
    private readonly bool _restoreGroupMembership;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _stopCts = new();

    // Coalesces disconnect notifications: at most one pending reconnect request is queued at a time.
    private readonly Channel<byte> _reconnectSignals =
        Channel.CreateBounded<byte>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    // The groups awaiting restoration after a reconnect, guarded by _restoreLock. Disconnects union the
    // client's last membership into this set; the reconnect loop drains it as each group is successfully
    // re-joined. Union-then-drain (rather than overwrite) means a restore interrupted by a fresh drop
    // leaves the not-yet-restored groups pending, so they survive to the next reconnect instead of being
    // lost from the client's now-depleted live membership.
    private readonly Lock _restoreLock = new();
    private readonly HashSet<string> _pendingGroupRestore = new(StringComparer.Ordinal);

    private Task? _reconnectLoopTask;
    private int _started;
    private int _disposed;

    /// <summary>
    /// Initialises a new <see cref="MeshClientReconnector"/>.
    /// </summary>
    /// <param name="client">The client whose connection is managed.</param>
    /// <param name="clientName">The name to register with on every connection.</param>
    /// <param name="transportFactory">Creates a fresh transport for each connection attempt.</param>
    /// <param name="retryDelay">How long to wait between failed reconnect attempts. Defaults to 1 second.</param>
    /// <param name="connectTimeout">The maximum time a single connection attempt may take. Defaults to 10 seconds.</param>
    /// <param name="restoreGroupMembership">
    /// Whether to automatically re-join the groups the client belonged to before a drop once the connection
    /// is re-established, before raising <see cref="Reconnected"/>. Defaults to <see langword="true"/>; set
    /// it to <see langword="false"/> to restore membership manually in a <see cref="Reconnected"/> handler.
    /// </param>
    /// <param name="logger">An optional logger.</param>
    /// <param name="credential">
    /// An opaque credential presented to the hub's authenticator on every connection attempt, including
    /// reconnects. Empty by default.
    /// </param>
    public MeshClientReconnector(
        IMeshClient client,
        string clientName,
        Func<CancellationToken, Task<ITransport>> transportFactory,
        TimeSpan? retryDelay = null,
        TimeSpan? connectTimeout = null,
        bool restoreGroupMembership = true,
        ILogger<MeshClientReconnector>? logger = null,
        ReadOnlyMemory<byte> credential = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(clientName);
        ArgumentNullException.ThrowIfNull(transportFactory);

        if (retryDelay is { } delay && delay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retryDelay), "The retry delay must be positive.");
        }

        if (connectTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(connectTimeout), "The connect timeout must be positive.");
        }

        Client = client;
        _clientName = clientName;
        _credential = credential;
        _transportFactory = transportFactory;
        _retryDelay = retryDelay ?? DefaultRetryDelay;
        _connectTimeout = connectTimeout ?? DefaultConnectTimeout;
        _restoreGroupMembership = restoreGroupMembership;
        _logger = logger ?? NullLogger<MeshClientReconnector>.Instance;
    }

    /// <summary>
    /// Gets the managed client. Use it to send and receive messages and to observe connection state.
    /// </summary>
    public IMeshClient Client { get; }

    /// <summary>
    /// Raised after the connection has been re-established following an unexpected disconnect. When
    /// <c>restoreGroupMembership</c> is enabled the client's previous groups have already been re-joined
    /// by the time this fires, so handlers only need to restore any remaining connection-scoped state.
    /// </summary>
    public event EventHandler? Reconnected;

    /// <summary>
    /// Establishes the initial connection and begins monitoring it for unexpected disconnects.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the initial connection attempt.</param>
    /// <exception cref="InvalidOperationException">The reconnector has already been started.</exception>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 1) == 1)
        {
            throw new InvalidOperationException("The reconnector has already been started.");
        }

        try
        {
            // Fail fast if the first connection cannot be made; the caller decides how to handle that.
            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            attemptCts.CancelAfter(_connectTimeout);
            ITransport transport = await _transportFactory(attemptCts.Token).ConfigureAwait(false);
            await Client.ConnectAsync(transport, _clientName, _credential, attemptCts.Token).ConfigureAwait(false);
        }
        catch
        {
            // The initial connection failed; allow StartAsync to be retried rather than locking the
            // reconnector into a started-but-unconnected state.
            Volatile.Write(ref _started, 0);
            throw;
        }

        Client.Disconnected += OnDisconnected;

        // ConnectAsync returns with the client's receive loop already running on a background task, so the
        // connection can be lost before the line above attaches the handler. That disconnect would be
        // raised with no subscriber, leaving nothing to signal the reconnect loop and no other trigger to
        // fall back on. Re-read the state now the handler is attached, and queue the signal the lost event
        // would have queued. The client reports itself disconnected from the moment teardown begins, well
        // before it raises Disconnected, so a drop in the window is always visible here.
        //
        // The cost of that early visibility is that a teardown straddling the subscription is seen twice:
        // once here, and again when the event reaches the handler. The two do not coalesce — the channel
        // only merges writes that overlap in the queue, and the loop drains before it reconnects — so the
        // duplicate is dealt with where it is serviced, by the revalidation guard in ConnectWithRetryAsync.
        if (!Client.IsConnected)
        {
            _reconnectSignals.Writer.TryWrite(0);
        }

        _reconnectLoopTask = ReconnectLoopAsync(_stopCts.Token);
    }

    private void OnDisconnected(object? sender, DisconnectedEventArgs e)
    {
        if (_restoreGroupMembership && e.JoinedGroups.Count > 0)
        {
            // Union rather than overwrite: a restore interrupted by a fresh drop leaves groups still
            // pending, and this disconnect only reports the client's now-depleted live membership.
            // Adding to the pending set preserves those not-yet-restored groups instead of losing them.
            lock (_restoreLock)
            {
                foreach (string group in e.JoinedGroups)
                {
                    _pendingGroupRestore.Add(group);
                }
            }
        }

        // Coalescing channel: signals the reconnect loop without blocking the client's receive loop.
        _reconnectSignals.Writer.TryWrite(0);
    }

    private async Task ReconnectLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _reconnectSignals.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (_reconnectSignals.Reader.TryRead(out _))
                {
                    // Drain coalesced signals; one reconnect covers them all.
                }

                await ConnectWithRetryAsync(cancellationToken).ConfigureAwait(false);

                if (_restoreGroupMembership)
                {
                    await RestoreGroupMembershipAsync(cancellationToken).ConfigureAwait(false);
                }

                try
                {
                    Reconnected?.Invoke(this, EventArgs.Empty);
                }
                catch (Exception ex)
                {
                    // Callback boundary — a throwing handler must not stop the reconnect loop.
                    _logger.LogError(ex, "A Reconnected handler threw an exception");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Disposed.
        }
    }

    private async Task ConnectWithRetryAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // A queued signal records that the connection was lost, not that it is still lost: it may
            // already have been recovered, either by an earlier pass servicing the same drop or by an
            // application Disconnected handler reconnecting from within itself, which the client
            // explicitly supports. Reconnecting a live client is not merely wasteful but impossible —
            // the client refuses a connect unless it is fully disconnected — so retrying towards a state
            // that has already been reached would loop for ever. Treat it as the goal met instead.
            if (Client.IsConnected)
            {
                return;
            }

            try
            {
                using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                attemptCts.CancelAfter(_connectTimeout);

                ITransport transport = await _transportFactory(attemptCts.Token).ConfigureAwait(false);

                try
                {
                    await Client.ConnectAsync(transport, _clientName, _credential, attemptCts.Token).ConfigureAwait(false);
                }
                catch
                {
                    // The client only takes ownership of the transport once it accepts it, so a connect
                    // rejected before that point leaves nothing else to close this one. Disposal is
                    // idempotent, so this is safe on the paths where the client did clean up itself.
                    await transport.DisposeAsync().ConfigureAwait(false);
                    throw;
                }

                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Reconnect attempt failed; retrying in {RetryDelay}", _retryDelay);
                await Task.Delay(_retryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RestoreGroupMembershipAsync(CancellationToken cancellationToken)
    {
        string[] groups;
        lock (_restoreLock)
        {
            if (_pendingGroupRestore.Count == 0)
            {
                return;
            }

            groups = [.. _pendingGroupRestore];
        }

        foreach (string group in groups)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await Client.JoinGroupAsync(group, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The connection dropped again mid-restore, or the join otherwise failed. The group stays
                // in the pending set (it is only removed on success below), so the reconnect that the
                // drop triggers retries it. Stop this pass rather than hammering a likely-dead connection.
                _logger.LogWarning(
                    ex, "Failed to restore membership of group {GroupName} after reconnect; it remains pending", group);
                return;
            }

            // Re-joined successfully: drop it from the pending set so a later reconnect need not repeat it.
            lock (_restoreLock)
            {
                _pendingGroupRestore.Remove(group);
            }
        }
    }

    /// <summary>
    /// Stops monitoring the connection and disconnects the managed client.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _stopCts.CancelAsync().ConfigureAwait(false);
        Client.Disconnected -= OnDisconnected;

        if (_reconnectLoopTask is not null)
        {
            try
            {
                await _reconnectLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
        }

        await Client.DisconnectAsync().ConfigureAwait(false);
        _stopCts.Dispose();
    }
}
