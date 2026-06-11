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
/// This type does not re-join groups or re-send in-flight messages after a reconnect — application state
/// is the application's responsibility. Subscribe to <see cref="Reconnected"/> to restore it, for example
/// by re-joining the groups in <see cref="IMeshClient.JoinedGroups"/> captured before the drop.
/// </para>
/// </remarks>
public sealed class MeshClientReconnector : IAsyncDisposable
{
    private static readonly TimeSpan DefaultRetryDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DefaultConnectTimeout = TimeSpan.FromSeconds(10);

    private readonly string _clientName;
    private readonly Func<CancellationToken, Task<ITransport>> _transportFactory;
    private readonly TimeSpan _retryDelay;
    private readonly TimeSpan _connectTimeout;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _stopCts = new();

    // Coalesces disconnect notifications: at most one pending reconnect request is queued at a time.
    private readonly Channel<byte> _reconnectSignals =
        Channel.CreateBounded<byte>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    private Task? _reconnectLoopTask;
    private int _started;

    /// <summary>
    /// Initialises a new <see cref="MeshClientReconnector"/>.
    /// </summary>
    /// <param name="client">The client whose connection is managed.</param>
    /// <param name="clientName">The name to register with on every connection.</param>
    /// <param name="transportFactory">Creates a fresh transport for each connection attempt.</param>
    /// <param name="retryDelay">How long to wait between failed reconnect attempts. Defaults to 1 second.</param>
    /// <param name="connectTimeout">The maximum time a single connection attempt may take. Defaults to 10 seconds.</param>
    /// <param name="logger">An optional logger.</param>
    public MeshClientReconnector(
        IMeshClient client,
        string clientName,
        Func<CancellationToken, Task<ITransport>> transportFactory,
        TimeSpan? retryDelay = null,
        TimeSpan? connectTimeout = null,
        ILogger<MeshClientReconnector>? logger = null)
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
        _transportFactory = transportFactory;
        _retryDelay = retryDelay ?? DefaultRetryDelay;
        _connectTimeout = connectTimeout ?? DefaultConnectTimeout;
        _logger = logger ?? NullLogger<MeshClientReconnector>.Instance;
    }

    /// <summary>
    /// Gets the managed client. Use it to send and receive messages and to observe connection state.
    /// </summary>
    public IMeshClient Client { get; }

    /// <summary>
    /// Raised after the connection has been re-established following an unexpected disconnect. Handlers
    /// may restore connection-scoped state such as group membership.
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
            await Client.ConnectAsync(transport, _clientName, attemptCts.Token).ConfigureAwait(false);
        }
        catch
        {
            // The initial connection failed; allow StartAsync to be retried rather than locking the
            // reconnector into a started-but-unconnected state.
            Volatile.Write(ref _started, 0);
            throw;
        }

        Client.Disconnected += OnDisconnected;
        _reconnectLoopTask = ReconnectLoopAsync(_stopCts.Token);
    }

    private void OnDisconnected(object? sender, DisconnectedEventArgs e)
    {
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
            try
            {
                using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                attemptCts.CancelAfter(_connectTimeout);

                ITransport transport = await _transportFactory(attemptCts.Token).ConfigureAwait(false);
                await Client.ConnectAsync(transport, _clientName, attemptCts.Token).ConfigureAwait(false);
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

    /// <summary>
    /// Stops monitoring the connection and disconnects the managed client.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
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
