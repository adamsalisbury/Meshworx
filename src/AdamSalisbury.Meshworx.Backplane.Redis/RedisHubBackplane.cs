using StackExchange.Redis;

namespace AdamSalisbury.Meshworx.Backplane.Redis;

/// <summary>
/// A Redis-backed <see cref="IHubBackplane"/>: messages are published/subscribed on a single Redis
/// pub/sub channel, and the client-name directory is a single Redis hash — the "keyspace" the issue this
/// package implements calls for.
/// </summary>
/// <remarks>
/// <para>
/// This type owns neither the <see cref="IConnectionMultiplexer"/> it is given nor, by extension, the
/// Redis connection itself — the caller creates and disposes the multiplexer, exactly as
/// <see cref="IHubBackplane"/>'s own remarks describe for a backplane object shared across several hub
/// instances. Multiple <see cref="RedisHubBackplane"/> instances (one per process, typically) built
/// against the same Redis server and the same channel/directory key (both constructor parameters) share
/// state exactly as if they were the same in-process object.
/// </para>
/// <para>
/// Every hub instance sharing a channel receives every message published on it, including a message a
/// different <see cref="RedisHubBackplane"/> published from a different process — Redis pub/sub does not
/// distinguish "this instance's own connection" the way a purely in-process implementation could skip a
/// step for. <see cref="MeshHub"/> itself is what filters a message back out by
/// <see cref="BackplaneMessage.OriginInstanceId"/>, so this type does not need to.
/// </para>
/// </remarks>
public sealed class RedisHubBackplane : IHubBackplane
{
    private readonly IConnectionMultiplexer _connection;
    private readonly RedisChannel _channel;
    private readonly RedisKey _directoryKey;

    // One handler per started instance id, so StopAsync can unsubscribe exactly the one it was given —
    // ISubscriber itself has no notion of "per caller" subscriptions, only "per channel", so this is
    // this type's own bookkeeping of who asked to be told what.
    private readonly Dictionary<Guid, Action<RedisChannel, RedisValue>> _handlersByInstance = new();
    private readonly Lock _handlersLock = new();

    private int _disposed;

    /// <summary>
    /// Initialises a new instance of <see cref="RedisHubBackplane"/>.
    /// </summary>
    /// <param name="connection">
    /// An already-connected multiplexer. Not owned or disposed by this type — see the type's own remarks.
    /// </param>
    /// <param name="channel">
    /// The Redis pub/sub channel every hub instance sharing this backplane publishes to and subscribes
    /// on. Defaults to <c>"meshworx:backplane"</c>. Every instance sharing state must use the same value.
    /// </param>
    /// <param name="directoryKey">
    /// The Redis hash key the client-name directory is stored under. Defaults to
    /// <c>"meshworx:directory"</c>. Every instance sharing state must use the same value.
    /// </param>
    public RedisHubBackplane(
        IConnectionMultiplexer connection,
        string channel = "meshworx:backplane",
        string directoryKey = "meshworx:directory")
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrEmpty(channel);
        ArgumentException.ThrowIfNullOrEmpty(directoryKey);

        _connection = connection;
        _channel = RedisChannel.Literal(channel);
        _directoryKey = directoryKey;
    }

    /// <inheritdoc/>
    public async Task StartAsync(
        Guid instanceId,
        Func<BackplaneMessage, CancellationToken, Task> onMessage,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentNullException.ThrowIfNull(onMessage);

        lock (_handlersLock)
        {
            if (_handlersByInstance.ContainsKey(instanceId))
            {
                throw new InvalidOperationException($"Instance {instanceId} is already started on this backplane.");
            }
        }

        // StackExchange.Redis invokes a subscription's handler on its own internal completion thread,
        // synchronously — an async void-shaped callback here would let an exception escape unobserved
        // and would not honour back-pressure the way an awaited handler chain does. Fire it onto the
        // thread pool and let the exception filter below decide what to do with a failure, rather than
        // ever blocking Redis's own dispatch thread on this instance's handler.
        void Handler(RedisChannel publishedChannel, RedisValue value)
        {
            _ = ProcessAsync(value, onMessage, cancellationToken);
        }

        lock (_handlersLock)
        {
            _handlersByInstance[instanceId] = Handler;
        }

        ISubscriber subscriber = _connection.GetSubscriber();
        await subscriber.SubscribeAsync(_channel, Handler).ConfigureAwait(false);
    }

    private static async Task ProcessAsync(
        RedisValue value, Func<BackplaneMessage, CancellationToken, Task> onMessage, CancellationToken cancellationToken)
    {
        if (!value.HasValue)
        {
            return;
        }

        BackplaneMessage message;
        try
        {
            message = BackplaneMessageSerializer.Deserialize((byte[])value!);
        }
        catch (FormatException)
        {
            // A malformed payload on the shared channel — not this backplane's to interpret further;
            // dropped the same way a malformed frame anywhere else in this library is (KI-9).
            return;
        }

        try
        {
            await onMessage(message, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
    }

    /// <inheritdoc/>
    public async Task StopAsync(Guid instanceId, CancellationToken cancellationToken = default)
    {
        Action<RedisChannel, RedisValue>? handler;
        lock (_handlersLock)
        {
            if (!_handlersByInstance.Remove(instanceId, out handler))
            {
                return;
            }
        }

        ISubscriber subscriber = _connection.GetSubscriber();
        await subscriber.UnsubscribeAsync(_channel, handler).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task PublishAsync(BackplaneMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        byte[] payload = BackplaneMessageSerializer.Serialize(message);
        ISubscriber subscriber = _connection.GetSubscriber();
        await subscriber.PublishAsync(_channel, payload).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task RegisterClientAsync(string clientName, Guid clientId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientName);

        IDatabase database = _connection.GetDatabase();
        await database.HashSetAsync(_directoryKey, clientName, clientId.ToString("N")).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task UnregisterClientAsync(string clientName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientName);

        IDatabase database = _connection.GetDatabase();
        await database.HashDeleteAsync(_directoryKey, clientName).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<Guid?> TryResolveClientAsync(string clientName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientName);

        IDatabase database = _connection.GetDatabase();
        RedisValue value = await database.HashGetAsync(_directoryKey, clientName).ConfigureAwait(false);

        return value.HasValue && Guid.TryParseExact(value.ToString(), "N", out Guid id) ? id : null;
    }

    /// <summary>
    /// Unsubscribes every instance still started on this backplane. Does <b>not</b> dispose the
    /// <see cref="IConnectionMultiplexer"/> passed to the constructor — that connection is not this
    /// type's to own, see the type's own remarks.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        ISubscriber subscriber = _connection.GetSubscriber();
        await subscriber.UnsubscribeAsync(_channel).ConfigureAwait(false);

        lock (_handlersLock)
        {
            _handlersByInstance.Clear();
        }
    }
}
