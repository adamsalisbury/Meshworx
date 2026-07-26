using AdamSalisbury.Meshworx.Transport;

namespace AdamSalisbury.Meshworx;

public interface IMeshHub : IAsyncDisposable
{
    /// <summary>
    /// Starts the hub, binding to its configured endpoint and accepting client connections.
    /// </summary>
    /// <remarks>
    /// Concurrent calls are refused: one starts the hub and the rest throw. A call made while a shutdown
    /// is still in progress is refused for the same reason.
    /// </remarks>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">
    /// The hub is already running, is already being started, or is still stopping.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The hub has been disposed.</exception>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the hub, disconnecting all registered clients and releasing all resources.
    /// </summary>
    /// <remarks>
    /// Idempotent and safe to call from more than one thread at a time. Calling it on a hub that is not
    /// running does nothing. When several calls overlap, one of them performs the shutdown and the rest
    /// await it, so every one of them returns only once the hub has actually stopped — the clients are
    /// notified once, not once per caller.
    /// <para>
    /// This releases the hub's own state, but not the transport listener's: <see cref="ITransportListener"/>
    /// has no stop, so the endpoint stays bound and both listeners in this library refuse a second
    /// <see cref="ITransportListener.StartAsync"/>. Treat a stopped hub as spent and dispose it, unless the
    /// listener is known to tolerate being started again.
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">
    /// A token to cancel the operation. Cancelling a call that joined a shutdown already in progress
    /// abandons the wait; it does not cancel the shutdown.
    /// </param>
    Task StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Raised after a client completes registration and becomes reachable on the hub.
    /// </summary>
    /// <remarks>
    /// Raised from the client's handler task, so handlers may be invoked concurrently for different
    /// clients and must be thread-safe.
    /// </remarks>
    event EventHandler<ClientConnectionEventArgs> ClientConnected;

    /// <summary>
    /// Raised after a registered client disconnects and is removed from the hub.
    /// </summary>
    /// <remarks>
    /// Raised from the client's handler task, so handlers may be invoked concurrently for different
    /// clients and must be thread-safe.
    /// </remarks>
    event EventHandler<ClientConnectionEventArgs> ClientDisconnected;

    /// <summary>
    /// Gets the number of clients currently registered with the hub.
    /// </summary>
    /// <remarks>
    /// The value is a point-in-time snapshot; clients may connect or disconnect concurrently.
    /// </remarks>
    int ConnectedClientCount { get; }

    /// <summary>
    /// Gets a value indicating whether the hub is currently running and accepting connections.
    /// </summary>
    /// <remarks>
    /// <see langword="true"/> from the moment <see cref="StartAsync"/> completes until
    /// <see cref="StopAsync"/> begins tearing the hub down. The value is a point-in-time snapshot; the
    /// hub's lifecycle may change concurrently.
    /// </remarks>
    bool IsRunning { get; }

    /// <summary>
    /// Gets the maximum number of clients the hub admits at once.
    /// </summary>
    int MaxClients { get; }

    /// <summary>
    /// Determines whether a client with the specified identifier is currently registered.
    /// </summary>
    /// <param name="clientId">The unique identifier of the client to look up.</param>
    /// <returns><see langword="true"/> if the client is registered; otherwise, <see langword="false"/>.</returns>
    bool IsClientRegistered(Guid clientId);
}
