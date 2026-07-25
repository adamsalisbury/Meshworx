namespace AdamSalisbury.Meshworx;

public interface IMeshHub : IAsyncDisposable
{
    /// <summary>
    /// Starts the hub, binding to its configured endpoint and accepting client connections.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">
    /// The hub is already running, is still stopping, or was stopped while this call was starting it.
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
    /// notified once, not once per caller. A hub that has stopped may be started again, unless it has
    /// been disposed.
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
    /// Determines whether a client with the specified identifier is currently registered.
    /// </summary>
    /// <param name="clientId">The unique identifier of the client to look up.</param>
    /// <returns><see langword="true"/> if the client is registered; otherwise, <see langword="false"/>.</returns>
    bool IsClientRegistered(Guid clientId);
}
