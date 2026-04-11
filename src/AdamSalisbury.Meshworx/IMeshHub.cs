namespace AdamSalisbury.Meshworx;

public interface IMeshHub : IAsyncDisposable
{
    /// <summary>
    /// Starts the hub, binding to its configured endpoint and accepting client connections.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The hub is already running.</exception>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the hub, disconnecting all registered clients and releasing all resources.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Determines whether a client with the specified identifier is currently registered.
    /// </summary>
    /// <param name="clientId">The unique identifier of the client to look up.</param>
    /// <returns><see langword="true"/> if the client is registered; otherwise, <see langword="false"/>.</returns>
    bool IsClientRegistered(Guid clientId);
}
