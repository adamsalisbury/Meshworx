namespace AdamSalisbury.Meshworx.Transport;

/// <summary>
/// Listens for and accepts incoming transport connections.
/// </summary>
/// <remarks>
/// Disposing the listener stops it and releases all associated resources.
/// Pending calls to <see cref="AcceptAsync"/> should be cancelled via the
/// <see cref="CancellationToken"/> before disposing.
/// </remarks>
public interface ITransportListener : IAsyncDisposable
{
    /// <summary>
    /// Begins listening for incoming connections.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits for and accepts the next incoming connection.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An <see cref="ITransport"/> representing the accepted connection.</returns>
    Task<ITransport> AcceptAsync(CancellationToken cancellationToken = default);
}
