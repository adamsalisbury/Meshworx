namespace AdamSalisbury.Meshworx.Transport;

/// <summary>
/// Listens for and accepts incoming transport connections.
/// </summary>
/// <remarks>
/// Disposing the listener stops it and releases all associated resources.
/// Pending calls to <see cref="AcceptAsync"/> should be cancelled via the
/// <see cref="CancellationToken"/> before disposing.
/// <para>
/// An implementation must not rely on callers doing so, however. A listener disposed with an accept still
/// pending must end that accept with an <see cref="ObjectDisposedException"/> rather than leaving the
/// caller waiting or reporting a transport-level error, and must throw the same for any accept attempted
/// afterwards. An accept loop uses that distinction to tell "this listener is finished" from "that one
/// connection failed": the former stops the loop, whereas the latter is logged and retried, which against
/// a listener that is never coming back would spin without end.
/// </para>
/// <para>
/// Disposal is idempotent and safe to call concurrently. Only the first call tears the listener down, and
/// every call — first or not — returns only once that teardown is complete.
/// </para>
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
