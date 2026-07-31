namespace AdamSalisbury.Meshworx.Backplane;

/// <summary>
/// A pluggable channel that lets several independent <see cref="MeshHub"/> instances — typically several
/// processes behind a load balancer, each holding a different subset of the overall client population —
/// behave as one logical hub: a client connected to one instance can reach a client, group or topic that
/// exists only on another.
/// </summary>
/// <remarks>
/// <para>
/// Two responsibilities, deliberately kept together in one seam rather than split across two: publishing
/// a message so every other instance can materialise whatever delivery it addresses that instance's own
/// clients (<see cref="PublishAsync"/>/<see cref="StartAsync"/>), and a shared directory mapping a
/// client's name to its id so a lookup for a name connected to a <em>different</em> instance still
/// resolves (<see cref="RegisterClientAsync"/>/<see cref="UnregisterClientAsync"/>/
/// <see cref="TryResolveClientAsync"/>).
/// </para>
/// <para>
/// A hub not configured with one behaves exactly as it always has — this is an entirely additive,
/// opt-in seam. See <see cref="InMemoryHubBackplane"/> for a process-local implementation suitable for
/// tests (and for genuinely running several <see cref="MeshHub"/> instances in one process), and the
/// separate <c>AdamSalisbury.Meshworx.Backplane.Redis</c> package for a real cross-process one.
/// </para>
/// <para>
/// A backplane is shared by design — many hub instances start against the very same
/// <see cref="IHubBackplane"/> object (in-process) or the same underlying store/bus (Redis, one
/// connection per process). Because of that, a hub never disposes the backplane it was given: it calls
/// <see cref="StopAsync"/> on its own instance id when it stops, and the object's actual lifecycle —
/// <see cref="IAsyncDisposable.DisposeAsync"/> — is the caller's to manage, once every hub sharing it is
/// done with it.
/// </para>
/// </remarks>
public interface IHubBackplane : IAsyncDisposable
{
    /// <summary>
    /// Begins receiving messages other instances publish, and identifies this instance for the lifetime
    /// of the subscription.
    /// </summary>
    /// <param name="instanceId">
    /// This hub instance's own identifier. Echoed back on every message this instance itself publishes
    /// via <see cref="PublishAsync"/>, so <paramref name="onMessage"/> can recognise, and skip, its own
    /// publications — see <see cref="BackplaneMessage.OriginInstanceId"/>.
    /// </param>
    /// <param name="onMessage">
    /// Invoked for every message published across the backplane, including — deliberately, so the
    /// callback alone decides what to do with it — this instance's own. Exceptions the callback raises
    /// are the implementation's to handle; a caller should not let one escape into whatever transport
    /// mechanism the implementation uses to receive messages.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="instanceId"/> is already started on this backplane.
    /// </exception>
    Task StartAsync(
        Guid instanceId,
        Func<BackplaneMessage, CancellationToken, Task> onMessage,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops receiving messages for the given instance, without affecting any other instance still
    /// started on this same backplane. Safe to call on an instance that was never started, or already
    /// stopped.
    /// </summary>
    /// <param name="instanceId">The instance id previously passed to <see cref="StartAsync"/>.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task StopAsync(Guid instanceId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message for every other instance sharing this backplane to materialise whatever
    /// delivery it addresses against their own locally-connected clients.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task PublishAsync(BackplaneMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records that a client is now connected to this instance, so a lookup for its name from any
    /// instance sharing this backplane resolves to its id.
    /// </summary>
    /// <param name="clientName">The client's name.</param>
    /// <param name="clientId">The client's id.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task RegisterClientAsync(string clientName, Guid clientId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a client's directory entry, once it disconnects from this instance.
    /// </summary>
    /// <param name="clientName">The client's name.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task UnregisterClientAsync(string clientName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves a client name to its id via the shared directory, regardless of which instance it is
    /// actually connected to.
    /// </summary>
    /// <param name="clientName">The name to resolve.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The client's id, or <see langword="null"/> if no instance has registered that name.</returns>
    Task<Guid?> TryResolveClientAsync(string clientName, CancellationToken cancellationToken = default);
}
