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
    /// clients and must be thread-safe. Also raised, carrying the reclaimed identity, when a connection
    /// successfully resumes a dormant session — the connection was never unreachable, but a subscriber
    /// tracking connected ids by id needs the new one, paired with a <see cref="ClientDisconnected"/> for
    /// the id it is replacing.
    /// </remarks>
    event EventHandler<ClientConnectionEventArgs> ClientConnected;

    /// <summary>
    /// Raised after a registered client disconnects and is removed from the hub.
    /// </summary>
    /// <remarks>
    /// Raised from the client's handler task, so handlers may be invoked concurrently for different
    /// clients and must be thread-safe. Also raised, carrying the discarded fresh identity, when a
    /// connection successfully resumes a dormant session under a different id — the connection itself has
    /// not actually disconnected, only the id it answers to has changed, and this is immediately followed
    /// by a <see cref="ClientConnected"/> for the reclaimed id.
    /// </remarks>
    event EventHandler<ClientConnectionEventArgs> ClientDisconnected;

    /// <summary>
    /// Raised whenever a message is dropped because the recipient's outbound queue was full, so
    /// saturation is observable in-process even when no wire-level notification was configured.
    /// </summary>
    /// <remarks>
    /// Raised from the client handler task that attempted the routing, so handlers may be invoked
    /// concurrently for different senders and recipients and must be thread-safe. This is always raised,
    /// for every shape of send — direct, broadcast and group alike — independently of whether the hub was
    /// constructed with <c>notifyOnQueueSaturation</c>. That flag only controls whether the
    /// <em>sender</em> is additionally told over the wire, which happens for directly addressed sends
    /// only.
    /// </remarks>
    event EventHandler<QueueSaturatedEventArgs> QueueSaturated;

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
    /// Gets the number of client slots currently claimed against <see cref="MaxClients"/>.
    /// </summary>
    /// <remarks>
    /// This is what admission is actually enforced against, not <see cref="ConnectedClientCount"/>: a slot
    /// is claimed as soon as a connection is accepted — before registration completes — and given back
    /// only once its handler has fully finished, so it stays ahead of <see cref="ConnectedClientCount"/>
    /// while a client is still mid-handshake or mid-teardown. A caller checking whether the hub is at
    /// capacity should compare this, not <see cref="ConnectedClientCount"/>, against <see cref="MaxClients"/>.
    /// The value is a point-in-time snapshot; clients may connect or disconnect concurrently.
    /// </remarks>
    int ClaimedClientSlots { get; }

    /// <summary>
    /// Determines whether a client with the specified identifier is currently registered.
    /// </summary>
    /// <param name="clientId">The unique identifier of the client to look up.</param>
    /// <returns><see langword="true"/> if the client is registered; otherwise, <see langword="false"/>.</returns>
    bool IsClientRegistered(Guid clientId);

    /// <summary>
    /// Gets this hub's own identifier on a peer link.
    /// </summary>
    Guid HubId { get; }

    /// <summary>
    /// Gets the number of peer hubs currently linked, in either direction.
    /// </summary>
    /// <remarks>The value is a point-in-time snapshot; peer links may connect or drop concurrently.</remarks>
    int LinkedPeerCount { get; }

    /// <summary>
    /// Links this hub to a peer hub over an already-connected transport, so a client on either hub can
    /// address a client, group or topic that exists only on the other transparently.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The hub takes ownership of <paramref name="transport"/>, exactly as
    /// <see cref="IMeshClient.ConnectAsync"/> does — it is disposed when the link ends, and the caller
    /// must not use or dispose it afterwards. Establishing the connection itself (dialling the peer's
    /// listener) is the caller's responsibility; this method performs only the peer handshake and the
    /// link's ongoing lifecycle from that point on.
    /// </para>
    /// <para>
    /// A link is single-hop: a route this hub learns from one peer is never re-advertised to another, and
    /// a message forwarded across one peer link is never forwarded again across a second. Reaching a
    /// client on a hub this one is not directly linked to requires a direct link to that hub too — there
    /// is no transitive routing through an intermediate peer. This is what makes the topology loop-free
    /// by construction rather than by a hop-count budget alone.
    /// </para>
    /// <para>
    /// Returns once the initial handshake and the first route exchange have completed; the link then
    /// runs on its own background tasks for the rest of its life, exactly like a client connection.
    /// </para>
    /// </remarks>
    /// <param name="transport">A connected transport to use for the peer link. Ownership is transferred to the hub.</param>
    /// <param name="credential">
    /// An opaque credential presented to the peer's <c>peerAuthenticator</c>, if it configured one.
    /// Empty by default.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The hub is not running.</exception>
    Task LinkPeerAsync(
        ITransport transport, ReadOnlyMemory<byte> credential = default, CancellationToken cancellationToken = default);
}
