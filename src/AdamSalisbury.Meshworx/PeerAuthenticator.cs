namespace AdamSalisbury.Meshworx;

/// <summary>
/// Decides whether an incoming connection claiming to be a peer hub may be admitted as one. Supplied to
/// <see cref="MeshHub"/> so an integrator can restrict which other hubs may federate with this one.
/// </summary>
/// <param name="context">The pending peer link's declared hub identifier and credential.</param>
/// <param name="cancellationToken">A token that is cancelled if the hub is shutting down.</param>
/// <returns>
/// <see langword="true"/> to admit the peer; <see langword="false"/> to refuse the link.
/// </returns>
/// <remarks>
/// Only consulted for an <em>incoming</em> connection — one this hub's own listener accepted and that
/// then sent <c>PeerHello</c> instead of a client registration. A link this hub establishes itself via
/// <see cref="MeshHub.LinkPeerAsync"/> is never subject to this callback, since admitting it was already
/// this hub's own decision. When <see langword="null"/> (the default), any peer is admitted once
/// <c>allowIncomingPeerLinks</c> is set — see that parameter's remarks for why incoming peer links are
/// refused outright unless a hub opts in, independently of whether an authenticator is configured.
/// </remarks>
public delegate ValueTask<bool> PeerAuthenticator(
    PeerLinkContext context,
    CancellationToken cancellationToken);
