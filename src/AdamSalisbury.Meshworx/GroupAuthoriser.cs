namespace AdamSalisbury.Meshworx;

/// <summary>
/// Decides whether an already-registered client may join a group. Supplied to <see cref="MeshHub"/> so an
/// integrator can make groups an access-control boundary without modifying the library.
/// </summary>
/// <param name="context">The pending join's client identity and target group name.</param>
/// <param name="cancellationToken">
/// A token that is cancelled if the client disconnects or the hub is shutting down.
/// </param>
/// <returns>
/// <see langword="true"/> to admit the client to the group; <see langword="false"/> to refuse the join.
/// </returns>
/// <remarks>
/// This is the authorisation half of the seam whose authentication half is
/// <see cref="ClientAuthenticator"/>: the authenticator establishes who a peer is, and this decides what
/// that peer may do. <see cref="GroupJoinContext.ClientName"/> is therefore only as trustworthy as the
/// authenticator that admitted it — with no authenticator configured the hub admits any peer under any
/// unused name, so authorise on <see cref="GroupJoinContext.ClientId"/> and a name you have actually
/// authenticated rather than on a self-asserted one.
/// <para>
/// The callback is invoked once per join request, including the re-joins a client issues after
/// reconnecting, so a decision is never carried across a connection. It is invoked from the calling
/// client's own receive loop, which processes nothing else from that client until it returns: a slow
/// callback stalls only the client that asked. A callback that throws, cancels, or does not return within
/// the hub's <c>groupAuthorisationTimeout</c> refuses the join — the decision fails closed.
/// </para>
/// </remarks>
public delegate ValueTask<bool> GroupAuthoriser(
    GroupJoinContext context,
    CancellationToken cancellationToken);
