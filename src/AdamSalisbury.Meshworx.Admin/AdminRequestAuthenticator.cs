namespace AdamSalisbury.Meshworx.Admin;

/// <summary>
/// Decides whether an incoming administrative HTTP request may proceed. Required to construct a
/// <see cref="MeshHubAdminServer"/> — there is no unauthenticated default — so an integrator secures the
/// admin surface however their deployment already authenticates operators: a static API key, mutual TLS
/// terminated by a reverse proxy in front of this listener, a bearer token validated against an identity
/// provider, and so on.
/// </summary>
/// <param name="context">The pending request's method, path, and raw <c>Authorization</c> header value.</param>
/// <param name="cancellationToken">A token cancelled if the server is shutting down.</param>
/// <returns>
/// <see langword="true"/> to admit the request; <see langword="false"/> to refuse it with a
/// <c>401 Unauthorized</c> response.
/// </returns>
/// <remarks>
/// Invoked for every request, including one that will go on to be refused as an unrecognised route — a
/// caller with no credential learns nothing about which routes exist. A callback that throws is treated
/// identically to one that returns <see langword="false"/>: the request is refused rather than the server
/// faulting the connection or leaking the exception to the caller.
/// </remarks>
public delegate ValueTask<bool> AdminRequestAuthenticator(
    AdminRequestContext context, CancellationToken cancellationToken);
