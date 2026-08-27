namespace AdamSalisbury.Meshworx.Admin;

/// <summary>
/// The pending administrative HTTP request an <see cref="AdminRequestAuthenticator"/> is asked to admit
/// or refuse.
/// </summary>
/// <param name="Method">The HTTP method, for example <c>GET</c> or <c>POST</c>.</param>
/// <param name="Path">The request's absolute path, for example <c>/clients</c>.</param>
/// <param name="AuthorizationHeaderValue">
/// The raw value of the request's <c>Authorization</c> header, or <see langword="null"/> if it carried
/// none. Never parsed or interpreted by <see cref="MeshHubAdminServer"/> itself — how it is validated,
/// and against what scheme, is entirely the authenticator's own decision.
/// </param>
public sealed record AdminRequestContext(string Method, string Path, string? AuthorizationHeaderValue);
