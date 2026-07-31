namespace AdamSalisbury.Meshworx.Admin;

/// <summary>
/// The optional JSON request body for <c>POST /clients/{id}/disconnect</c>.
/// </summary>
/// <param name="Reason">
/// An optional, opaque reason recorded for observability. See <see cref="IMeshHub.DisconnectClient"/>'s
/// own remarks on <c>reason</c> for what happens to it.
/// </param>
internal sealed record DisconnectClientRequest(string? Reason);
