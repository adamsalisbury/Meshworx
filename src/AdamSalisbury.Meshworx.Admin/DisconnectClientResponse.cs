namespace AdamSalisbury.Meshworx.Admin;

/// <summary>
/// The JSON response body for <c>POST /clients/{id}/disconnect</c>.
/// </summary>
/// <param name="Disconnected">
/// Mirrors <see cref="IMeshHub.DisconnectClient"/>'s own return value: <see langword="true"/> if a client
/// with that identifier was connected and disconnection was requested; <see langword="false"/> if no such
/// client was connected, in which case the response also carries a <c>404</c> status.
/// </param>
internal sealed record DisconnectClientResponse(bool Disconnected);
