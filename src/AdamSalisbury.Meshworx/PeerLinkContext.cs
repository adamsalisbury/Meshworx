namespace AdamSalisbury.Meshworx;

/// <summary>
/// The details of a pending incoming peer link, presented to a <see cref="PeerAuthenticator"/> so it
/// can decide whether to admit the peer.
/// </summary>
public sealed record PeerLinkContext
{
    /// <summary>
    /// The connecting hub's own identifier, as declared in its <c>PeerHello</c> frame. Self-asserted —
    /// nothing about the wire handshake proves a peer is who it claims, so an authenticator that needs a
    /// stronger guarantee must derive one from <see cref="Credential"/> or the transport itself (mutual
    /// TLS, for instance).
    /// </summary>
    public required Guid PeerHubId { get; init; }

    /// <summary>
    /// The opaque credential the peer supplied in its <c>PeerHello</c>. Empty when the peer sent none.
    /// The library does not interpret these bytes.
    /// </summary>
    public required ReadOnlyMemory<byte> Credential { get; init; }
}
