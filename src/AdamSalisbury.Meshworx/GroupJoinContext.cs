namespace AdamSalisbury.Meshworx;

/// <summary>
/// The details of a pending group join, presented to a <see cref="GroupAuthoriser"/> so it can decide
/// whether the client may become a member.
/// </summary>
public sealed record GroupJoinContext
{
    /// <summary>
    /// The hub-assigned identifier of the client requesting the join.
    /// </summary>
    public required Guid ClientId { get; init; }

    /// <summary>
    /// The name the client registered under.
    /// </summary>
    /// <remarks>
    /// This is the name that passed the hub's <see cref="ClientAuthenticator"/>, so it identifies the
    /// client exactly as strongly as that authenticator does. When the hub has no authenticator the name
    /// is self-asserted and must not be treated as an identity.
    /// </remarks>
    public required string ClientName { get; init; }

    /// <summary>
    /// The name of the group the client is asking to join. Supplied by the client, so it is untrusted
    /// input: match it against known groups rather than parsing meaning out of it.
    /// </summary>
    public required string GroupName { get; init; }
}
