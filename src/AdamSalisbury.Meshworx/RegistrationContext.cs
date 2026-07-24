namespace AdamSalisbury.Meshworx;

/// <summary>
/// The details of a pending client registration, presented to a <see cref="ClientAuthenticator"/> so
/// it can decide whether to admit the client.
/// </summary>
public sealed record RegistrationContext
{
    /// <summary>
    /// The name the client is registering under.
    /// </summary>
    public required string ClientName { get; init; }

    /// <summary>
    /// The opaque credential the client supplied in its registration request. Empty when the client
    /// sent no credential. The library does not interpret these bytes.
    /// </summary>
    public required ReadOnlyMemory<byte> Credential { get; init; }
}
