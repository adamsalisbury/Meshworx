namespace AdamSalisbury.Meshworx;

/// <summary>
/// Identifies the reason a client registration was refused by the hub.
/// </summary>
public enum RegistrationErrorCode : byte
{
    /// <summary>
    /// A client with the same name is already registered on the hub.
    /// </summary>
    DuplicateClientName = 0x01,

    /// <summary>
    /// The client's protocol version is not supported by the hub.
    /// </summary>
    UnsupportedProtocolVersion = 0x02,

    /// <summary>
    /// The client name exceeds the maximum allowed length.
    /// </summary>
    ClientNameTooLong = 0x03,

    /// <summary>
    /// The hub has reached its maximum number of registered clients and cannot accept more.
    /// </summary>
    HubAtCapacity = 0x04,

    /// <summary>
    /// The hub's authenticator rejected the client's credential.
    /// </summary>
    AuthenticationFailed = 0x05,
}
