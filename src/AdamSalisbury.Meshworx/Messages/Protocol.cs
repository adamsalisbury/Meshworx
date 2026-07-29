namespace AdamSalisbury.Meshworx.Messages;

internal static class Protocol
{
    /// <summary>
    /// The lowest wire-protocol version this build of the hub and client will negotiate down to.
    /// </summary>
    internal const byte MinSupportedVersion = 4;

    /// <summary>
    /// The highest wire-protocol version this build of the hub and client supports, and the version
    /// advertised when there is no reason to negotiate down.
    /// </summary>
    internal const byte MaxSupportedVersion = 6;

    /// <summary>
    /// The lowest negotiated protocol version at which the structured message-header envelope
    /// (<see cref="MessageHeaders"/>) may be used. A connection negotiated below this version
    /// understands only the plain, header-less message frames.
    /// </summary>
    internal const byte HeaderEnvelopeMinVersion = 5;

    /// <summary>
    /// The lowest negotiated protocol version at which a client may reclaim a previous session — keeping
    /// its assigned id and group memberships across a reconnect. A connection negotiated below this
    /// version is never issued a resumption token and its <see cref="MessageType.RegistrationComplete"/>
    /// reply carries none, so the frame stays byte-identical to what earlier versions produced.
    /// </summary>
    internal const byte SessionResumptionMinVersion = 6;

    /// <summary>
    /// The length, in bytes, of a session resumption token. 32 bytes of cryptographically secure
    /// randomness — the token is a bearer credential for an identity, so it has to be long enough that
    /// guessing one is not a realistic attack even against a hub that will happily be asked repeatedly.
    /// </summary>
    internal const int SessionTokenLength = 32;

    internal const int MaxClientNameLength = 256;
}
