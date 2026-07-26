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
    internal const byte MaxSupportedVersion = 5;

    /// <summary>
    /// The lowest negotiated protocol version at which the structured message-header envelope
    /// (<see cref="MessageHeaders"/>) may be used. A connection negotiated below this version
    /// understands only the plain, header-less message frames.
    /// </summary>
    internal const byte HeaderEnvelopeMinVersion = 5;

    internal const int MaxClientNameLength = 256;
}
