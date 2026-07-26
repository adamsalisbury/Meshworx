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
    internal const byte MaxSupportedVersion = 4;

    internal const int MaxClientNameLength = 256;
}
