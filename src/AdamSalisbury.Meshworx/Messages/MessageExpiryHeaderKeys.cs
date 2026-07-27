namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The well-known <see cref="MessageHeaders"/> key used to carry a per-message time-to-live.
/// </summary>
/// <remarks>
/// The value is the absolute expiry instant, as Unix milliseconds measured against the
/// <b>sending client's own clock</b>, formatted as an invariant-culture integer. There is no hub clock
/// authority: the hub and the recipient both compare this value against their own local clock, so
/// meaningful use of a short time-to-live assumes the clocks involved are reasonably synchronised (for
/// example via NTP) — under material clock skew a message could expire earlier or later than the sender
/// intended, or not at all.
/// </remarks>
internal static class MessageExpiryHeaderKeys
{
    /// <summary>
    /// The header key whose value is the message's absolute expiry instant, in Unix milliseconds.
    /// </summary>
    internal const string ExpiresAtUnixMilliseconds = "mesh.expires-at";
}
