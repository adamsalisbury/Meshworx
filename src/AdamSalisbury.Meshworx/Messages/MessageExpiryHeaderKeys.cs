using System.Globalization;

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

    /// <summary>
    /// Parses <see cref="ExpiresAtUnixMilliseconds"/>'s value into an expiry instant, shared by both the
    /// hub's and the client's expiry checks so the two never drift apart on how a bad value is treated.
    /// </summary>
    /// <remarks>
    /// Anything that is not a valid, in-range Unix-millisecond value — absent, non-numeric, or numeric
    /// but too large or small for <see cref="DateTimeOffset"/> to represent — is treated as "does not
    /// expire", the same tolerant treatment already given to a header that is simply missing. The value
    /// comes from the sender's own header block and is not otherwise validated, so this must never throw:
    /// an adversarial or merely malformed value (for example <see cref="long.MaxValue"/>, which parses as
    /// a perfectly good integer but is far outside the range <see cref="DateTimeOffset.FromUnixTimeMilliseconds"/>
    /// can represent) must not be able to crash whichever loop is checking it.
    /// </remarks>
    internal static bool TryParseExpiry(string? value, out DateTimeOffset expiry)
    {
        expiry = default;

        if (value is null
            || !long.TryParse(
                value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long unixMilliseconds))
        {
            return false;
        }

        try
        {
            expiry = DateTimeOffset.FromUnixTimeMilliseconds(unixMilliseconds);
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }
    }
}
