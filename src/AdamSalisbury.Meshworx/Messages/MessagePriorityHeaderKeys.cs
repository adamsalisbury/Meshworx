namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The well-known <see cref="MessageHeaders"/> key used to carry a message's <see cref="MessagePriority"/>.
/// </summary>
/// <remarks>
/// The hub reads this one key, exactly as it already reads <see cref="MessageExpiryHeaderKeys"/> and
/// <see cref="BackpressureHeaderKeys"/>, to decide which outbound lane to queue a frame on — it never
/// decodes anything else in the header block, and never touches the opaque body.
/// </remarks>
internal static class MessagePriorityHeaderKeys
{
    /// <summary>
    /// The header key whose value is the message's <see cref="MessagePriority"/>, encoded via
    /// <see cref="ToHeaderValue"/>.
    /// </summary>
    internal const string Priority = "mesh.priority";

    private const string HighValue = "high";
    private const string LowValue = "low";
    private const string NormalValue = "normal";

    /// <summary>
    /// Encodes a <see cref="MessagePriority"/> as the header value <see cref="Priority"/> carries.
    /// </summary>
    internal static string ToHeaderValue(MessagePriority priority)
    {
        return priority switch
        {
            MessagePriority.High => HighValue,
            MessagePriority.Low => LowValue,
            _ => NormalValue,
        };
    }

    /// <summary>
    /// Parses <see cref="Priority"/>'s value into a <see cref="MessagePriority"/>, shared by the hub's
    /// lane-selection check.
    /// </summary>
    /// <remarks>
    /// Anything that is not exactly one of the recognised values — absent, malformed, or simply
    /// unexpected — resolves to <see cref="MessagePriority.Normal"/>, the same tolerant treatment
    /// <see cref="MessageExpiryHeaderKeys.TryParseExpiry"/> gives a bad expiry value: this comes from the
    /// sender's own header block and must never throw or otherwise disrupt routing.
    /// </remarks>
    internal static MessagePriority Parse(string? value)
    {
        return value switch
        {
            HighValue => MessagePriority.High,
            LowValue => MessagePriority.Low,
            _ => MessagePriority.Normal,
        };
    }
}
