namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The well-known <see cref="MessageHeaders"/> keys used to carry an opt-in delivery acknowledgement
/// requested via <see cref="DeliveryOptions.RequireAck"/>.
/// </summary>
/// <remarks>
/// The original message carries <see cref="CorrelationId"/> and <see cref="Request"/>; the
/// acknowledgement frame the recipient's client sends back carries <see cref="CorrelationId"/> and
/// <see cref="Ack"/>. The hub never inspects any of them — an acknowledgement is just an ordinary
/// routed message the two clients exchange between themselves.
/// </remarks>
internal static class DeliveryAcknowledgementHeaderKeys
{
    /// <summary>
    /// The header key whose value is the sending client's own acknowledgement correlation id,
    /// formatted as an invariant-culture integer.
    /// </summary>
    internal const string CorrelationId = "mesh.ack-id";

    /// <summary>
    /// The header key present, with value <c>"1"</c>, on the original message when the sender asked for
    /// a delivery acknowledgement.
    /// </summary>
    internal const string Request = "mesh.ack-request";

    /// <summary>
    /// The header key present, with value <c>"1"</c>, only on the acknowledgement frame itself — its
    /// absence is what distinguishes an incoming message that happens to carry
    /// <see cref="CorrelationId"/> from the acknowledgement answering it.
    /// </summary>
    internal const string Ack = "mesh.ack";
}
