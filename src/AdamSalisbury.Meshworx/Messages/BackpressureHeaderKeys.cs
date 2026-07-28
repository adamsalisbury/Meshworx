namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The well-known <see cref="MessageHeaders"/> key used to opt a single direct send into awaiting
/// capacity on the recipient's outbound queue rather than being dropped immediately if that queue is
/// full when the hub routes it.
/// </summary>
/// <remarks>
/// Requested via <see cref="DeliveryOptions.AwaitCapacity"/>. The hub never inspects the message body to
/// decide this — the flag travels as an ordinary header, exactly like <see cref="MessageExpiryHeaderKeys"/>
/// and <see cref="DeliveryAcknowledgementHeaderKeys"/> — and only <c>RouteMessageWithHeaders</c> (a
/// direct send to a single recipient) honours it; a broadcast or group send never blocks the whole
/// fan-out on one slow member.
/// </remarks>
internal static class BackpressureHeaderKeys
{
    /// <summary>
    /// The header key present, with value <c>"1"</c>, when the sender asked the hub to await capacity
    /// on the recipient's outbound queue instead of dropping the message immediately.
    /// </summary>
    internal const string AwaitCapacity = "mesh.await-capacity";
}
