namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The well-known <see cref="MessageHeaders"/> key used to ask the hub to keep a group or topic send as
/// the last-value message replayed to whoever joins the group, or subscribes to the topic, next.
/// </summary>
/// <remarks>
/// The flag travels as an ordinary header, exactly like <see cref="BackpressureHeaderKeys.AwaitCapacity"/>
/// and <see cref="MessagePriorityHeaderKeys.Priority"/> — the hub never inspects the message body to
/// decide this. Sending an empty body with this header set clears whatever was previously retained,
/// mirroring the last-value-message semantics of MQTT's own retained-message flag.
/// </remarks>
internal static class RetainHeaderKeys
{
    /// <summary>
    /// The header key present, with value <c>"1"</c>, when the sender asked the hub to retain this send
    /// as the group's or topic's last-value message.
    /// </summary>
    internal const string Retain = "mesh.retain";
}
