namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The relative priority a sender may give a message, so it can overtake a backlog of lower-priority
/// traffic in a recipient's outbound queue instead of waiting behind it in strict arrival order.
/// </summary>
/// <remarks>
/// Carried as a header hint (<see cref="MessagePriorityHeaderKeys.Priority"/>) — the hub never inspects
/// the message body to decide this. <see cref="Normal"/> is the enum's default value, so a message sent
/// with no priority set resolves to exactly the queueing behaviour that existed before priority lanes
/// were introduced.
/// </remarks>
public enum MessagePriority
{
    /// <summary>
    /// The default priority: queued and delivered in the same order as before priority lanes existed,
    /// relative to other <see cref="Normal"/> traffic.
    /// </summary>
    Normal = 0,

    /// <summary>
    /// Lower than <see cref="Normal"/>. Serviced only once every high- and normal-priority burst has run
    /// its course, but never starved indefinitely — the hub's send loop guarantees it a turn every cycle.
    /// </summary>
    Low = 1,

    /// <summary>
    /// Higher than <see cref="Normal"/>. Drained ahead of both other lanes, so it can overtake a backlog
    /// of bulk traffic already queued for the same recipient.
    /// </summary>
    High = 2,
}
