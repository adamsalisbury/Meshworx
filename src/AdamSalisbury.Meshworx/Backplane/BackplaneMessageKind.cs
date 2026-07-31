namespace AdamSalisbury.Meshworx.Backplane;

/// <summary>
/// What a <see cref="BackplaneMessage"/> addresses — the same three shapes direct sends, group sends
/// and topic publishes already have within a single hub.
/// </summary>
public enum BackplaneMessageKind : byte
{
    /// <summary>Addressed to a single client id (<see cref="BackplaneMessage.RecipientId"/>).</summary>
    Direct = 0,

    /// <summary>Addressed to a group's members (<see cref="BackplaneMessage.GroupName"/>).</summary>
    Group = 1,

    /// <summary>Addressed to a topic's subscribers (<see cref="BackplaneMessage.Topic"/>).</summary>
    Topic = 2,
}
