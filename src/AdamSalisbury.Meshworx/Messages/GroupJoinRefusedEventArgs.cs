namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Provides data for the <see cref="IMeshClient.GroupJoinRefused"/> event.
/// </summary>
public sealed class GroupJoinRefusedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the name of the group the client was refused membership of.
    /// </summary>
    public required string GroupName { get; init; }
}
