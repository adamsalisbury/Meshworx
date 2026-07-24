namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Provides data for the <see cref="IMeshClient.Disconnected"/> event.
/// </summary>
public sealed class DisconnectedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the reason the connection to the hub ended.
    /// </summary>
    public required DisconnectReason Reason { get; init; }

    /// <summary>
    /// Gets the names of the groups the client was a member of at the moment the connection ended.
    /// </summary>
    /// <remarks>
    /// The client clears its group membership as it resets to a disconnected state, so the collection
    /// is captured beforehand and reported here. It lets a handler restore membership after reconnecting
    /// — <see cref="MeshClientReconnector"/> uses it to re-join groups automatically. Empty when the
    /// client had not joined any groups.
    /// </remarks>
    public IReadOnlyCollection<string> JoinedGroups { get; init; } = [];
}
