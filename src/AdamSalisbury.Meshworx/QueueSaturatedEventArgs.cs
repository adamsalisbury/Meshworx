namespace AdamSalisbury.Meshworx;

/// <summary>
/// Provides data for the <see cref="IMeshHub.QueueSaturated"/> event.
/// </summary>
public sealed class QueueSaturatedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the unique identifier of the client whose message was dropped.
    /// </summary>
    public required Guid SenderId { get; init; }

    /// <summary>
    /// Gets the unique identifier of the client whose outbound queue was full, causing the drop.
    /// </summary>
    public required Guid RecipientId { get; init; }
}
