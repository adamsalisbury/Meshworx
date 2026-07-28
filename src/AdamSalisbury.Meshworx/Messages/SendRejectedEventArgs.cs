namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Provides data for the <see cref="IMeshClient.SendRejected"/> event.
/// </summary>
public sealed class SendRejectedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the unique identifier of the recipient whose outbound queue was full, causing the drop.
    /// </summary>
    public required Guid RecipientId { get; init; }
}
