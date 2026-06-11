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
}
