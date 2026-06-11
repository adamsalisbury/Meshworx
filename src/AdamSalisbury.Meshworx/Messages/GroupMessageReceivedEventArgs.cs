namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Provides data for the <see cref="IMeshClient.GroupMessageReceived"/> event.
/// </summary>
public sealed class GroupMessageReceivedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the unique identifier of the client that sent the message.
    /// </summary>
    public required Guid SenderId { get; init; }

    /// <summary>
    /// Gets the name of the group the message was sent to.
    /// </summary>
    public required string GroupName { get; init; }

    /// <summary>
    /// Gets the message payload.
    /// </summary>
    public required ReadOnlyMemory<byte> Data { get; init; }
}
