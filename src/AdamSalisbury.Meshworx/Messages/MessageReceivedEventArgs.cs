namespace AdamSalisbury.Meshworx.Messages;

public sealed class MessageReceivedEventArgs : EventArgs
{
    public required Guid SenderId { get; init; }
    public required ReadOnlyMemory<byte> Data { get; init; }

    /// <summary>
    /// Gets the structured headers that travelled alongside <see cref="Data"/>, or
    /// <see cref="MessageHeaders.Empty"/> if the sender attached none.
    /// </summary>
    public MessageHeaders Headers { get; init; } = MessageHeaders.Empty;
}
