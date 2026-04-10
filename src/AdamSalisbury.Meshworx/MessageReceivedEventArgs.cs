namespace AdamSalisbury.Meshworx;

public sealed class MessageReceivedEventArgs : EventArgs
{
    public required Guid SenderId { get; init; }
    public required ReadOnlyMemory<byte> Data { get; init; }
}
