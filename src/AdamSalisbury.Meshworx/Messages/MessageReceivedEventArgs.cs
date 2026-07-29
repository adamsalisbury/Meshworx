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

    /// <summary>
    /// Gets the correlation id if this message is a request sent via
    /// <see cref="IMeshClient.RequestAsync"/> awaiting a reply, or <see langword="null"/> if it is an
    /// ordinary message.
    /// </summary>
    /// <remarks>
    /// A handler that finds this set should answer the request with
    /// <see cref="IMeshClient.ReplyAsync"/>, passing this event's arguments back in. Replies themselves
    /// are resolved internally by the receive loop and never raise <see cref="IMeshClient.MessageReceived"/>.
    /// </remarks>
    public long? CorrelationId { get; init; }
}
