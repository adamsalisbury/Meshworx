namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Provides data for the <see cref="IMeshClient.TopicMessageReceived"/> event.
/// </summary>
public sealed class TopicMessageReceivedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the unique identifier of the client that published the message.
    /// </summary>
    public required Guid SenderId { get; init; }

    /// <summary>
    /// Gets the concrete topic the message was published to, as the publisher wrote it — never a
    /// pattern, and not necessarily identical to the pattern this client subscribed with.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the message payload.
    /// </summary>
    public required ReadOnlyMemory<byte> Data { get; init; }

    /// <summary>
    /// Gets the structured headers that travelled alongside <see cref="Data"/>, or
    /// <see cref="MessageHeaders.Empty"/> if the publisher attached none.
    /// </summary>
    public MessageHeaders Headers { get; init; } = MessageHeaders.Empty;
}
