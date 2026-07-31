namespace AdamSalisbury.Meshworx.Backplane;

/// <summary>
/// A message published across a <see cref="IHubBackplane"/> so every hub instance sharing it can
/// materialise a delivery for whichever of its own locally-connected clients the message addresses.
/// </summary>
public sealed record BackplaneMessage
{
    /// <summary>
    /// The identifier of the hub instance that published this message — never a client id. Consulted by
    /// the publishing instance's own handler to recognise, and skip, a message it published itself: it
    /// has already materialised any local delivery its own send already produced, and processing its own
    /// echo again would deliver a second time to any client it holds.
    /// </summary>
    public required Guid OriginInstanceId { get; init; }

    /// <summary>What this message addresses.</summary>
    public required BackplaneMessageKind Kind { get; init; }

    /// <summary>
    /// The recipient's id, when <see cref="Kind"/> is <see cref="BackplaneMessageKind.Direct"/>.
    /// Meaningless for any other kind.
    /// </summary>
    public Guid RecipientId { get; init; }

    /// <summary>
    /// The target group's name, when <see cref="Kind"/> is <see cref="BackplaneMessageKind.Group"/>.
    /// <see langword="null"/> for any other kind.
    /// </summary>
    public string? GroupName { get; init; }

    /// <summary>
    /// The target topic, when <see cref="Kind"/> is <see cref="BackplaneMessageKind.Topic"/>.
    /// <see langword="null"/> for any other kind.
    /// </summary>
    public string? Topic { get; init; }

    /// <summary>The id of the client the message originated from.</summary>
    public required Guid SenderId { get; init; }

    /// <summary>The opaque message payload. Never inspected by the backplane itself.</summary>
    public required ReadOnlyMemory<byte> Body { get; init; }
}
