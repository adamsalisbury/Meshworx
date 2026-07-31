namespace AdamSalisbury.Meshworx;

/// <summary>
/// A point-in-time snapshot of one client currently connected to the hub, for administrative inspection
/// via <see cref="IMeshHub.GetClients"/>.
/// </summary>
/// <param name="Id">The unique identifier the hub assigned the client.</param>
/// <param name="Name">The name the client registered under.</param>
/// <param name="Groups">Every group the client is currently a member of.</param>
/// <param name="OutboundQueueDepth">The number of frames currently queued for delivery to this client.</param>
/// <param name="ConnectedAt">The moment this client's connection was registered.</param>
public sealed record ConnectedClientInfo(
    Guid Id,
    string Name,
    IReadOnlyList<string> Groups,
    int OutboundQueueDepth,
    DateTimeOffset ConnectedAt);
