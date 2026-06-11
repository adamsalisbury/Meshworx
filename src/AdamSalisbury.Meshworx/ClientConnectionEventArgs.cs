namespace AdamSalisbury.Meshworx;

/// <summary>
/// Provides data for the <see cref="IMeshHub.ClientConnected"/> and
/// <see cref="IMeshHub.ClientDisconnected"/> events.
/// </summary>
public sealed class ClientConnectionEventArgs : EventArgs
{
    /// <summary>
    /// Gets the unique identifier the hub assigned to the client.
    /// </summary>
    public required Guid ClientId { get; init; }

    /// <summary>
    /// Gets the unique name the client registered with.
    /// </summary>
    public required string ClientName { get; init; }
}
