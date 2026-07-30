namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Provides data for the <see cref="IMeshClient.PresenceChanged"/> event.
/// </summary>
public sealed class PresenceChangedEventArgs : EventArgs
{
    /// <summary>
    /// Gets the unique identifier of the client whose presence changed.
    /// </summary>
    public required Guid ClientId { get; init; }

    /// <summary>
    /// Gets the name of the client whose presence changed.
    /// </summary>
    public required string ClientName { get; init; }

    /// <summary>
    /// Gets whether the client joined or left.
    /// </summary>
    public required PresenceChangeType ChangeType { get; init; }
}
