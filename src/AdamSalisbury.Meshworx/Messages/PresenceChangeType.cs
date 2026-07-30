namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Whether a <see cref="PresenceChangedEventArgs"/> reports a client joining or leaving the hub.
/// </summary>
public enum PresenceChangeType : byte
{
    /// <summary>The client became reachable on the hub.</summary>
    Joined = 0x01,

    /// <summary>The client is no longer reachable on the hub.</summary>
    Left = 0x02,
}
