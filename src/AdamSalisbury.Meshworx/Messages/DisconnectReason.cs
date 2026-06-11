namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// Identifies why a client's connection to the hub ended.
/// </summary>
public enum DisconnectReason
{
    /// <summary>
    /// The hub gracefully closed the connection by sending a disconnect notification.
    /// </summary>
    RemoteDisconnect,

    /// <summary>
    /// The connection was lost unexpectedly — the underlying transport closed or failed.
    /// </summary>
    ConnectionLost,
}
