namespace AdamSalisbury.Meshworx.Interfaces;

public interface IMeshClient
{
    /// <summary>
    /// Gets the unique identifier assigned to this client by the hub during registration.
    /// </summary>
    Guid Id { get; }

    /// <summary>
    /// Connects to a hub at the specified endpoint and completes the registration handshake.
    /// </summary>
    /// <param name="host">The hostname or IP address of the hub.</param>
    /// <param name="port">The TCP port the hub is listening on.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is already connected.</exception>
    Task ConnectAsync(string host, int port, CancellationToken cancellationToken = default);

    /// <summary>
    /// Disconnects from the hub and releases all associated resources.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task DisconnectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message to another client via the hub.
    /// </summary>
    /// <param name="recipientId">The unique identifier of the target client.</param>
    /// <param name="message">The message payload to deliver.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task SendAsync(Guid recipientId, ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Raised when a message is received from another client.
    /// </summary>
    event EventHandler<MessageReceivedEventArgs> MessageReceived;
}
