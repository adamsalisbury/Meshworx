using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;

namespace AdamSalisbury.Meshworx;

public interface IMeshClient : IAsyncDisposable
{
    /// <summary>
    /// Gets the unique identifier assigned to this client by the hub during registration.
    /// </summary>
    Guid Id { get; }

    /// <summary>
    /// Gets the unique name of this client, assigned upon successful connection to a hub.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Connects to a hub via the specified transport and completes the registration handshake.
    /// </summary>
    /// <remarks>
    /// The client takes ownership of the transport. It will be disposed when the client
    /// disconnects or if the handshake fails. The caller must not use or dispose the
    /// transport after calling this method.
    /// </remarks>
    /// <param name="transport">A connected transport to use for communication with the hub. Ownership is transferred to the client.</param>
    /// <param name="clientName">The unique name of this client.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is already connected.</exception>
    Task ConnectAsync(ITransport transport, string clientName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Disconnects from the hub and releases all associated resources.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task DisconnectAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message to another client via the hub.
    /// </summary>
    /// <remarks>
    /// Delivery is best-effort and fire-and-forget. The method completes once the hub has
    /// accepted the message, but provides no guarantee that the recipient received it.
    /// </remarks>
    /// <param name="recipientId">The unique identifier of the target client.</param>
    /// <param name="message">The message payload to deliver.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task SendAsync(Guid recipientId, ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Queries the hub for a client's Id based on its name
    /// </summary>
    /// <param name="name">The name of the client for which the Id should be retrieved.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>Guid? value representing the client's Id, or null if no client by that name is found.</returns>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task<Guid?> GetClientIdByNameAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Raised when a message is received from another client.
    /// </summary>
    event EventHandler<MessageReceivedEventArgs> MessageReceived;
}
