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
    /// Gets a value indicating whether the client is currently connected to a hub.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Gets the names of the groups this client has joined.
    /// </summary>
    /// <remarks>
    /// The collection reflects the <see cref="JoinGroupAsync"/> and <see cref="LeaveGroupAsync"/> calls
    /// made on the current connection and is cleared when the client disconnects. It is a snapshot taken
    /// when the property is read.
    /// </remarks>
    IReadOnlyCollection<string> JoinedGroups { get; }

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
    /// <param name="credential">
    /// An opaque credential presented to the hub's authenticator, if it configured one. Empty by
    /// default; the hub does not require it unless it was constructed with an authenticator.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is already connected.</exception>
    Task ConnectAsync(
        ITransport transport,
        string clientName,
        ReadOnlyMemory<byte> credential = default,
        CancellationToken cancellationToken = default);

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
    /// Broadcasts a message to every other client currently registered with the hub.
    /// </summary>
    /// <remarks>
    /// Delivery is best-effort and fire-and-forget, mirroring <see cref="SendAsync"/>. The message is
    /// not echoed back to the sender. Recipients receive it through <see cref="MessageReceived"/> and
    /// cannot distinguish it from a directly addressed message.
    /// </remarks>
    /// <param name="message">The message payload to broadcast.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task BroadcastAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Joins the named group, so that messages sent to the group are delivered to this client.
    /// </summary>
    /// <remarks>
    /// Groups are created implicitly on first join and removed once empty. Joining a group the client
    /// is already a member of has no effect.
    /// </remarks>
    /// <param name="groupName">The name of the group to join.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task JoinGroupAsync(string groupName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Leaves the named group, so that the client no longer receives messages sent to it.
    /// </summary>
    /// <remarks>
    /// Leaving a group the client is not a member of has no effect.
    /// </remarks>
    /// <param name="groupName">The name of the group to leave.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task LeaveGroupAsync(string groupName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message to every other member of the named group.
    /// </summary>
    /// <remarks>
    /// Delivery is best-effort and fire-and-forget. The message is not echoed back to the sender, and
    /// the sender need not be a member of the group. Recipients receive it through
    /// <see cref="GroupMessageReceived"/>, which carries the group name.
    /// </remarks>
    /// <param name="groupName">The name of the group to send to.</param>
    /// <param name="message">The message payload to deliver.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task SendToGroupAsync(string groupName, ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Queries the hub for a client's Id based on its name
    /// </summary>
    /// <param name="name">The name of the client for which the Id should be retrieved.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>Guid? value representing the client's Id, or null if no client by that name is found.</returns>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task<Guid?> GetClientIdByNameAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Raised when a directly addressed or broadcast message is received from another client.
    /// </summary>
    event EventHandler<MessageReceivedEventArgs> MessageReceived;

    /// <summary>
    /// Raised when a message sent to a group this client is a member of is received. Unlike
    /// <see cref="MessageReceived"/>, the event carries the name of the group the message was sent to.
    /// </summary>
    event EventHandler<GroupMessageReceivedEventArgs> GroupMessageReceived;

    /// <summary>
    /// Raised when the connection to the hub ends for a reason other than a local call to
    /// <see cref="DisconnectAsync"/> — that is, when the hub closes the connection or the
    /// underlying transport fails. It is not raised for application-initiated disconnects.
    /// </summary>
    /// <remarks>
    /// When this event fires the client has already reset to a disconnected state, so the
    /// handler may immediately attempt to reconnect via <see cref="ConnectAsync"/>.
    /// </remarks>
    event EventHandler<DisconnectedEventArgs> Disconnected;
}
