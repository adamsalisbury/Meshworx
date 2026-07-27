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
    /// Gets the wire-protocol version negotiated with the hub during the last successful
    /// <see cref="ConnectAsync"/>, or <c>0</c> if the client is not connected.
    /// </summary>
    /// <remarks>
    /// The hub selects the highest version common to its own supported range and the range this
    /// client advertised, so a newer client connecting to an older hub negotiates down to whatever
    /// that hub supports rather than being refused outright. Only the shared feature set of the
    /// negotiated version is guaranteed to work.
    /// </remarks>
    byte NegotiatedProtocolVersion { get; }

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
    /// <remarks>
    /// The disconnect is graceful and does not raise <see cref="Disconnected"/>. That holds even
    /// when the connection is lost remotely at the same moment: a teardown already in flight when
    /// this is called is claimed as application-initiated and stays silent, so the outcome does not
    /// depend on which side wins the race. The one exception is a call made after the client has
    /// already published its disconnected state, at which point the event is committed and this is
    /// simply a no-op on an unconnected client.
    /// <para>
    /// Calling this when the client is not connected is a no-op, and it is safe to call from inside
    /// a <see cref="MessageReceived"/> or <see cref="Disconnected"/> handler.
    /// </para>
    /// </remarks>
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
    /// Sends a message to another client via the hub, carrying structured headers alongside the
    /// opaque message body.
    /// </summary>
    /// <remarks>
    /// Delivery is best-effort and fire-and-forget, mirroring <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, CancellationToken)"/>.
    /// Headers are metadata the hub can route and observe without ever decoding <paramref name="message"/>
    /// itself. Passing <see cref="MessageHeaders.Empty"/> is equivalent to calling the overload without
    /// headers — no header block is written to the wire, so it costs nothing extra.
    /// </remarks>
    /// <param name="recipientId">The unique identifier of the target client.</param>
    /// <param name="message">The message payload to deliver.</param>
    /// <param name="headers">The structured headers to attach to the message.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    /// <exception cref="NotSupportedException">
    /// <paramref name="headers"/> is non-empty but the hub negotiated a protocol version that predates
    /// the header envelope.
    /// </exception>
    Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        MessageHeaders headers,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message to another client via the hub, optionally waiting for the recipient's client to
    /// acknowledge that the message reached its application.
    /// </summary>
    /// <remarks>
    /// With <see cref="DeliveryOptions.None"/> this is identical to
    /// <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, CancellationToken)"/> — best-effort,
    /// fire-and-forget, and the returned task completes once the hub has accepted the message. With
    /// <see cref="DeliveryOptions.RequireAck"/> the returned task instead completes once the recipient's
    /// client has raised <see cref="MessageReceived"/> for the message and sent back an acknowledgement,
    /// or fails with a <see cref="TimeoutException"/> if that does not happen in time. The
    /// acknowledgement is an ordinary routed message between the two clients; the hub does not
    /// participate in or observe it.
    /// </remarks>
    /// <param name="recipientId">The unique identifier of the target client.</param>
    /// <param name="message">The message payload to deliver.</param>
    /// <param name="options">Whether to require a delivery acknowledgement, and its timeout.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    /// <exception cref="NotSupportedException">
    /// <paramref name="options"/> requires an acknowledgement but the hub negotiated a protocol version
    /// that predates the header envelope.
    /// </exception>
    /// <exception cref="TimeoutException">
    /// <paramref name="options"/> requires an acknowledgement and none arrived within its timeout.
    /// </exception>
    Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        DeliveryOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Broadcasts a message to every other client currently registered with the hub.
    /// </summary>
    /// <remarks>
    /// Delivery is best-effort and fire-and-forget, mirroring
    /// <see cref="SendAsync(Guid, ReadOnlyMemory{byte}, CancellationToken)"/>. The message is
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
    /// <para>
    /// The join is asynchronous and optimistic: this returns once the request has been sent, and the
    /// group appears in <see cref="JoinedGroups"/> from that moment. A hub configured with a
    /// <see cref="GroupAuthoriser"/> may refuse it, in which case the group is removed from
    /// <see cref="JoinedGroups"/> again and <see cref="GroupJoinRefused"/> is raised. Applications that
    /// depend on membership should therefore watch that event rather than treat this method's return as
    /// proof of membership.
    /// </para>
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
    /// Delivery is best-effort and fire-and-forget. The message is not echoed back to the sender.
    /// Recipients receive it through <see cref="GroupMessageReceived"/>, which carries the group name.
    /// <para>
    /// <b>The sender must be a member of the group.</b> The hub silently drops a group message from a
    /// client that has not joined the group, so a send made before <see cref="JoinGroupAsync"/> has been
    /// applied — or after a join was refused — reaches nobody. After a reconnect, membership is restored
    /// by re-joining, so wait for that to complete before sending.
    /// </para>
    /// </remarks>
    /// <param name="groupName">The name of the group to send to.</param>
    /// <param name="message">The message payload to deliver.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task SendToGroupAsync(string groupName, ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message to every other member of the named group, carrying structured headers
    /// alongside the opaque message body.
    /// </summary>
    /// <remarks>
    /// Delivery is best-effort and fire-and-forget, mirroring
    /// <see cref="SendToGroupAsync(string, ReadOnlyMemory{byte}, CancellationToken)"/>. Passing
    /// <see cref="MessageHeaders.Empty"/> is equivalent to calling the overload without headers — no
    /// header block is written to the wire, so it costs nothing extra.
    /// </remarks>
    /// <param name="groupName">The name of the group to send to.</param>
    /// <param name="message">The message payload to deliver.</param>
    /// <param name="headers">The structured headers to attach to the message.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    /// <exception cref="NotSupportedException">
    /// <paramref name="headers"/> is non-empty but the hub negotiated a protocol version that predates
    /// the header envelope.
    /// </exception>
    Task SendToGroupAsync(
        string groupName,
        ReadOnlyMemory<byte> message,
        MessageHeaders headers,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Queries the hub for a client's Id based on its name
    /// </summary>
    /// <param name="name">The name of the client for which the Id should be retrieved.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>Guid? value representing the client's Id, or null if no client by that name is found.</returns>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    Task<Guid?> GetClientIdByNameAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a request to another client and awaits a correlated reply.
    /// </summary>
    /// <remarks>
    /// The correlation id that ties the reply back to this call travels as a header, so this requires a
    /// connection that negotiated a protocol version supporting the structured header envelope (see
    /// <see cref="MessageHeaders"/>). The responder observes the request through
    /// <see cref="MessageReceived"/> — a raised event whose
    /// <see cref="MessageReceivedEventArgs.CorrelationId"/> is not <see langword="null"/> is a
    /// request awaiting a reply — and answers it with <see cref="ReplyAsync"/>.
    /// <para>
    /// Concurrent requests from the same client are independent: each is tracked by its own correlation
    /// id and resolved only by a reply carrying that same id. A reply that arrives after this call has
    /// already timed out or been cancelled is discarded rather than misrouted to a later request that
    /// happens to reuse the id.
    /// </para>
    /// </remarks>
    /// <param name="recipientId">The unique identifier of the client to request a reply from.</param>
    /// <param name="message">The request payload.</param>
    /// <param name="timeout">The maximum time to wait for a reply before the call fails.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The reply payload sent back via <see cref="ReplyAsync"/>.</returns>
    /// <exception cref="InvalidOperationException">The client is not connected to a hub.</exception>
    /// <exception cref="NotSupportedException">
    /// The hub negotiated a protocol version that predates the header envelope.
    /// </exception>
    /// <exception cref="TimeoutException">No reply arrived within <paramref name="timeout"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is not positive.</exception>
    Task<ReadOnlyMemory<byte>> RequestAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Replies to a request previously received via <see cref="MessageReceived"/>, completing the
    /// sender's pending <see cref="RequestAsync"/> call.
    /// </summary>
    /// <param name="request">
    /// The <see cref="MessageReceivedEventArgs"/> the request arrived on. Its
    /// <see cref="MessageReceivedEventArgs.CorrelationId"/> must be set, i.e. it must have come
    /// from a <see cref="RequestAsync"/> call rather than an ordinary send.
    /// </param>
    /// <param name="message">The reply payload.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">
    /// The client is not connected to a hub, or <paramref name="request"/> was not a request.
    /// </exception>
    Task ReplyAsync(
        MessageReceivedEventArgs request,
        ReadOnlyMemory<byte> message,
        CancellationToken cancellationToken = default);

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
    /// Raised when the hub refuses this client membership of a group it asked to join, because the hub's
    /// <see cref="GroupAuthoriser"/> did not authorise it.
    /// </summary>
    /// <remarks>
    /// The group has already been removed from <see cref="JoinedGroups"/> by the time this fires. The
    /// refusal is not retried — including by <see cref="MeshClientReconnector"/>, which drops a refused
    /// group from the membership it restores — so a handler that wants to try again must ask again
    /// itself, having first dealt with whatever made the hub refuse.
    /// </remarks>
    event EventHandler<GroupJoinRefusedEventArgs> GroupJoinRefused;

    /// <summary>
    /// Raised when the connection to the hub ends for a reason other than a local call to
    /// <see cref="DisconnectAsync"/> — that is, when the hub closes the connection or the
    /// underlying transport fails. It is not raised for application-initiated disconnects.
    /// </summary>
    /// <remarks>
    /// When this event fires the client has already reset to a disconnected state, so the
    /// handler may immediately attempt to reconnect via <see cref="ConnectAsync"/>.
    /// <para>
    /// A remote drop that coincides with a local <see cref="DisconnectAsync"/> does not raise it:
    /// the disconnect the application asked for wins. The exception is the one noted on
    /// <see cref="DisconnectAsync"/> — a call arriving after the client has published its
    /// disconnected state is too late, because the decision to raise has already been taken.
    /// </para>
    /// </remarks>
    event EventHandler<DisconnectedEventArgs> Disconnected;
}
