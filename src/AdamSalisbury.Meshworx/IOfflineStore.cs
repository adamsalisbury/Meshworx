namespace AdamSalisbury.Meshworx;

/// <summary>
/// Holds messages addressed to a client that was not connected at the time, keyed by that client's
/// name, until it next registers. Supplying one to <see cref="MeshHub"/> is what turns store-and-forward
/// on; leaving it <see langword="null"/> — the default — leaves the hub's original drop-on-unknown-recipient
/// behaviour completely untouched.
/// </summary>
/// <remarks>
/// <para>
/// Keyed by <em>name</em> rather than by connection id deliberately: the id is minted per connection, so
/// it is the name that survives a client going away and coming back. The hub resolves the id a sender
/// addressed back to the name that last held it before calling
/// <see cref="TryEnqueueAsync"/> — see the hub documentation for how long that association is retained.
/// </para>
/// <para>
/// The default <see cref="InMemoryOfflineStore"/> is bounded and process-local. Implement this interface
/// to back the queue with something durable instead; the hub never inspects a stored message's bytes, so
/// an implementation is free to serialise them however it likes.
/// </para>
/// <para>
/// <strong>Implementations must be thread-safe.</strong> Both methods are called from per-connection
/// handler tasks that run concurrently with one another, and a store for one name can be enqueued to by
/// many senders at once while its owner is reconnecting.
/// </para>
/// </remarks>
public interface IOfflineStore
{
    /// <summary>
    /// Offers a message for storage against a client name.
    /// </summary>
    /// <param name="clientName">The name of the disconnected client the message was addressed to.</param>
    /// <param name="message">The message to hold.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// <see langword="true"/> if the message was stored; <see langword="false"/> if it was refused —
    /// because a bound was reached, or because this store declines to hold anything for that name. A
    /// refusal makes the hub drop the message exactly as if store-and-forward were switched off.
    /// </returns>
    /// <remarks>
    /// An implementation that evicts to make room (rather than refusing) should still return
    /// <see langword="true"/>: the return value describes whether <em>this</em> message was accepted, not
    /// whether anything was displaced to accept it.
    /// </remarks>
    ValueTask<bool> TryEnqueueAsync(
        string clientName, OfflineMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes and returns everything held for a client name, oldest first.
    /// </summary>
    /// <param name="clientName">The name of the client that has just registered.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    /// The stored messages in the order they were accepted, or an empty list if there are none. The
    /// messages are removed from the store by this call, whether or not the caller manages to deliver
    /// them.
    /// </returns>
    /// <remarks>
    /// Called once per successful registration, so an implementation should treat a name it holds
    /// nothing for as the common case and return an empty result cheaply. Anything already past its
    /// retention window should be discarded here rather than returned.
    /// </remarks>
    ValueTask<IReadOnlyList<OfflineMessage>> TakeAllAsync(
        string clientName, CancellationToken cancellationToken = default);
}
