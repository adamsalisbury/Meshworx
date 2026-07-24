namespace AdamSalisbury.Meshworx.Transport;

/// <summary>
/// An optional capability a transport implements when it can frame several messages into a single
/// underlying write. The hub's send loop uses it to coalesce a burst of already-queued frames into
/// one syscall; transports that do not implement it simply receive the frames one at a time.
/// </summary>
/// <remarks>
/// This is internal on purpose: only the bundled stream-framing transport benefits from coalescing,
/// and only the in-assembly hub consumes it, so it stays off the public <see cref="ITransport"/>
/// contract and out of the way of external implementers and their test doubles.
/// </remarks>
internal interface IBatchSendTransport
{
    /// <summary>
    /// Sends several complete messages to the remote endpoint in order, coalescing them into a single
    /// underlying write. Each element is delivered as its own message, exactly as a sequence of
    /// <see cref="ITransport.SendAsync(System.ReadOnlyMemory{byte}, System.Threading.CancellationToken)"/>
    /// calls would be.
    /// </summary>
    /// <param name="messages">The message payloads to send, in delivery order.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task SendAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> messages,
        CancellationToken cancellationToken = default);
}
