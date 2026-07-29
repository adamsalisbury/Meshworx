namespace AdamSalisbury.Meshworx;

/// <summary>
/// One direct message held for a client that was not connected when it was sent, in the shape the hub
/// stores it: the routing metadata and the opaque bytes, never a built delivery frame.
/// </summary>
/// <param name="SenderId">The id of the client that sent the message.</param>
/// <param name="HeaderBlock">
/// The sender's encoded header block, or empty for a message sent without headers. Stored encoded and
/// undecoded, exactly as it arrived — see the remarks.
/// </param>
/// <param name="Body">The opaque message body.</param>
/// <param name="QueuedAt">When the hub accepted the message for storage.</param>
/// <remarks>
/// <para>
/// The frame that eventually reaches the client is built when it reconnects, not when the message is
/// stored, because the frame's shape depends on the protocol version that <em>returning</em> connection
/// negotiates — which is unknowable at storage time. Holding the parts rather than a finished frame is
/// what lets one stored message be delivered as either
/// <see cref="Messages.MessageType.DeliverMessageWithHeaders"/> or the plain, header-stripped
/// <see cref="Messages.MessageType.DeliverMessage"/>, matching what live routing would have done.
/// </para>
/// <para>
/// The hub copies both byte ranges out of the inbound frame before constructing this record, so neither
/// aliases the larger receive buffer and an implementation may hold onto them for as long as it likes.
/// </para>
/// </remarks>
public sealed record OfflineMessage(
    Guid SenderId,
    ReadOnlyMemory<byte> HeaderBlock,
    ReadOnlyMemory<byte> Body,
    DateTimeOffset QueuedAt)
{
    /// <summary>
    /// The number of bytes this message contributes to a store's size bound: the body plus the header
    /// block, ignoring the fixed per-message overhead a particular store implementation adds.
    /// </summary>
    public int ByteCount => Body.Length + HeaderBlock.Length;
}
