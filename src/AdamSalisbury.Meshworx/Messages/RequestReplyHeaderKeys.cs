namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The well-known <see cref="MessageHeaders"/> keys used by <see cref="IMeshClient.RequestAsync"/> and
/// <see cref="IMeshClient.ReplyAsync"/> to correlate a request with its reply.
/// </summary>
/// <remarks>
/// Both a request and its reply carry <see cref="CorrelationId"/>; only the reply also carries
/// <see cref="Reply"/>. The hub never inspects either key — they are ordinary header entries that ride
/// alongside the opaque body, resolved entirely by the two clients involved.
/// </remarks>
internal static class RequestReplyHeaderKeys
{
    /// <summary>
    /// The header key whose value is the sending client's own request correlation id, formatted as an
    /// invariant-culture integer.
    /// </summary>
    internal const string CorrelationId = "mesh.request-id";

    /// <summary>
    /// The header key present, with value <c>"1"</c>, only on the reply frame — its absence is what
    /// distinguishes an incoming request from an incoming reply that both carry
    /// <see cref="CorrelationId"/>.
    /// </summary>
    internal const string Reply = "mesh.reply";
}
