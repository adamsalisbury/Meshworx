using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx.Serialization;

/// <summary>
/// Typed send extensions for <see cref="IMeshClient"/>: serialize a value with a codec, tag the body
/// with its content type, and send it as an ordinary opaque message.
/// </summary>
/// <remarks>
/// Every method here is a thin wrapper over the byte-oriented method of the same name on
/// <see cref="IMeshClient"/>. Nothing about routing, delivery, or the wire changes: the hub sees a body
/// it does not interpret and a header block it passes through, exactly as it would for a hand-rolled
/// byte array. The convenience is entirely on the two endpoints.
/// </remarks>
public static class MeshClientSerializationExtensions
{
    /// <summary>
    /// Serializes a value and sends it to a single recipient.
    /// </summary>
    /// <typeparam name="TValue">The type of the value to send.</typeparam>
    /// <param name="client">The client to send from.</param>
    /// <param name="recipientId">The id of the recipient.</param>
    /// <param name="value">The value to serialize and send.</param>
    /// <param name="serializer">The codec to serialize with.</param>
    /// <param name="headers">
    /// Additional headers to send alongside the body, or <see langword="null"/> for none. The content
    /// type is added to a copy of these; the instance passed in is never mutated.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that completes once the message has been handed to the transport.</returns>
    public static Task SendAsync<TValue>(
        this IMeshClient client,
        Guid recipientId,
        TValue value,
        IMessageSerializer serializer,
        MessageHeaders? headers = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(serializer);

        ReadOnlyMemory<byte> body = serializer.Serialize(value);
        return client.SendAsync(
            recipientId, body, WithContentType(headers, serializer.ContentType), cancellationToken);
    }

    /// <summary>
    /// Serializes a value and sends it to every member of a group.
    /// </summary>
    /// <typeparam name="TValue">The type of the value to send.</typeparam>
    /// <param name="client">The client to send from.</param>
    /// <param name="groupName">The group to send to. The sender must be a member of it.</param>
    /// <param name="value">The value to serialize and send.</param>
    /// <param name="serializer">The codec to serialize with.</param>
    /// <param name="headers">
    /// Additional headers to send alongside the body, or <see langword="null"/> for none. The content
    /// type is added to a copy of these; the instance passed in is never mutated.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that completes once the message has been handed to the transport.</returns>
    public static Task SendToGroupAsync<TValue>(
        this IMeshClient client,
        string groupName,
        TValue value,
        IMessageSerializer serializer,
        MessageHeaders? headers = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(serializer);

        ReadOnlyMemory<byte> body = serializer.Serialize(value);
        return client.SendToGroupAsync(
            groupName, body, WithContentType(headers, serializer.ContentType), cancellationToken);
    }

    /// <summary>
    /// Serializes a value and sends it as a request, awaiting a serialized reply.
    /// </summary>
    /// <typeparam name="TRequest">The type of the request value.</typeparam>
    /// <typeparam name="TReply">The type to deserialize the reply into.</typeparam>
    /// <param name="client">The client to send from.</param>
    /// <param name="recipientId">The id of the recipient.</param>
    /// <param name="value">The request value to serialize and send.</param>
    /// <param name="serializer">The codec to serialize the request and deserialize the reply with.</param>
    /// <param name="timeout">How long to wait for a reply before giving up.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The deserialized reply.</returns>
    /// <remarks>
    /// The reply is deserialized with the same codec the request was serialized with, on the assumption
    /// that a responder replies in the format it was asked in. A responder that deliberately replies in
    /// another format should be called via the byte-oriented
    /// <see cref="IMeshClient.RequestAsync"/> instead, and its reply decoded explicitly.
    /// </remarks>
    public static async Task<TReply?> RequestAsync<TRequest, TReply>(
        this IMeshClient client,
        Guid recipientId,
        TRequest value,
        IMessageSerializer serializer,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(serializer);

        ReadOnlyMemory<byte> body = serializer.Serialize(value);
        ReadOnlyMemory<byte> reply = await client
            .RequestAsync(recipientId, body, timeout, cancellationToken)
            .ConfigureAwait(false);

        return serializer.Deserialize<TReply>(reply.Span);
    }

    /// <summary>
    /// Serializes a value and sends it as the reply to a received request.
    /// </summary>
    /// <typeparam name="TValue">The type of the reply value.</typeparam>
    /// <param name="client">The client to reply from.</param>
    /// <param name="request">The received request being replied to.</param>
    /// <param name="value">The value to serialize and send as the reply.</param>
    /// <param name="serializer">The codec to serialize with.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that completes once the reply has been handed to the transport.</returns>
    public static Task ReplyAsync<TValue>(
        this IMeshClient client,
        MessageReceivedEventArgs request,
        TValue value,
        IMessageSerializer serializer,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(serializer);

        ReadOnlyMemory<byte> body = serializer.Serialize(value);
        return client.ReplyAsync(request, body, cancellationToken);
    }

    /// <summary>
    /// Returns the supplied headers with the content type added, without mutating the original.
    /// </summary>
    /// <remarks>
    /// A caller's own <see cref="MessageHeaders"/> is immutable and may be reused across many sends, so
    /// the content type is added to a copy. A caller that has already set the content type itself keeps
    /// its value: an explicit choice at the call site is a deliberate one, and silently overwriting it
    /// would make a header the caller can set a header the caller cannot set.
    /// </remarks>
    private static MessageHeaders WithContentType(MessageHeaders? headers, string contentType)
    {
        if (headers is null || headers.Count == 0)
        {
            return new MessageHeaders(
                new Dictionary<string, string>(1, StringComparer.Ordinal)
                {
                    [SerializationHeaderKeys.ContentType] = contentType,
                });
        }

        if (headers.ContainsKey(SerializationHeaderKeys.ContentType))
        {
            return headers;
        }

        var merged = new Dictionary<string, string>(headers.Count + 1, StringComparer.Ordinal);

        foreach (KeyValuePair<string, string> header in headers)
        {
            merged[header.Key] = header.Value;
        }

        merged[SerializationHeaderKeys.ContentType] = contentType;
        return new MessageHeaders(merged);
    }
}
