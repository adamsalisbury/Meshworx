using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx.Serialization;

/// <summary>
/// Typed receive extensions: decode a received message's body with a codec, having first checked the
/// body was produced by that codec.
/// </summary>
/// <remarks>
/// The content-type check is what makes these safe to call on a connection carrying more than one kind
/// of traffic. A body is just bytes, and a codec asked to decode another codec's output will either
/// throw or — worse — succeed and produce a plausible but wrong value. Comparing
/// <see cref="SerializationHeaderKeys.ContentType"/> against the codec's own
/// <see cref="IMessageSerializer.ContentType"/> first turns that into an explicit, checkable outcome.
/// </remarks>
public static class MessageSerializationExtensions
{
    /// <summary>
    /// Deserializes a received direct message's body.
    /// </summary>
    /// <typeparam name="TValue">The type to deserialize into.</typeparam>
    /// <param name="message">The received message.</param>
    /// <param name="serializer">The codec to deserialize with.</param>
    /// <returns>
    /// The deserialized value, or <see langword="null"/> when the body encodes an explicit null.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The message carries a <see cref="SerializationHeaderKeys.ContentType"/> that
    /// <paramref name="serializer"/> did not produce.
    /// </exception>
    public static TValue? Deserialize<TValue>(
        this MessageReceivedEventArgs message, IMessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(serializer);

        ThrowIfContentTypeMismatched(message.Headers, serializer);
        return serializer.Deserialize<TValue>(message.Data.Span);
    }

    /// <summary>
    /// Deserializes a received group message's body.
    /// </summary>
    /// <typeparam name="TValue">The type to deserialize into.</typeparam>
    /// <param name="message">The received group message.</param>
    /// <param name="serializer">The codec to deserialize with.</param>
    /// <returns>
    /// The deserialized value, or <see langword="null"/> when the body encodes an explicit null.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The message carries a <see cref="SerializationHeaderKeys.ContentType"/> that
    /// <paramref name="serializer"/> did not produce.
    /// </exception>
    public static TValue? Deserialize<TValue>(
        this GroupMessageReceivedEventArgs message, IMessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(serializer);

        ThrowIfContentTypeMismatched(message.Headers, serializer);
        return serializer.Deserialize<TValue>(message.Data.Span);
    }

    /// <summary>
    /// Attempts to deserialize a received direct message's body, returning <see langword="false"/>
    /// rather than throwing when the body is not this codec's to decode, or cannot be decoded.
    /// </summary>
    /// <typeparam name="TValue">The type to deserialize into.</typeparam>
    /// <param name="message">The received message.</param>
    /// <param name="serializer">The codec to deserialize with.</param>
    /// <param name="value">
    /// The deserialized value when this returns <see langword="true"/>; otherwise the default for
    /// <typeparamref name="TValue"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the body was produced by this codec and decoded successfully;
    /// otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// The intended shape for a handler on a connection carrying mixed traffic: try each codec or type
    /// in turn without an exception per miss. A body arrives from a remote peer, so a decode failure is
    /// an ordinary runtime condition rather than a programming error, and is reported as one here.
    /// </remarks>
    public static bool TryDeserialize<TValue>(
        this MessageReceivedEventArgs message, IMessageSerializer serializer, out TValue? value)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(serializer);

        return TryDeserializeCore(message.Headers, message.Data, serializer, out value);
    }

    /// <summary>
    /// Attempts to deserialize a received group message's body, returning <see langword="false"/>
    /// rather than throwing when the body is not this codec's to decode, or cannot be decoded.
    /// </summary>
    /// <typeparam name="TValue">The type to deserialize into.</typeparam>
    /// <param name="message">The received group message.</param>
    /// <param name="serializer">The codec to deserialize with.</param>
    /// <param name="value">
    /// The deserialized value when this returns <see langword="true"/>; otherwise the default for
    /// <typeparamref name="TValue"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the body was produced by this codec and decoded successfully;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool TryDeserialize<TValue>(
        this GroupMessageReceivedEventArgs message, IMessageSerializer serializer, out TValue? value)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(serializer);

        return TryDeserializeCore(message.Headers, message.Data, serializer, out value);
    }

    private static bool TryDeserializeCore<TValue>(
        MessageHeaders headers,
        ReadOnlyMemory<byte> data,
        IMessageSerializer serializer,
        out TValue? value)
    {
        value = default;

        if (!IsContentTypeAcceptable(headers, serializer))
        {
            return false;
        }

        try
        {
            value = serializer.Deserialize<TValue>(data.Span);
            return true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
        {
            // A codec is integrator-supplied and free to throw whatever its underlying library throws —
            // JsonException here, but a MessagePack or Protobuf codec would throw its own unrelated
            // types. Catching broadly is the only way this can honour its contract of not throwing on a
            // body it cannot decode, and it is exactly the application boundary where a broad catch is
            // warranted: a malformed frame from a remote peer must not take down a receive loop.
            return false;
        }
    }

    /// <summary>
    /// Whether a message's declared content type is one the codec claims to have produced.
    /// </summary>
    /// <remarks>
    /// A message with no content-type header at all is accepted. The header is only written by this
    /// package's own typed sends, so an absent one means the body came from a byte-oriented send or from
    /// a peer that predates this package — in which case the caller reaching for a codec is asserting it
    /// knows the format, and there is nothing to contradict it.
    /// </remarks>
    private static bool IsContentTypeAcceptable(MessageHeaders headers, IMessageSerializer serializer)
    {
        return !headers.TryGetValue(SerializationHeaderKeys.ContentType, out string? contentType)
            || string.Equals(contentType, serializer.ContentType, StringComparison.Ordinal);
    }

    private static void ThrowIfContentTypeMismatched(MessageHeaders headers, IMessageSerializer serializer)
    {
        if (!IsContentTypeAcceptable(headers, serializer))
        {
            throw new InvalidOperationException(
                $"The message declares content type '{headers[SerializationHeaderKeys.ContentType]}', "
                + $"which was not produced by this serializer ('{serializer.ContentType}').");
        }
    }
}
