namespace AdamSalisbury.Meshworx.Serialization;

/// <summary>
/// Turns an application's typed values into the opaque bytes Meshworx routes, and back again.
/// </summary>
/// <remarks>
/// This abstraction lives entirely above the wire. The hub never sees it, never loads it, and never
/// interprets what it produces: a serialized value is an ordinary message body, exactly as opaque to
/// routing as a hand-rolled byte array. Everything a codec adds is carried in the header envelope, which
/// the hub already passes through unchanged.
/// <para>
/// Implementations must be thread-safe. A single serializer instance is typically shared by every send
/// and receive on a client, and a client's sends and its receive loop run concurrently.
/// </para>
/// </remarks>
public interface IMessageSerializer
{
    /// <summary>
    /// The media type this codec produces, written to the
    /// <see cref="SerializationHeaderKeys.ContentType"/> header on every value it serializes.
    /// </summary>
    /// <remarks>
    /// A receiver compares this against the header on an inbound message to decide whether the codec it
    /// holds is the one that produced the body — see
    /// <see cref="MessageSerializationExtensions"/>.TryDeserialize. Two codecs that produce
    /// incompatible bytes must therefore report different content types, or a receiver holding one will
    /// try to decode the other's output.
    /// </remarks>
    string ContentType { get; }

    /// <summary>
    /// Serializes a value into a message body.
    /// </summary>
    /// <typeparam name="TValue">The type of the value to serialize.</typeparam>
    /// <param name="value">The value to serialize.</param>
    /// <returns>The serialized body, ready to send as an opaque message.</returns>
    ReadOnlyMemory<byte> Serialize<TValue>(TValue value);

    /// <summary>
    /// Deserializes a message body back into a value.
    /// </summary>
    /// <typeparam name="TValue">The type to deserialize into.</typeparam>
    /// <param name="data">The message body, as received.</param>
    /// <returns>
    /// The deserialized value, or <see langword="null"/> when the body encodes an explicit null.
    /// </returns>
    /// <remarks>
    /// The body arrives from a remote peer and is not validated by the hub or by this library, so an
    /// implementation is expected to throw on malformed input rather than return a partially populated
    /// value. Callers that would rather not handle an exception per message should use
    /// <see cref="MessageSerializationExtensions"/>.TryDeserialize.
    /// </remarks>
    TValue? Deserialize<TValue>(ReadOnlySpan<byte> data);
}
