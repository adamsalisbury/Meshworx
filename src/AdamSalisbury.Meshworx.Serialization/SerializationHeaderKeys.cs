namespace AdamSalisbury.Meshworx.Serialization;

/// <summary>
/// The well-known <see cref="Messages.MessageHeaders"/> keys this codec layer writes and reads.
/// </summary>
/// <remarks>
/// Public, unlike the core library's own header-key constants, because a codec is a peer-to-peer
/// agreement rather than a hub one: the two endpoints must be able to name the same key, and they do so
/// from this assembly rather than from the core's internals. The hub neither writes nor reads these — it
/// passes the header block through unchanged, as it does for every header it has no behaviour for.
/// </remarks>
public static class SerializationHeaderKeys
{
    /// <summary>
    /// The header key whose value is the media type of the message body, as reported by the
    /// <see cref="IMessageSerializer.ContentType"/> of the codec that produced it.
    /// </summary>
    /// <remarks>
    /// Written by every typed send in <see cref="MeshClientSerializationExtensions"/> and checked by
    /// <see cref="MessageSerializationExtensions"/>.TryDeserialize, so a receiver holding the
    /// wrong codec for a message declines it rather than decoding another codec's bytes into a plausible
    /// but wrong value.
    /// </remarks>
    public const string ContentType = "mesh.content-type";
}
