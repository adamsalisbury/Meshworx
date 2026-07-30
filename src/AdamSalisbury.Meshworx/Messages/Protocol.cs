namespace AdamSalisbury.Meshworx.Messages;

internal static class Protocol
{
    /// <summary>
    /// The lowest wire-protocol version this build of the hub and client will negotiate down to.
    /// </summary>
    internal const byte MinSupportedVersion = 4;

    /// <summary>
    /// The highest wire-protocol version this build of the hub and client supports, and the version
    /// advertised when there is no reason to negotiate down.
    /// </summary>
    internal const byte MaxSupportedVersion = 10;

    /// <summary>
    /// The lowest negotiated protocol version at which the structured message-header envelope
    /// (<see cref="MessageHeaders"/>) may be used. A connection negotiated below this version
    /// understands only the plain, header-less message frames.
    /// </summary>
    internal const byte HeaderEnvelopeMinVersion = 5;

    /// <summary>
    /// The lowest negotiated protocol version at which a client may reclaim a previous session — keeping
    /// its assigned id and group memberships across a reconnect. A connection negotiated below this
    /// version is never issued a resumption token and its <see cref="MessageType.RegistrationComplete"/>
    /// reply carries none, so the frame stays byte-identical to what earlier versions produced.
    /// </summary>
    internal const byte SessionResumptionMinVersion = 6;

    /// <summary>
    /// The lowest negotiated protocol version at which a <see cref="MessageType.SessionResumed"/> reply
    /// carries the group memberships the hub restored for the reclaimed identity. A connection negotiated
    /// below this version still has those memberships restored on the hub — <see
    /// cref="SessionResumptionMinVersion"/> alone governs that — it just learns nothing about them from the
    /// reply itself, so the reply stays byte-identical to what version 6 produces.
    /// </summary>
    internal const byte SessionResumedGroupsMinVersion = 7;

    /// <summary>
    /// The lowest negotiated protocol version at which topic pub/sub (<see cref="MessageType.SubscribeTopic"/>,
    /// <see cref="MessageType.UnsubscribeTopic"/>, <see cref="MessageType.PublishTopicMessage"/> and their
    /// header-bearing and delivery counterparts) may be used. A connection negotiated below this version
    /// has never had the opcodes defined at its end, on either side, so both <see cref="MeshClient"/> and
    /// <see cref="MeshHub"/> refuse to send or act on them rather than risk a peer silently discarding an
    /// opcode it does not recognise.
    /// </summary>
    internal const byte TopicPubSubMinVersion = 8;

    /// <summary>
    /// The lowest negotiated protocol version at which client attribute metadata
    /// (<see cref="MessageType.SetClientAttributes"/>, <see cref="MessageType.FindClientsRequest"/> and
    /// <see cref="MessageType.FindClientsResponse"/>) may be used. Follows the same rationale as
    /// <see cref="TopicPubSubMinVersion"/>: the opcodes did not exist before this version on either end,
    /// so a connection negotiated below it refuses to send or act on them.
    /// </summary>
    internal const byte ClientAttributesMinVersion = 9;

    /// <summary>
    /// The lowest negotiated protocol version at which presence subscription
    /// (<see cref="MessageType.SubscribePresence"/>, <see cref="MessageType.UnsubscribePresence"/> and
    /// <see cref="MessageType.PresenceChanged"/>) may be used. Follows the same rationale as
    /// <see cref="TopicPubSubMinVersion"/> and <see cref="ClientAttributesMinVersion"/>.
    /// </summary>
    internal const byte PresenceMinVersion = 10;

    /// <summary>
    /// The length, in bytes, of a session resumption token. 32 bytes of cryptographically secure
    /// randomness — the token is a bearer credential for an identity, so it has to be long enough that
    /// guessing one is not a realistic attack even against a hub that will happily be asked repeatedly.
    /// </summary>
    internal const int SessionTokenLength = 32;

    internal const int MaxClientNameLength = 256;

    /// <summary>
    /// The maximum number of key/value pairs a single client's attribute bag may hold. Bounds the memory
    /// a hub commits per client to directory metadata, independently of how many clients are connected.
    /// </summary>
    internal const int MaxClientAttributeCount = 32;

    /// <summary>
    /// The maximum length, in UTF-8 bytes, of a single attribute key.
    /// </summary>
    internal const int MaxClientAttributeKeyLength = 128;

    /// <summary>
    /// The maximum length, in UTF-8 bytes, of a single attribute value.
    /// </summary>
    internal const int MaxClientAttributeValueLength = 512;
}
