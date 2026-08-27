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
    internal const byte MaxSupportedVersion = 12;

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
    /// The lowest protocol version that can advertise and query compression algorithm support.
    /// </summary>
    /// <remarks>
    /// Gated from the outset, as client attributes and presence were. A connection below this negotiates
    /// no capabilities at all, and a compressing send on it behaves exactly as it did before negotiation
    /// existed — the sender uses its own best algorithm and the receiver drops the message if it cannot
    /// read it. Negotiation is an optimisation over that, never a precondition for it.
    /// </remarks>
    internal const byte CompressionNegotiationMinVersion = 11;

    /// <summary>
    /// The lowest protocol version at which a chunked message's body may be compressed, and the lowest
    /// at which a <see cref="MessageType.CompressionCapabilityResponse"/> carries the subject client's
    /// own negotiated version.
    /// </summary>
    /// <remarks>
    /// The two are one version because the first needs the second. A chunked compressed message is
    /// compressed a chunk at a time, so its receiver must decompress each frame before reassembling it —
    /// where a receiver from <see cref="CompressionNegotiationMinVersion"/> reassembles first and
    /// decompresses after, and would therefore try to decompress the concatenation of the chunks rather
    /// than each of them. A sender cannot tell which of those a recipient does from its own negotiated
    /// version, which is its agreement with the hub rather than with the recipient, so the hub reports the
    /// recipient's version alongside the algorithms it advertised and the sender gates on that. A
    /// recipient below this version, or one whose version cannot be established, is sent uncompressed
    /// chunks.
    /// </remarks>
    internal const byte ChunkedCompressionMinVersion = 12;

    /// <summary>
    /// The most compression algorithms one client may advertise.
    /// </summary>
    /// <remarks>
    /// A ceiling on what a peer can assert before any of it is held: the advertised set is kept per
    /// connection for as long as that connection lives, and handed back to anyone who asks. Sixteen is far
    /// past the number of genuinely distinct compressors any endpoint has reason to register, while
    /// leaving no room to use the advertisement as a place to park data.
    /// </remarks>
    internal const int MaxAdvertisedCompressionAlgorithms = 16;

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

    /// <summary>
    /// The lowest peer-link protocol version this build of <see cref="MeshHub"/> will negotiate down to
    /// when federating with another hub. Deliberately a separate range from
    /// <see cref="MinSupportedVersion"/>/<see cref="MaxSupportedVersion"/>: a peer link
    /// (<see cref="MessageType.PeerHello"/> onward) is a distinct protocol spoken only between two
    /// <see cref="MeshHub"/> instances, never by <see cref="MeshClient"/>, so it has no reason to share a
    /// version number with the client-facing protocol or to move in step with it.
    /// </summary>
    internal const byte MinFederationVersion = 1;

    /// <summary>
    /// The highest peer-link protocol version this build of <see cref="MeshHub"/> supports.
    /// </summary>
    internal const byte MaxFederationVersion = 1;

    /// <summary>
    /// The maximum number of routes a single hub will accept as advertised by one peer. Bounds the
    /// memory a misbehaving or compromised peer link can force a hub to commit to remote routing state —
    /// a peer is trusted once its link is admitted (see <see cref="MeshHub"/>'s <c>peerAuthenticator</c>
    /// and <c>allowIncomingPeerLinks</c>), but trusted is not the same as unbounded. Generous for any
    /// real federated deployment; pass nothing to accept the default.
    /// </summary>
    internal const int MaxRemoteRoutesPerPeer = 100_000;

    /// <summary>
    /// The maximum size, in bytes, of a message body the hub will retain as a group's or topic's
    /// last-value message. Deliberately far smaller than <see cref="AdamSalisbury.Meshworx.Transport.Framing.StreamFramer.MaxPayloadSize"/>
    /// — an ordinary fan-out frame passes through once, but a retained value persists indefinitely and is
    /// replayed to every future joiner or subscriber, so it is bounded far more tightly than a message
    /// that is only ever in flight momentarily.
    /// </summary>
    internal const int MaxRetainedMessageBytes = 64 * 1024;

    /// <summary>
    /// The maximum number of distinct topics the hub will hold a retained value for at once.
    /// </summary>
    /// <remarks>
    /// <c>_retainedTopics</c> is a new top-level dictionary with no existing container to ride alongside
    /// — unlike a group's retained value, which piggybacks on the already-unbounded-by-precedent
    /// <c>_groups</c> dictionary (see KI-63) — so it could otherwise grow without bound purely from
    /// retained publishes, even with zero live subscribers to any of them. See also
    /// <see cref="MaxRetainedGroupCount"/>, which bounds the equivalent amplification for groups.
    /// </remarks>
    internal const int MaxRetainedTopicCount = 10_000;

    /// <summary>
    /// The maximum number of groups that may simultaneously hold a retained value.
    /// </summary>
    /// <remarks>
    /// A group's own membership is already unbounded by precedent (see KI-63), but only by a few hundred
    /// bytes of overhead each. A retained value turns that same unbounded count into up to
    /// <see cref="MaxRetainedMessageBytes"/> each — a materially larger amplification than the pre-
    /// existing gap, so it is bounded independently of group count itself, mirroring
    /// <see cref="MaxRetainedTopicCount"/> for topics.
    /// </remarks>
    internal const int MaxRetainedGroupCount = 10_000;

    /// <summary>
    /// The maximum length, in characters, of a compression algorithm id registered with an
    /// <see cref="Compression.ICompressionStrategyRegistry"/>.
    /// </summary>
    /// <remarks>
    /// The id travels between the two endpoints as a header value and, once endpoints advertise their
    /// registered set to one another, as a repeated one — so it is bounded at the shape of a content-
    /// coding name rather than left to grow into a payload of its own. Nothing in the hub reads it; the
    /// bound exists so an endpoint cannot be talked into holding an arbitrarily long id on a peer's
    /// behalf.
    /// </remarks>
    internal const int MaxCompressionAlgorithmIdLength = 32;

    /// <summary>
    /// The smallest body a compressing send will even attempt to compress.
    /// </summary>
    /// <remarks>
    /// Below this, every algorithm's own container overhead is a meaningful fraction of the payload and
    /// the attempt is more likely to grow the message than shrink it — so the work is skipped rather
    /// than done and thrown away. A fixed floor rather than a knob: the useful range is narrow, and the
    /// send already falls back to the uncompressed body whenever compression fails to help, which is the
    /// general guard this is merely a cheap shortcut past.
    /// </remarks>
    internal const int MinimumCompressionSize = 256;

    /// <summary>
    /// The default ceiling on what a single compressed message may decompress to.
    /// </summary>
    /// <remarks>
    /// The uncompressed length arrives in a header written by the peer, so it is an assertion before it
    /// is a fact: a receiver that trusted it would let a small frame ask it to allocate an arbitrarily
    /// large buffer. The declared length is checked against this before a byte is decompressed. Matches
    /// the default reassembly ceiling deliberately — both bound memory held on a peer's behalf, and a
    /// consumer who has reasoned about one has already reasoned about the other.
    /// </remarks>
    internal const int DefaultMaxDecompressedMessageBytes = 64 * 1024 * 1024;
}
