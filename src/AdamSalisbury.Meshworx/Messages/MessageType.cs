namespace AdamSalisbury.Meshworx.Messages;

internal enum MessageType : byte
{
    RegistrationComplete = 0x01,
    SendMessage = 0x02,
    DeliverMessage = 0x03,
    RegistrationRequest = 0x04,
    Error = 0x05,
    ClientLookupRequest = 0x06,
    ClientLookupResponse = 0x07,
    Disconnect = 0x08,
    Ping = 0x09,
    Pong = 0x0A,
    BroadcastMessage = 0x0B,
    JoinGroup = 0x0C,
    LeaveGroup = 0x0D,
    GroupMessage = 0x0E,
    DeliverGroupMessage = 0x0F,
    GroupJoinRefused = 0x10,
    SendMessageWithHeaders = 0x11,
    DeliverMessageWithHeaders = 0x12,
    GroupMessageWithHeaders = 0x13,
    DeliverGroupMessageWithHeaders = 0x14,
    QueueSaturated = 0x15,
    ResumeSession = 0x16,
    SessionResumed = 0x17,
    SessionResumeRefused = 0x18,
    SubscribeTopic = 0x19,
    UnsubscribeTopic = 0x1A,
    PublishTopicMessage = 0x1B,
    PublishTopicMessageWithHeaders = 0x1C,
    DeliverTopicMessage = 0x1D,
    DeliverTopicMessageWithHeaders = 0x1E,
    SetClientAttributes = 0x1F,
    FindClientsRequest = 0x20,
    FindClientsResponse = 0x21,
    SubscribePresence = 0x22,
    UnsubscribePresence = 0x23,
    PresenceChanged = 0x24,

    // Hub-to-hub federation (issue #40). These opcodes are never sent or understood by MeshClient — they
    // are spoken only between two MeshHub instances over a peer link established via LinkPeerAsync, and
    // share this byte space purely because MessageType is the one place every wire opcode in this
    // library is enumerated, not because a client connection and a peer link are the same protocol.
    PeerHello = 0x25,
    PeerHelloAck = 0x26,
    PeerRouteAdvertise = 0x27,
    PeerRouteWithdraw = 0x28,
    PeerDeliverMessage = 0x29,
    PeerDeliverGroupMessage = 0x2A,
    PeerDeliverTopicMessage = 0x2B,

    /// <summary>
    /// Client → hub. Advertises the compression algorithms this client can decompress, so a peer sending
    /// to it can pick one it will actually be able to read. Sent immediately after registration rather
    /// than inside it: the registration frame's credential consumes everything after the name, leaving
    /// nowhere to splice this in.
    /// </summary>
    AdvertiseCompression = 0x2C,

    /// <summary>
    /// Client → hub. Asks what compression algorithms another client has advertised.
    /// </summary>
    CompressionCapabilityRequest = 0x2D,

    /// <summary>
    /// Hub → client. Answers a <see cref="CompressionCapabilityRequest"/>.
    /// </summary>
    CompressionCapabilityResponse = 0x2E,
}
