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
}
