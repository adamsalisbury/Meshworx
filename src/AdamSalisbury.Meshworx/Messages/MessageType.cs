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
}
