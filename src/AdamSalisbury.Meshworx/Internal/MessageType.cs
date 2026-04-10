namespace AdamSalisbury.Meshworx.Internal;

internal enum MessageType : byte
{
    RegistrationComplete = 0x01,
    SendMessage = 0x02,
    DeliverMessage = 0x03,
    RegistrationRequest = 0x04,
}
