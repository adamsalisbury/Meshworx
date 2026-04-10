namespace AdamSalisbury.Meshworx.Interfaces;

public interface IMeshClient
{
    Guid Id { get; }
    Task ConnectAsync(string host, int port, CancellationToken cancellationToken = default);
    Task DisconnectAsync(CancellationToken cancellationToken = default);
    Task SendAsync(Guid recipientId, ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default);
    event EventHandler<MessageReceivedEventArgs> MessageReceived;
}
