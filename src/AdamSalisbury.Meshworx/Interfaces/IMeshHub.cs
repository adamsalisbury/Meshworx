namespace AdamSalisbury.Meshworx.Interfaces;

public interface IMeshHub
{
    Task StartAsync(CancellationToken cancellationToken = default);
    Task StopAsync(CancellationToken cancellationToken = default);
    bool IsClientRegistered(Guid clientId);
}
