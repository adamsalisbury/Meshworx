namespace AdamSalisbury.Meshworx;

/// <summary>
/// Identifies a single client matched by <see cref="IMeshClient.FindClientsAsync"/>.
/// </summary>
/// <param name="Id">The unique identifier the hub assigned the client.</param>
/// <param name="Name">The name the client registered under.</param>
public sealed record ClientDescriptor(Guid Id, string Name);
