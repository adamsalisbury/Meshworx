namespace AdamSalisbury.Meshworx.Transport;

/// <summary>
/// Represents a bidirectional message-oriented communication channel.
/// </summary>
/// <remarks>
/// Implementations are responsible for their own message framing. Callers send and receive
/// complete messages as opaque byte payloads. The transport does not interpret the content.
/// <para>
/// Implementations must support concurrent calls to <see cref="SendAsync"/>. However,
/// <see cref="ReceiveAsync"/> assumes a single reader — callers must not invoke it
/// concurrently from multiple threads.
/// </para>
/// </remarks>
public interface ITransport : IAsyncDisposable
{
    /// <summary>
    /// Sends a complete message to the remote endpoint.
    /// </summary>
    /// <param name="data">The message payload to send.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);

    /// <summary>
    /// Receives the next complete message from the remote endpoint.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The received message payload, or <see langword="null"/> if the connection has been closed.</returns>
    Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default);
}
