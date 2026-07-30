using System.Threading.Channels;
using AdamSalisbury.Meshworx.Transport.Framing;

namespace AdamSalisbury.Meshworx.Transport.InMemory;

/// <summary>
/// An in-process <see cref="ITransport"/> that exchanges messages with a paired endpoint through
/// channels rather than a network. Useful for hosting a hub and clients in the same process and for
/// fast, deterministic testing.
/// </summary>
/// <remarks>
/// Each message passed to <see cref="SendAsync"/> is copied, so callers may reuse their buffers. The
/// channels preserve message boundaries, so no framing is applied.
/// <para>
/// The payload cap is enforced in both directions even though nothing here frames a message, because
/// this type's purpose is to stand in for the stream transports: a double that accepts what its
/// subject rejects turns a real defect into a passing test. The cap is a property of
/// <see cref="ITransport"/>, not of stream framing.
/// </para>
/// </remarks>
public sealed class InMemoryTransport : ITransport
{
    private readonly ChannelReader<byte[]> _receive;
    private readonly ChannelWriter<byte[]> _receiveWriter;
    private readonly ChannelWriter<byte[]> _send;
    private int _disposed;

    /// <remarks>
    /// The writer end of this endpoint's own inbound channel is held alongside the reader so that
    /// disposal can complete it. Without that, a peer goes on writing successfully into a channel this
    /// endpoint will never read again.
    /// </remarks>
    internal InMemoryTransport(Channel<byte[]> inbound, ChannelWriter<byte[]> send)
    {
        _receive = inbound.Reader;
        _receiveWriter = inbound.Writer;
        _send = send;
    }

    /// <summary>
    /// Creates a connected pair of in-memory transports. A message sent on one endpoint is received on
    /// the other.
    /// </summary>
    /// <returns>The two connected endpoints.</returns>
    public static (InMemoryTransport First, InMemoryTransport Second) CreatePair()
    {
        var firstInbound = Channel.CreateUnbounded<byte[]>();
        var secondInbound = Channel.CreateUnbounded<byte[]>();

        var first = new InMemoryTransport(firstInbound, secondInbound.Writer);
        var second = new InMemoryTransport(secondInbound, firstInbound.Writer);

        return (first, second);
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentException">
    /// <paramref name="data"/> exceeds the maximum frame payload, as it would on any stream transport.
    /// </exception>
    /// <exception cref="IOException">The peer has disposed its endpoint.</exception>
    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (data.Length > StreamFramer.MaxPayloadSize)
        {
            throw new ArgumentException(
                $"Payload size {data.Length} exceeds the maximum frame payload of "
                + $"{StreamFramer.MaxPayloadSize} bytes.",
                nameof(data));
        }

        // Copy: the message stays queued until the peer reads it, so it must not alias the caller's buffer.
        if (!_send.TryWrite(data.ToArray()))
        {
            // The peer completed this channel by disposing. Discarding the result instead would report
            // success for every send to a departed peer, for ever — which no stream transport does.
            throw new IOException("The peer endpoint has been disposed.");
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    /// <exception cref="IOException">The peer sent a payload over the maximum frame payload.</exception>
    public async Task<byte[]?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        // Guarded like SendAsync. Without this, receiving on a disposed endpoint awaits a channel
        // nothing will ever complete, while every stream transport throws promptly.
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        byte[] message;

        try
        {
            message = await _receive.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            // The peer disposed its endpoint and completed this channel: signal a closed connection.
            return null;
        }

        if (message.Length > StreamFramer.MaxPayloadSize)
        {
            // Rejected on the way in as well as on the way out, matching StreamFramer's treatment of an
            // oversize declared length: a peer is not trusted to respect the cap just because this
            // endpoint does.
            throw new IOException(
                $"Payload size {message.Length} exceeds the maximum frame payload of "
                + $"{StreamFramer.MaxPayloadSize} bytes.");
        }

        return message;
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            // Completing the send channel ends the peer's ReceiveAsync, mirroring a closed connection.
            _send.TryComplete();

            // Completing this endpoint's own inbound channel is the other half of that: the peer's next
            // send fails rather than succeeding into a queue with no reader, which is what a stream
            // transport does once the far end has gone.
            _receiveWriter.TryComplete();
        }

        return ValueTask.CompletedTask;
    }
}
