using System.Net.Sockets;
using AdamSalisbury.Meshworx.Interfaces;
using AdamSalisbury.Meshworx.Internal;
using Microsoft.Extensions.Logging;

namespace AdamSalisbury.Meshworx;

public sealed class MeshClient : IMeshClient, IAsyncDisposable
{
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ILogger<MeshClient> _logger;
    private TcpClient? _tcpClient;
    private NetworkStream? _stream;
    private CancellationTokenSource? _cts;
    private Task? _receiveLoopTask;

    public MeshClient(ILogger<MeshClient> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc/>
    public Guid Id { get; private set; }

    /// <inheritdoc/>
    public event EventHandler<MessageReceivedEventArgs>? MessageReceived;

    /// <inheritdoc/>
    public async Task ConnectAsync(string host, int port, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(host);

        try
        {
            if (_tcpClient is not null)
            {
                throw new InvalidOperationException("Already connected to a hub.");
            }

            _logger.LogInformation("Connecting to hub at {Host}:{Port}", host, port);
            _tcpClient = new TcpClient();
            await _tcpClient.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
            _stream = _tcpClient.GetStream();

            (MessageType Type, byte[] Payload)? frame = await MeshFrameCodec.ReadFrameAsync(
                _stream,
                cancellationToken).ConfigureAwait(false);

            if (frame is null
                || frame.Value.Type != MessageType.RegistrationComplete
                || frame.Value.Payload.Length != 16)
            {
                CleanUp();
                throw new InvalidOperationException("Failed to register with the hub.");
            }

            Id = new Guid(frame.Value.Payload);
            _logger.LogInformation("Connected to hub at {Host}:{Port} with id {ClientId}", host, port, Id);

            _cts = new CancellationTokenSource();
            _receiveLoopTask = ReceiveLoopAsync(_cts.Token);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Failed to connect to hub");
            throw;
        }
    }

    private void CleanUp()
    {
        _stream?.Dispose();
        _tcpClient?.Dispose();
        _cts?.Dispose();

        _stream = null;
        _tcpClient = null;
        _cts = null;
        _receiveLoopTask = null;
    }

    /// <inheritdoc/>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (_tcpClient is null)
        {
            return;
        }

        if (_cts is not null)
        {
            await _cts.CancelAsync().ConfigureAwait(false);
        }

        if (_receiveLoopTask is not null)
        {
            try
            {
                await _receiveLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // CancelationToken triggered
            }
        }

        CleanUp();
        Id = Guid.Empty;
    }

    /// <inheritdoc/>
    public async Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        CancellationToken cancellationToken = default)
    {
        if (_stream is null)
        {
            throw new InvalidOperationException("Not connected to a hub.");
        }

        var payload = new byte[16 + message.Length];
        recipientId.TryWriteBytes(payload);
        message.CopyTo(payload.AsMemory(16));

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await MeshFrameCodec.WriteFrameAsync(
                _stream,
                MessageType.SendMessage,
                payload,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
        _writeLock.Dispose();
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                (MessageType Type, byte[] Payload)? frame = await MeshFrameCodec.ReadFrameAsync(
                    _stream!,
                    cancellationToken).ConfigureAwait(false);

                if (frame is null)
                {
                    break;
                }

                if (frame.Value.Type == MessageType.DeliverMessage && frame.Value.Payload.Length >= 16)
                {
                    var senderId = new Guid(frame.Value.Payload.AsSpan(0, 16));
                    ReadOnlyMemory<byte> messageData = frame.Value.Payload.AsMemory(16);

                    MessageReceived?.Invoke(this, new MessageReceivedEventArgs
                    {
                        SenderId = senderId,
                        Data = messageData,
                    });
                }
            }
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException)
        {
            _logger.LogError(ex, "Exiting receive loop. Likely cancellation token received.");
        }
    }
}
