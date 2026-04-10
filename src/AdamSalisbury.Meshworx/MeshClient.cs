using System.Text;
using AdamSalisbury.Meshworx.Internal;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;

namespace AdamSalisbury.Meshworx;

public sealed class MeshClient : IMeshClient, IAsyncDisposable
{
    private readonly ILogger<MeshClient> _logger;
    private ITransport? _transport;
    private CancellationTokenSource? _cts;
    private Task? _receiveLoopTask;

    public MeshClient(ILogger<MeshClient> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc/>
    public Guid Id { get; private set; }

    /// <inheritdoc/>
    public string Name { get; private set; } = string.Empty;

    /// <inheritdoc/>
    public event EventHandler<MessageReceivedEventArgs>? MessageReceived;

    /// <inheritdoc/>
    public async Task ConnectAsync(ITransport transport, string clientName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentException.ThrowIfNullOrEmpty(clientName);

        try
        {
            if (_transport is not null)
            {
                throw new InvalidOperationException("Already connected to a hub.");
            }

            _transport = transport;

            byte[] nameBytes = Encoding.UTF8.GetBytes(clientName);
            var requestPayload = new byte[1 + nameBytes.Length];
            requestPayload[0] = (byte)MessageType.RegistrationRequest;
            nameBytes.CopyTo(requestPayload, 1);
            await _transport.SendAsync(requestPayload, cancellationToken).ConfigureAwait(false);

            byte[]? responseData = await _transport.ReceiveAsync(cancellationToken).ConfigureAwait(false);

            if (responseData is null
                || responseData.Length != 17
                || (MessageType)responseData[0] != MessageType.RegistrationComplete)
            {
                await CleanUpAsync().ConfigureAwait(false);
                throw new InvalidOperationException("Failed to register with the hub.");
            }

            Id = new Guid(responseData.AsSpan(1, 16));
            Name = clientName;
            _logger.LogInformation("Connected to hub with id {ClientId}", Id);

            _cts = new CancellationTokenSource();
            _receiveLoopTask = ReceiveLoopAsync(_cts.Token);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Failed to connect to hub");
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (_transport is null)
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
                // CancellationToken triggered
            }
        }

        await CleanUpAsync().ConfigureAwait(false);
        Id = Guid.Empty;
        Name = string.Empty;
    }

    /// <inheritdoc/>
    public async Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        CancellationToken cancellationToken = default)
    {
        if (_transport is null)
        {
            throw new InvalidOperationException("Not connected to a hub.");
        }

        var payload = new byte[1 + 16 + message.Length];
        payload[0] = (byte)MessageType.SendMessage;
        recipientId.TryWriteBytes(payload.AsSpan(1));
        message.CopyTo(payload.AsMemory(17));

        await _transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public Task<Guid?> GetClientIdByName(string name, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
    }

    private async Task CleanUpAsync()
    {
        if (_transport is not null)
        {
            await _transport.DisposeAsync().ConfigureAwait(false);
        }

        _cts?.Dispose();
        _transport = null;
        _cts = null;
        _receiveLoopTask = null;
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                byte[]? data = await _transport!.ReceiveAsync(cancellationToken).ConfigureAwait(false);

                if (data is null)
                {
                    break;
                }

                if (data.Length >= 17
                    && (MessageType)data[0] == MessageType.DeliverMessage)
                {
                    var senderId = new Guid(data.AsSpan(1, 16));
                    ReadOnlyMemory<byte> messageData = data.AsMemory(17);

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
