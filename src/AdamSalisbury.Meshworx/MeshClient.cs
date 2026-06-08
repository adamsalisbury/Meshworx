using System.Buffers.Binary;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;

namespace AdamSalisbury.Meshworx;

public sealed class MeshClient : IMeshClient, IAsyncDisposable
{
    private readonly ILogger<MeshClient> _logger;
    private readonly Lock _stateLock = new();
    private readonly SemaphoreSlim _lookupLock = new(1, 1);
    private ConnectionState _state = ConnectionState.Disconnected;
    private ITransport? _transport;
    private CancellationTokenSource? _cts;
    private Task? _receiveLoopTask;

    // Single-slot pending lookup, serialised by _lookupLock. Each request carries a
    // correlation id echoed by the hub; the receive loop only completes the pending
    // lookup when the ids match, so a response from a cancelled request cannot resolve
    // a subsequent lookup with a stale result.
    private PendingLookup? _pendingLookup;
    private int _lookupCorrelationId;

    public MeshClient(ILogger<MeshClient> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
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

        if (clientName.Length > Protocol.MaxClientNameLength)
        {
            throw new ArgumentException(
                $"Client name exceeds the maximum length of {Protocol.MaxClientNameLength} characters.",
                nameof(clientName));
        }

        lock (_stateLock)
        {
            if (_state is not ConnectionState.Disconnected)
            {
                throw new InvalidOperationException(_state switch
                {
                    ConnectionState.Connecting => "A connection attempt is already in progress.",
                    ConnectionState.Disconnecting => "A disconnect is in progress.",
                    _ => "Already connected to a hub.",
                });
            }

            _state = ConnectionState.Connecting;
            _transport = transport;
        }

        try
        {
            byte[] nameBytes = Encoding.UTF8.GetBytes(clientName);
            var requestPayload = new byte[2 + nameBytes.Length];
            requestPayload[0] = (byte)MessageType.RegistrationRequest;
            requestPayload[1] = Protocol.Version;
            nameBytes.CopyTo(requestPayload, 2);
            await _transport.SendAsync(requestPayload, cancellationToken).ConfigureAwait(false);

            byte[]? responseData = await _transport.ReceiveAsync(cancellationToken).ConfigureAwait(false);

            if (responseData is { Length: >= 2 }
                && (MessageType)responseData[0] == MessageType.Error)
            {
                var errorCode = (RegistrationErrorCode)responseData[1];
                throw new RegistrationRefusedException(errorCode);
            }

            if (responseData is null
                || responseData.Length != 17
                || (MessageType)responseData[0] != MessageType.RegistrationComplete)
            {
                throw new InvalidOperationException("Failed to register with the hub.");
            }

            Id = new Guid(responseData.AsSpan(1, 16));
            Name = clientName;
            _logger.LogInformation("Connected to hub with id {ClientId}", Id);

            _cts = new CancellationTokenSource();
            _receiveLoopTask = ReceiveLoopAsync(_cts.Token);

            lock (_stateLock)
            {
                _state = ConnectionState.Connected;
            }
        }
        catch (Exception exception)
        {
            await CleanUpAsync().ConfigureAwait(false);

            lock (_stateLock)
            {
                _state = ConnectionState.Disconnected;
            }

            if (exception is RegistrationRefusedException or InvalidOperationException)
            {
                _logger.LogWarning(exception, "Failed to connect to hub");
            }
            else
            {
                _logger.LogError(exception, "Failed to connect to hub");
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        ITransport? transport;
        CancellationTokenSource? cts;
        Task? receiveLoopTask;

        lock (_stateLock)
        {
            if (_state is not ConnectionState.Connected)
            {
                return;
            }

            _state = ConnectionState.Disconnecting;
            transport = _transport;
            cts = _cts;
            receiveLoopTask = _receiveLoopTask;
        }

        try
        {
            byte[] disconnectPayload = [(byte)MessageType.Disconnect];
            await transport!.SendAsync(disconnectPayload, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or OperationCanceledException)
        {
            // Best-effort disconnect notification; the transport may already be closed.
        }

        if (cts is not null)
        {
            await cts.CancelAsync().ConfigureAwait(false);
        }

        if (receiveLoopTask is not null)
        {
            try
            {
                await receiveLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // CancellationToken triggered.
            }
        }

        await CleanUpAsync().ConfigureAwait(false);

        lock (_stateLock)
        {
            Id = Guid.Empty;
            Name = string.Empty;
            _state = ConnectionState.Disconnected;
        }
    }

    /// <inheritdoc/>
    public async Task SendAsync(
        Guid recipientId,
        ReadOnlyMemory<byte> message,
        CancellationToken cancellationToken = default)
    {
        ITransport transport;

        lock (_stateLock)
        {
            if (_state is not ConnectionState.Connected)
            {
                throw new InvalidOperationException("Not connected to a hub.");
            }

            transport = _transport!;
        }

        var payload = new byte[1 + 16 + message.Length];
        payload[0] = (byte)MessageType.SendMessage;
        recipientId.TryWriteBytes(payload.AsSpan(1));
        message.CopyTo(payload.AsMemory(17));

        await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<Guid?> GetClientIdByNameAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        ITransport transport;

        lock (_stateLock)
        {
            if (_state is not ConnectionState.Connected)
            {
                throw new InvalidOperationException("Not connected to a hub.");
            }

            transport = _transport!;
        }

        await _lookupLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // _lookupLock serialises lookups, so a plain increment is sufficient.
            int correlationId = unchecked(_lookupCorrelationId++);
            var completion = new TaskCompletionSource<Guid?>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingLookup = new PendingLookup(correlationId, completion);

            byte[] nameBytes = Encoding.UTF8.GetBytes(name);
            var payload = new byte[1 + 4 + nameBytes.Length];
            payload[0] = (byte)MessageType.ClientLookupRequest;
            BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(1, 4), correlationId);
            nameBytes.CopyTo(payload, 5);
            await transport.SendAsync(payload, cancellationToken).ConfigureAwait(false);

            return await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _pendingLookup = null;

            try
            {
                _lookupLock.Release();
            }
            catch (ObjectDisposedException)
            {
                // The semaphore was disposed during a concurrent DisposeAsync call.
            }
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
        _lookupLock.Dispose();
    }

    private async Task CleanUpAsync()
    {
        ITransport? transport;
        CancellationTokenSource? cts;

        lock (_stateLock)
        {
            transport = _transport;
            cts = _cts;
            _transport = null;
            _cts = null;
            _receiveLoopTask = null;
        }

        if (transport is not null)
        {
            await transport.DisposeAsync().ConfigureAwait(false);
        }

        cts?.Dispose();
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        ITransport transport;

        lock (_stateLock)
        {
            transport = _transport ?? throw new InvalidOperationException("Transport is not initialised.");
        }

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                byte[]? data = await transport.ReceiveAsync(cancellationToken).ConfigureAwait(false);

                if (data is null)
                {
                    break;
                }

                if (data.Length == 0)
                {
                    // Empty frames carry no opcode; ignore rather than indexing data[0].
                    continue;
                }

                if (data.Length >= 17
                    && (MessageType)data[0] == MessageType.DeliverMessage)
                {
                    var senderId = new Guid(data.AsSpan(1, 16));
                    ReadOnlyMemory<byte> messageData = data.AsMemory(17);

                    try
                    {
                        MessageReceived?.Invoke(this, new MessageReceivedEventArgs
                        {
                            SenderId = senderId,
                            Data = messageData,
                        });
                    }
                    catch (Exception ex)
                    {
                        // A throwing subscriber must not tear down the receive loop and
                        // silently halt all further delivery. This is a callback boundary.
                        _logger.LogError(ex, "A MessageReceived handler threw an exception");
                    }
                }
                else if (data.Length >= 6
                    && (MessageType)data[0] == MessageType.ClientLookupResponse)
                {
                    int correlationId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(1, 4));
                    PendingLookup? pending = _pendingLookup;

                    if (pending is null || pending.CorrelationId != correlationId)
                    {
                        // Stale or unsolicited response (e.g. from a cancelled lookup); discard.
                        _logger.LogDebug(
                            "Discarding lookup response with unmatched correlation id {CorrelationId}",
                            correlationId);
                    }
                    else if (data[5] == 0x01 && data.Length >= 22)
                    {
                        pending.Completion.TrySetResult(new Guid(data.AsSpan(6, 16)));
                    }
                    else
                    {
                        pending.Completion.TrySetResult(null);
                    }
                }
                else if ((MessageType)data[0] == MessageType.Disconnect)
                {
                    _logger.LogInformation("Hub sent disconnect");
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Receive loop cancelled");
        }
        catch (IOException ex)
        {
            _logger.LogWarning(ex, "Receive loop terminated due to transport error");
        }
        catch (ObjectDisposedException ex)
        {
            _logger.LogDebug(ex, "Receive loop terminated: transport disposed");
        }
    }

    private enum ConnectionState
    {
        Disconnected,
        Connecting,
        Connected,
        Disconnecting,
    }

    private sealed record PendingLookup(int CorrelationId, TaskCompletionSource<Guid?> Completion);
}
