using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;

namespace AdamSalisbury.Meshworx;

public sealed class MeshHub : IMeshHub, IAsyncDisposable
{
    private readonly ILogger<MeshHub> _logger;
    private readonly ITransportListener _listener;
    private readonly ConcurrentDictionary<Guid, ClientConnection> _clients = new();
    private readonly ConcurrentDictionary<string, Guid> _clientNames = new();
    private readonly ConcurrentDictionary<Task, byte> _handlerTasks = new();
    private CancellationTokenSource? _cts;
    private Task? _acceptLoopTask;

    public MeshHub(ILogger<MeshHub> logger, ITransportListener listener)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(listener);
        _logger = logger;
        _listener = listener;
    }

    /// <inheritdoc/>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_cts is not null)
        {
            throw new InvalidOperationException("The hub is already running.");
        }

        await _listener.StartAsync(cancellationToken).ConfigureAwait(false);
        _cts = new CancellationTokenSource();
        _acceptLoopTask = AcceptLoopAsync(_cts.Token);
    }

    /// <inheritdoc/>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_cts is null)
        {
            return;
        }

        byte[] disconnectPayload = [(byte)MessageType.Disconnect];
        foreach (ClientConnection client in _clients.Values)
        {
            try
            {
                await client.Transport.SendAsync(disconnectPayload, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException or OperationCanceledException)
            {
                // Best-effort disconnect notification; the client may already be gone.
            }
        }

        await _cts.CancelAsync().ConfigureAwait(false);

        if (_acceptLoopTask is not null)
        {
            try
            {
                await _acceptLoopTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when the cancellation token is triggered during shutdown.
            }
        }

        foreach (ClientConnection client in _clients.Values)
        {
            await client.DisposeAsync().ConfigureAwait(false);
        }

        try
        {
            await Task.WhenAll(_handlerTasks.Keys).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Handler task exceptions are already logged individually via ContinueWith.
            // This catch prevents WhenAll from propagating during shutdown.
        }

        _handlerTasks.Clear();
        _clientNames.Clear();
        _clients.Clear();
        _cts.Dispose();
        _cts = null;
        _acceptLoopTask = null;
    }

    /// <inheritdoc/>
    public bool IsClientRegistered(Guid clientId)
    {
        return _clients.ContainsKey(clientId);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        await _listener.DisposeAsync().ConfigureAwait(false);
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            ITransport transport;
            try
            {
                transport = await _listener.AcceptAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            var handlerTask = HandleClientAsync(transport, cancellationToken);
            _handlerTasks.TryAdd(handlerTask, 0);
            _ = handlerTask.ContinueWith(
                t =>
                {
                    _handlerTasks.TryRemove(t, out _);
                    if (t.IsFaulted)
                    {
                        _logger.LogError(t.Exception, "Unhandled exception in client handler");
                    }
                },
                TaskScheduler.Default);
        }
    }

    private async Task HandleClientAsync(ITransport transport, CancellationToken cancellationToken)
    {
        var clientId = Guid.NewGuid();
        ClientConnection? connection = null;

        try
        {
            byte[]? registrationData = await transport.ReceiveAsync(cancellationToken).ConfigureAwait(false);

            if (registrationData is null
                || registrationData.Length < 3
                || (MessageType)registrationData[0] != MessageType.RegistrationRequest)
            {
                return;
            }

            if (registrationData[1] != Protocol.Version)
            {
                byte[] versionError =
                    [(byte)MessageType.Error, (byte)RegistrationErrorCode.UnsupportedProtocolVersion];
                await transport.SendAsync(versionError, cancellationToken).ConfigureAwait(false);
                return;
            }

            string clientName = Encoding.UTF8.GetString(registrationData.AsSpan(2));

            if (clientName.Length > Protocol.MaxClientNameLength)
            {
                byte[] nameTooLongError =
                    [(byte)MessageType.Error, (byte)RegistrationErrorCode.ClientNameTooLong];
                await transport.SendAsync(nameTooLongError, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (!_clientNames.TryAdd(clientName, clientId))
            {
                byte[] errorPayload = [(byte)MessageType.Error, (byte)RegistrationErrorCode.DuplicateClientName];
                await transport.SendAsync(errorPayload, cancellationToken).ConfigureAwait(false);
                return;
            }

            connection = new ClientConnection(clientId, clientName, transport);
            _clients.TryAdd(clientId, connection);

            var responsePayload = new byte[17];
            responsePayload[0] = (byte)MessageType.RegistrationComplete;
            clientId.TryWriteBytes(responsePayload.AsSpan(1));
            await transport.SendAsync(responsePayload, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation("Client {ClientId} ({ClientName}) connected", clientId, clientName);

            while (!cancellationToken.IsCancellationRequested)
            {
                byte[]? data = await transport.ReceiveAsync(cancellationToken).ConfigureAwait(false);

                if (data is null)
                {
                    break;
                }

                if (data.Length >= 17
                    && (MessageType)data[0] == MessageType.SendMessage)
                {
                    var recipientId = new Guid(data.AsSpan(1, 16));
                    ReadOnlyMemory<byte> messageData = data.AsMemory(17);

                    await RouteMessageAsync(clientId, recipientId, messageData, cancellationToken)
                        .ConfigureAwait(false);
                }
                else if (data.Length >= 2
                    && (MessageType)data[0] == MessageType.ClientLookupRequest)
                {
                    string lookupName = Encoding.UTF8.GetString(data.AsSpan(1));

                    byte[] lookupResponse;
                    if (_clientNames.TryGetValue(lookupName, out Guid foundId)
                        && _clients.TryGetValue(foundId, out ClientConnection? found))
                    {
                        lookupResponse = new byte[18];
                        lookupResponse[0] = (byte)MessageType.ClientLookupResponse;
                        lookupResponse[1] = 0x01;
                        found.Id.TryWriteBytes(lookupResponse.AsSpan(2));
                    }
                    else
                    {
                        lookupResponse = [(byte)MessageType.ClientLookupResponse, 0x00];
                    }

                    await transport.SendAsync(lookupResponse, cancellationToken).ConfigureAwait(false);
                }
                else if ((MessageType)data[0] == MessageType.Disconnect)
                {
                    _logger.LogDebug("Client {ClientId} sent disconnect", clientId);
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when the cancellation token is triggered during shutdown.
        }
        catch (IOException ex)
        {
            _logger.LogWarning(ex, "Client {ClientId} transport error", clientId);
        }
        finally
        {
            if (connection is not null)
            {
                _clientNames.TryRemove(connection.Name, out _);
                _clients.TryRemove(clientId, out _);
                await connection.DisposeAsync().ConfigureAwait(false);
                _logger.LogInformation("Client {ClientId} disconnected", clientId);
            }
            else
            {
                await transport.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async Task RouteMessageAsync(
        Guid senderId,
        Guid recipientId,
        ReadOnlyMemory<byte> messageData,
        CancellationToken cancellationToken)
    {
        if (!_clients.TryGetValue(recipientId, out ClientConnection? recipient))
        {
            _logger.LogDebug(
                "Message from {SenderId} dropped: recipient {RecipientId} not found",
                senderId,
                recipientId);
            return;
        }

        int payloadSize = 1 + 16 + messageData.Length;
        byte[] deliveryPayload = ArrayPool<byte>.Shared.Rent(payloadSize);
        try
        {
            deliveryPayload[0] = (byte)MessageType.DeliverMessage;
            senderId.TryWriteBytes(deliveryPayload.AsSpan(1));
            messageData.CopyTo(deliveryPayload.AsMemory(17));

            await recipient.Transport
                .SendAsync(deliveryPayload.AsMemory(0, payloadSize), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException)
        {
            _logger.LogWarning(
                ex,
                "Delivery to {RecipientId} failed, evicting recipient",
                recipientId);
            _clients.TryRemove(recipientId, out _);
            _clientNames.TryRemove(recipient.Name, out _);
            await recipient.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(deliveryPayload);
        }
    }

    private sealed class ClientConnection(Guid id, string name, ITransport transport) : IAsyncDisposable
    {
        private int _disposed;

        public Guid Id { get; } = id;
        public string Name { get; } = name;
        public ITransport Transport { get; } = transport;

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                await Transport.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
