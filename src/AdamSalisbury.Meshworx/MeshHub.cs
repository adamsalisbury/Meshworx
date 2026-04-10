using System.Collections.Concurrent;
using System.Text;
using AdamSalisbury.Meshworx.Internal;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;

namespace AdamSalisbury.Meshworx;

public sealed class MeshHub : IMeshHub, IAsyncDisposable
{
    private readonly ILogger<MeshHub> _logger;
    private readonly ITransportListener _listener;
    private readonly ConcurrentDictionary<Guid, ClientConnection> _clients = new();
    private CancellationTokenSource? _cts;
    private Task? _acceptLoopTask;

    public MeshHub(ILogger<MeshHub> logger, ITransportListener listener)
    {
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

        await _cts.CancelAsync().ConfigureAwait(false);

        if (_acceptLoopTask is not null)
        {
            try
            {
                await _acceptLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                //HUMANTODO
            }
        }

        foreach (ClientConnection client in _clients.Values)
        {
            await client.DisposeAsync().ConfigureAwait(false);
        }

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

            _ = HandleClientAsync(transport, cancellationToken);
        }
    }

    private async Task HandleClientAsync(ITransport transport, CancellationToken cancellationToken)
    {
        var clientId = Guid.NewGuid();

        byte[]? registrationData = await transport.ReceiveAsync(cancellationToken).ConfigureAwait(false);

        if (registrationData is null
            || registrationData.Length < 2
            || (MessageType)registrationData[0] != MessageType.RegistrationRequest)
        {
            await transport.DisposeAsync().ConfigureAwait(false);
            return;
        }

        string clientName = Encoding.UTF8.GetString(registrationData.AsSpan(1));

        bool nameExists = _clients.Values.Any(
            c => string.Equals(c.Name, clientName, StringComparison.Ordinal));

        if (nameExists)
        {
            byte[] errorPayload = [(byte)MessageType.Error, (byte)RegistrationErrorCode.DuplicateClientName];
            await transport.SendAsync(errorPayload, cancellationToken).ConfigureAwait(false);
            await transport.DisposeAsync().ConfigureAwait(false);
            return;
        }

        var connection = new ClientConnection(clientId, clientName, transport);

        _clients.TryAdd(clientId, connection);
        _logger.LogInformation("Client {ClientId} ({ClientName}) connected", clientId, clientName);
        try
        {
            var responsePayload = new byte[17];
            responsePayload[0] = (byte)MessageType.RegistrationComplete;
            clientId.TryWriteBytes(responsePayload.AsSpan(1));
            await transport.SendAsync(responsePayload, cancellationToken).ConfigureAwait(false);

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
                    ClientConnection? found = _clients.Values.FirstOrDefault(
                        c => string.Equals(c.Name, lookupName, StringComparison.Ordinal));

                    byte[] lookupResponse;
                    if (found is not null)
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
            }
        }
        catch (OperationCanceledException)
        {
            //HUMANTODO
        }
        catch (IOException)
        {
            //HUMANTODO
        }
        finally
        {
            _clients.TryRemove(clientId, out _);
            await connection.DisposeAsync().ConfigureAwait(false);
            _logger.LogInformation("Client {ClientId} disconnected", clientId);
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
            return;
        }

        var deliveryPayload = new byte[1 + 16 + messageData.Length];
        deliveryPayload[0] = (byte)MessageType.DeliverMessage;
        senderId.TryWriteBytes(deliveryPayload.AsSpan(1));
        messageData.CopyTo(deliveryPayload.AsMemory(17));

        await recipient.Transport.SendAsync(deliveryPayload, cancellationToken).ConfigureAwait(false);
    }

    private sealed class ClientConnection : IAsyncDisposable
    {
        public ClientConnection(Guid id, string name, ITransport transport)
        {
            Id = id;
            Name = name;
            Transport = transport;
        }

        public Guid Id { get; }
        public string Name { get; }
        public ITransport Transport { get; }

        public async ValueTask DisposeAsync()
        {
            await Transport.DisposeAsync().ConfigureAwait(false);
        }
    }
}
