using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using AdamSalisbury.Meshworx.Interfaces;
using AdamSalisbury.Meshworx.Internal;
using Microsoft.Extensions.Logging;

namespace AdamSalisbury.Meshworx;

public sealed class MeshHub : IMeshHub, IAsyncDisposable
{
    private readonly ILogger<MeshHub> _logger;
    private readonly IPEndPoint _endPoint;
    private readonly ConcurrentDictionary<Guid, ClientConnection> _clients = new();
    private TcpListener? _listener;
    private CancellationTokenSource? _cts;
    private Task? _acceptLoopTask;

    public MeshHub(ILogger<MeshHub> logger, IPEndPoint endPoint)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        _logger = logger;
        _endPoint = endPoint;
    }

    public MeshHub(ILogger<MeshHub> logger, int port)
        : this(logger, new IPEndPoint(IPAddress.Any, port))
    {
    }

    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_listener is not null)
        {
            throw new InvalidOperationException("The hub is already running.");
        }

        _cts = new CancellationTokenSource();
        _listener = new TcpListener(_endPoint);
        _listener.Start();
        _acceptLoopTask = AcceptLoopAsync(_cts.Token);

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_listener is null)
        {
            return;
        }

        if (_cts is not null)
        {
            await _cts.CancelAsync().ConfigureAwait(false);
        }

        _listener.Stop();

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
            client.Dispose();
        }

        _clients.Clear();
        _listener = null;
        _cts?.Dispose();
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
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient tcpClient;
            try
            {
                tcpClient = await _listener!.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            _ = HandleClientAsync(tcpClient, cancellationToken);
        }
    }

    private async Task HandleClientAsync(TcpClient tcpClient, CancellationToken cancellationToken)
    {
        var clientId = Guid.NewGuid();
        NetworkStream stream = tcpClient.GetStream();
        var connection = new ClientConnection(clientId, tcpClient, stream);

        _clients.TryAdd(clientId, connection);
        Console.WriteLine("Client " + clientId + "connected");
        try
        {
            var idBytes = clientId.ToByteArray();
            await MeshFrameCodec.WriteFrameAsync(
                stream,
                MessageType.RegistrationComplete,
                idBytes,
                cancellationToken).ConfigureAwait(false);

            while (!cancellationToken.IsCancellationRequested)
            {
                (MessageType Type, byte[] Payload)? frame = await MeshFrameCodec.ReadFrameAsync(
                    stream,
                    cancellationToken).ConfigureAwait(false);

                if (frame is null)
                {
                    break;
                }

                if (frame.Value.Type == MessageType.SendMessage && frame.Value.Payload.Length >= 16)
                {
                    var recipientId = new Guid(frame.Value.Payload.AsSpan(0, 16));
                    ReadOnlyMemory<byte> messageData = frame.Value.Payload.AsMemory(16);

                    await RouteMessageAsync(clientId, recipientId, messageData, cancellationToken)
                        .ConfigureAwait(false);
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
            connection.Dispose();
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

        var deliveryPayload = new byte[16 + messageData.Length];
        senderId.TryWriteBytes(deliveryPayload);
        messageData.CopyTo(deliveryPayload.AsMemory(16));

        await recipient.WriteSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await MeshFrameCodec.WriteFrameAsync(
                recipient.Stream,
                MessageType.DeliverMessage,
                deliveryPayload,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            recipient.WriteSemaphore.Release();
        }
    }

    private sealed class ClientConnection : IDisposable
    {
        private readonly TcpClient _tcpClient;

        public ClientConnection(Guid id, TcpClient tcpClient, NetworkStream stream)
        {
            _tcpClient = tcpClient;

            Id = id;
            Stream = stream;
            WriteSemaphore = new SemaphoreSlim(1, 1);
        }

        public Guid Id { get; }
        public NetworkStream Stream { get; }
        public SemaphoreSlim WriteSemaphore { get; }

        public void Dispose()
        {
            WriteSemaphore.Dispose();
            Stream.Dispose();
            _tcpClient.Dispose();
        }
    }
}
