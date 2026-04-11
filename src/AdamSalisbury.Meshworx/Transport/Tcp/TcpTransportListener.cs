using System.Net;
using System.Net.Sockets;

namespace AdamSalisbury.Meshworx.Transport.Tcp;

/// <summary>
/// An <see cref="ITransportListener"/> implementation that accepts incoming TCP connections.
/// </summary>
public sealed class TcpTransportListener : ITransportListener
{
    private readonly IPEndPoint _endPoint;
    private TcpListener? _listener;

    internal EndPoint? LocalEndPoint => _listener?.LocalEndpoint;

    public TcpTransportListener(IPEndPoint endPoint)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        _endPoint = endPoint;
    }

    public TcpTransportListener(int port)
        : this(new IPEndPoint(IPAddress.Any, port))
    {
    }

    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_listener is not null)
        {
            throw new InvalidOperationException("The listener is already running.");
        }

        _listener = new TcpListener(_endPoint);
        _listener.Start();

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task<ITransport> AcceptAsync(CancellationToken cancellationToken = default)
    {
        if (_listener is null)
        {
            throw new InvalidOperationException("The listener has not been started.");
        }

        TcpClient tcpClient = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
        tcpClient.NoDelay = true;
        return new TcpTransport(tcpClient);
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        _listener?.Stop();
        _listener = null;

        return ValueTask.CompletedTask;
    }
}
