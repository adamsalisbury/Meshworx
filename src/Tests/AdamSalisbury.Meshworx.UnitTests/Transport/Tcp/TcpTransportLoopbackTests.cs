using System.Net;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;

public sealed class TcpTransportLoopbackTests
{
    /// <summary>
    /// When a client connects to a listener on loopback and sends a message, the accepted
    /// transport receives the same payload.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAndAccept_SendFromClient_ServerReceivesPayload()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = TcpTransport.ConnectAsync("127.0.0.1", port);
            var acceptTask = listener.AcceptAsync();

            await using var clientTransport = await connectTask.ConfigureAwait(false);
            await using var serverTransport = await acceptTask.ConfigureAwait(false);

            var payload = new byte[] { 10, 20, 30 };
            await clientTransport.SendAsync(payload).ConfigureAwait(false);
            byte[]? received = await serverTransport.ReceiveAsync().ConfigureAwait(false);

            Assert.NotNull(received);
            Assert.Equal(payload, received);
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When a server sends a message back through the accepted transport, the client receives
    /// the same payload, confirming bidirectional communication.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAndAccept_SendFromServer_ClientReceivesPayload()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = TcpTransport.ConnectAsync("127.0.0.1", port);
            var acceptTask = listener.AcceptAsync();

            await using var clientTransport = await connectTask.ConfigureAwait(false);
            await using var serverTransport = await acceptTask.ConfigureAwait(false);

            var payload = new byte[] { 40, 50, 60 };
            await serverTransport.SendAsync(payload).ConfigureAwait(false);
            byte[]? received = await clientTransport.ReceiveAsync().ConfigureAwait(false);

            Assert.NotNull(received);
            Assert.Equal(payload, received);
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }
}