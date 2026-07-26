using System.Net;
using System.Net.Security;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using AdamSalisbury.Meshworx.Transport.WebSocket;
using AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.WebSocket;

public sealed class WebSocketTransportLoopbackTests
{
    /// <summary>
    /// When a client connects to a listener over ws:// and sends a message, the accepted transport
    /// receives the same payload, and vice versa — confirming bidirectional communication over the
    /// real WebSocket handshake and framing.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ConnectAndAccept_SendBothWays_PayloadsRoundTrip()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = WebSocketTransport.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"));
            var acceptTask = listener.AcceptAsync();

            await using WebSocketTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using var serverTransport = await acceptTask.ConfigureAwait(false);

            Assert.False(clientTransport.IsEncrypted);

            var fromClient = new byte[] { 10, 20, 30 };
            await clientTransport.SendAsync(fromClient).ConfigureAwait(false);
            Assert.Equal(fromClient, await serverTransport.ReceiveAsync().ConfigureAwait(false));

            var fromServer = new byte[] { 40, 50, 60 };
            await serverTransport.SendAsync(fromServer).ConfigureAwait(false);
            Assert.Equal(fromServer, await clientTransport.ReceiveAsync().ConfigureAwait(false));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A message spanning several WebSocket frames (larger than the internal receive chunk size) is
    /// reassembled into a single payload.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ConnectAndAccept_MultiFrameMessage_ReassemblesWholePayload()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = WebSocketTransport.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"));
            var acceptTask = listener.AcceptAsync();

            await using WebSocketTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using var serverTransport = await acceptTask.ConfigureAwait(false);

            var payload = new byte[200_000];
            Random.Shared.NextBytes(payload);

            await clientTransport.SendAsync(payload).ConfigureAwait(false);
            byte[]? received = await serverTransport.ReceiveAsync().ConfigureAwait(false);

            Assert.Equal(payload, received);
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When SendAsync is called with a payload larger than the maximum frame size, an ArgumentException
    /// is thrown up front rather than emitting a message the peer would reject.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task SendAsync_PayloadExceedsMaxSize_ThrowsArgumentException()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = WebSocketTransport.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"));
            var acceptTask = listener.AcceptAsync();

            await using WebSocketTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using var serverTransport = await acceptTask.ConfigureAwait(false);

            var oversized = new byte[(1024 * 1024) + 1];
            await Assert.ThrowsAsync<ArgumentException>(() => clientTransport.SendAsync(oversized));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When the remote peer closes the WebSocket gracefully, ReceiveAsync returns null to signal
    /// disconnection, matching the TCP transport's contract for a cleanly closed connection.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ReceiveAsync_RemoteClosesGracefully_ReturnsNull()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = WebSocketTransport.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"));
            var acceptTask = listener.AcceptAsync();

            WebSocketTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using var serverTransport = await acceptTask.ConfigureAwait(false);

            await clientTransport.DisposeAsync().ConfigureAwait(false);

            byte[]? received = await serverTransport.ReceiveAsync().ConfigureAwait(false);
            Assert.Null(received);
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A connection attempt for a path the listener is not configured to upgrade on is refused rather
    /// than accepted.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ConnectAsync_WrongPath_ThrowsWebSocketException()
    {
        var listener = new WebSocketTransportListener(new IPEndPoint(IPAddress.Loopback, 0), path: "/mesh");
        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            await Assert.ThrowsAsync<WebSocketException>(
                () => WebSocketTransport.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/wrong-path")));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When a listener is configured with TLS options, a client that trusts the certificate connects
    /// over wss:// and exchanges payloads normally in both directions, and both ends report the
    /// connection as encrypted.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ServerTls_ClientTrustsCertificate_ExchangesPayloadsOverWss()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new WebSocketTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            tlsOptions: new SslServerAuthenticationOptions { ServerCertificate = serverCertificate });

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = WebSocketTransport.ConnectAsync(
                new Uri($"wss://localhost:{port}/"),
                options => options.RemoteCertificateValidationCallback = TestCertificates.PinnedTo(serverCertificate));
            var acceptTask = listener.AcceptAsync();

            await using WebSocketTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using var serverTransport = await acceptTask.ConfigureAwait(false);

            Assert.True(clientTransport.IsEncrypted);
            Assert.True(Assert.IsType<WebSocketTransport>(serverTransport).IsEncrypted);

            var payload = new byte[] { 1, 2, 3 };
            await clientTransport.SendAsync(payload).ConfigureAwait(false);
            Assert.Equal(payload, await serverTransport.ReceiveAsync().ConfigureAwait(false));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A cleartext ws:// client cannot complete the handshake against a listener configured for wss://
    /// only.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ServerTls_CleartextClient_IsNotAccepted()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new WebSocketTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            tlsOptions: new SslServerAuthenticationOptions { ServerCertificate = serverCertificate });

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            await Assert.ThrowsAnyAsync<Exception>(
                () => WebSocketTransport.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/")));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }
}
