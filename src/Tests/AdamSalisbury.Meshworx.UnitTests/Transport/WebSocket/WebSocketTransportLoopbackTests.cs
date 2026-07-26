using System.Net;
using System.Net.Security;
using System.Net.WebSockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using AdamSalisbury.Meshworx.Transport;
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
    /// The batched SendAsync (IBatchSendTransport) delivers each payload as its own message, in order,
    /// exactly as a sequence of individual SendAsync calls would.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task SendAsync_Batch_DeliversEachPayloadAsIndividualMessageInOrder()
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

            var batchTransport = Assert.IsAssignableFrom<IBatchSendTransport>(clientTransport);

            byte[][] payloads = [[1, 2], [3], [4, 5, 6]];
            ReadOnlyMemory<byte>[] batch = [payloads[0], payloads[1], payloads[2]];
            await batchTransport.SendAsync(batch);

            foreach (byte[] expected in payloads)
            {
                Assert.Equal(expected, await serverTransport.ReceiveAsync().ConfigureAwait(false));
            }
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When a batch contains a valid payload ahead of an oversize one, the valid payload is still
    /// delivered before the batched send throws — matching the single-send path's deliver-then-fault
    /// behaviour rather than discarding the whole batch.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task SendAsync_Batch_OversizePayloadAfterValid_SendsValidPrefixThenThrows()
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

            var batchTransport = Assert.IsAssignableFrom<IBatchSendTransport>(clientTransport);

            var valid = new byte[] { 1, 2, 3 };
            var oversize = new byte[(1024 * 1024) + 1];
            ReadOnlyMemory<byte>[] batch = [valid, oversize];

            await Assert.ThrowsAsync<ArgumentException>(async () => await batchTransport.SendAsync(batch));

            Assert.Equal(valid, await serverTransport.ReceiveAsync().ConfigureAwait(false));
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
    /// When far more peers connect and stay silent than there are negotiation slots, a genuine client
    /// still connects promptly. Silent peers must not be able to hold the negotiation budget, or a
    /// trivial flood would stop the listener admitting anyone — mirroring
    /// TcpTransportListener's equivalent guarantee for its TLS handshake pump.
    /// </summary>
    [Fact(Timeout = 30000)]
    public async Task ServerTls_SilentPeersOutnumberNegotiationSlots_GenuineClientStillConnects()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        const int negotiationSlots = 2;
        const int silentPeerCount = 12;

        var listener = new WebSocketTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            tlsOptions: new SslServerAuthenticationOptions { ServerCertificate = serverCertificate },
            // Generous relative to the test's own deadline: if a silent peer could hold a negotiation
            // slot, the genuine client below would not get one before the assertion times out.
            handshakeTimeout: TimeSpan.FromSeconds(20),
            maxConcurrentHandshakes: negotiationSlots);

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        var silentPeers = new List<System.Net.Sockets.TcpClient>();
        try
        {
            for (int i = 0; i < silentPeerCount; i++)
            {
                var silent = new System.Net.Sockets.TcpClient();
                silentPeers.Add(silent);
                await silent.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);
            }

            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            await using WebSocketTransport clientTransport = await WebSocketTransport.ConnectAsync(
                new Uri($"wss://localhost:{port}/"),
                options => options.RemoteCertificateValidationCallback = TestCertificates.PinnedTo(serverCertificate),
                deadline.Token).ConfigureAwait(false);

            await using var serverTransport = await listener.AcceptAsync(deadline.Token).ConfigureAwait(false);

            var payload = new byte[] { 9, 9, 9 };
            await clientTransport.SendAsync(payload, deadline.Token).ConfigureAwait(false);
            Assert.Equal(payload, await serverTransport.ReceiveAsync(deadline.Token).ConfigureAwait(false));
        }
        finally
        {
            foreach (System.Net.Sockets.TcpClient silent in silentPeers)
            {
                silent.Dispose();
            }

            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When the client rejects the server's certificate, the TLS handshake fails — surfaced by
    /// <see cref="ClientWebSocket"/> as a WebSocketException wrapping the underlying
    /// AuthenticationException — and the connection never reaches the hub.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ServerTls_ClientRejectsCertificate_ConnectThrowsWebSocketException()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new WebSocketTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            tlsOptions: new SslServerAuthenticationOptions { ServerCertificate = serverCertificate });

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            await AssertCertificateRejectedAsync(port);
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A failed TLS handshake against one peer does not stop the listener negotiating a subsequent
    /// genuine connection — the failed negotiation's slot and stream are released rather than stuck or
    /// leaked.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ServerTls_HandshakeFailure_ListenerRemainsUsableForNextClient()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new WebSocketTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            tlsOptions: new SslServerAuthenticationOptions { ServerCertificate = serverCertificate });

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            await AssertCertificateRejectedAsync(port);

            await using WebSocketTransport clientTransport = await WebSocketTransport.ConnectAsync(
                new Uri($"wss://localhost:{port}/"),
                options => options.RemoteCertificateValidationCallback = TestCertificates.PinnedTo(serverCertificate));
            await using var serverTransport = await listener.AcceptAsync().ConfigureAwait(false);

            var payload = new byte[] { 5, 5, 5 };
            await clientTransport.SendAsync(payload).ConfigureAwait(false);
            Assert.Equal(payload, await serverTransport.ReceiveAsync().ConfigureAwait(false));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When ReceiveAsync would need to buffer a message larger than the maximum frame payload, an
    /// IOException is thrown so receive loops treat it as a transport failure, matching the send-side
    /// cap enforced by SendAsync.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ReceiveAsync_PayloadExceedsMaxSize_ThrowsIOException()
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

            // The client transport itself would reject this at SendAsync, so drive the raw WebSocket
            // directly to prove the receiver's own accumulation cap — not merely the sender's check —
            // rejects an oversized message.
            System.Net.WebSockets.WebSocket rawClientSocket =
                (System.Net.WebSockets.WebSocket)typeof(WebSocketTransport)
                    .GetField("_webSocket", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                    .GetValue(clientTransport)!;

            var oversized = new byte[(1024 * 1024) + 1];
            await rawClientSocket.SendAsync(
                oversized, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None)
                .ConfigureAwait(false);

            await Assert.ThrowsAsync<IOException>(() => serverTransport.ReceiveAsync());
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

    /// <summary>
    /// Connects with a certificate validation callback that rejects everything, and asserts the failure
    /// is the WebSocketException that ClientWebSocket wraps a rejected-certificate AuthenticationException
    /// in, rather than any other connection failure.
    /// </summary>
    private static async Task AssertCertificateRejectedAsync(int port)
    {
        WebSocketException thrown = await Assert.ThrowsAsync<WebSocketException>(
            () => WebSocketTransport.ConnectAsync(
                new Uri($"wss://localhost:{port}/"),
                options => options.RemoteCertificateValidationCallback = (_, _, _, _) => false));

        Exception? innermost = thrown;
        while (innermost.InnerException is not null)
        {
            innermost = innermost.InnerException;
        }

        Assert.IsType<AuthenticationException>(innermost);
    }
}
