using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;

public sealed class TcpTransportTlsTests
{
    /// <summary>
    /// When a listener is configured with TLS options, a client that completes the handshake exchanges
    /// payloads normally in both directions and both ends report the connection as encrypted.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ServerTls_ClientTrustsCertificate_ExchangesPayloadsBothWays()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = serverCertificate });

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = TcpTransport.ConnectAsync(
                "localhost",
                port,
                new SslClientAuthenticationOptions
                {
                    RemoteCertificateValidationCallback = TestCertificates.PinnedTo(serverCertificate),
                });
            var acceptTask = listener.AcceptAsync();

            await using TcpTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using ITransport serverTransport = await acceptTask.ConfigureAwait(false);

            Assert.True(clientTransport.IsEncrypted);
            Assert.True(Assert.IsType<TcpTransport>(serverTransport).IsEncrypted);

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
    /// When the server presents a certificate the client does not trust, the client's connect fails with
    /// an AuthenticationException rather than silently proceeding in the clear.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ServerTls_ClientRejectsCertificate_ConnectThrowsAuthenticationException()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = serverCertificate });

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            await Assert.ThrowsAsync<AuthenticationException>(
                () => TcpTransport.ConnectAsync(
                    "localhost",
                    port,
                    new SslClientAuthenticationOptions
                    {
                        RemoteCertificateValidationCallback = (_, _, _, _) => false,
                    }));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When the listener requires and pins a client certificate, a client presenting it is accepted and
    /// the server sees the expected client certificate — mutual TLS end to end.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task MutualTls_ClientPresentsTrustedCertificate_ConnectionIsAccepted()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned("localhost");
        using X509Certificate2 clientCertificate = TestCertificates.CreateSelfSigned("mesh-client");

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions
            {
                ServerCertificate = serverCertificate,
                ClientCertificateRequired = true,
                RemoteCertificateValidationCallback = TestCertificates.PinnedTo(clientCertificate),
            });

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = TcpTransport.ConnectAsync(
                "localhost",
                port,
                new SslClientAuthenticationOptions
                {
                    ClientCertificates = [clientCertificate],
                    RemoteCertificateValidationCallback = TestCertificates.PinnedTo(serverCertificate),
                });
            var acceptTask = listener.AcceptAsync();

            await using TcpTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using ITransport serverTransport = await acceptTask.ConfigureAwait(false);

            var payload = new byte[] { 1, 2, 3, 4 };
            await clientTransport.SendAsync(payload).ConfigureAwait(false);
            Assert.Equal(payload, await serverTransport.ReceiveAsync().ConfigureAwait(false));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When the listener requires a client certificate and the client presents one it does not trust, the
    /// connection is refused: the listener never surfaces it from AcceptAsync.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task MutualTls_ClientCertificateUntrusted_ConnectionIsNeverAccepted()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned("localhost");
        using X509Certificate2 trustedClient = TestCertificates.CreateSelfSigned("mesh-client");
        using X509Certificate2 untrustedClient = TestCertificates.CreateSelfSigned("impostor");

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions
            {
                ServerCertificate = serverCertificate,
                ClientCertificateRequired = true,
                RemoteCertificateValidationCallback = TestCertificates.PinnedTo(trustedClient),
            });

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            using var acceptCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            Task<ITransport> acceptTask = listener.AcceptAsync(acceptCts.Token);

            // The handshake fails on one side or the other depending on when the server's rejection
            // reaches the client, so the client's connect may succeed or throw. Either way the point of
            // the test is that the listener never hands the connection to the hub.
            try
            {
                await using TcpTransport clientTransport = await TcpTransport.ConnectAsync(
                    "localhost",
                    port,
                    new SslClientAuthenticationOptions
                    {
                        ClientCertificates = [untrustedClient],
                        RemoteCertificateValidationCallback = TestCertificates.PinnedTo(serverCertificate),
                    }).ConfigureAwait(false);
            }
            catch (AuthenticationException)
            {
            }
            catch (IOException)
            {
            }

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => acceptTask);
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When a peer opens a TCP connection to a TLS listener and never starts a handshake, the listener
    /// abandons it at the handshake timeout instead of holding the slot, and goes on to serve a genuine
    /// client. This is the head-of-line blocking guard.
    /// </summary>
    [Fact(Timeout = 20000)]
    public async Task ServerTls_PeerNeverHandshakes_AbandonedAtTimeoutAndLaterClientStillAccepted()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = serverCertificate },
            tlsHandshakeTimeout: TimeSpan.FromMilliseconds(300));

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            // A bare socket that connects and then says nothing: exactly the peer that would stall an
            // accept-path handshake.
            using var silent = new System.Net.Sockets.TcpClient();
            await silent.ConnectAsync(IPAddress.Loopback, port).ConfigureAwait(false);

            var connectTask = TcpTransport.ConnectAsync(
                "localhost",
                port,
                new SslClientAuthenticationOptions
                {
                    RemoteCertificateValidationCallback = TestCertificates.PinnedTo(serverCertificate),
                });

            await using TcpTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using ITransport serverTransport = await listener.AcceptAsync().ConfigureAwait(false);

            var payload = new byte[] { 7, 7, 7 };
            await clientTransport.SendAsync(payload).ConfigureAwait(false);
            Assert.Equal(payload, await serverTransport.ReceiveAsync().ConfigureAwait(false));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When a cleartext client connects to a TLS listener, its frames are not mistaken for a handshake:
    /// the listener refuses it rather than admitting an unencrypted peer.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ServerTls_CleartextClient_IsNotAccepted()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = serverCertificate },
            tlsHandshakeTimeout: TimeSpan.FromMilliseconds(300));

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            await using TcpTransport cleartext =
                await TcpTransport.ConnectAsync("127.0.0.1", port).ConfigureAwait(false);
            await cleartext.SendAsync(new byte[] { 1, 2, 3 }).ConfigureAwait(false);

            using var acceptCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => listener.AcceptAsync(acceptCts.Token));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When TLS options are supplied without any means of producing a server certificate, the listener
    /// refuses to be constructed rather than failing every handshake at run time.
    /// </summary>
    [Fact]
    public void Constructor_TlsOptionsWithoutCertificate_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(
            () => new TcpTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0),
                new SslServerAuthenticationOptions()));
    }

    /// <summary>
    /// When a non-positive handshake timeout or concurrency limit is supplied, the listener throws.
    /// </summary>
    [Theory]
    [InlineData(0, 1)]
    [InlineData(-1, 1)]
    [InlineData(1, 0)]
    [InlineData(1, -1)]
    public void Constructor_NonPositiveTlsBounds_ThrowsArgumentOutOfRangeException(
        int timeoutSeconds,
        int maxHandshakes)
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new TcpTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0),
                new SslServerAuthenticationOptions { ServerCertificate = serverCertificate },
                TimeSpan.FromSeconds(timeoutSeconds),
                maxHandshakes));
    }

    /// <summary>
    /// When ConnectAsync is given null TLS options it throws rather than quietly falling back to a
    /// cleartext connection.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_NullTlsOptions_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => TcpTransport.ConnectAsync("localhost", 1, tlsOptions: null!));
    }

    /// <summary>
    /// When the caller leaves TargetHost unset, the transport defaults it to the host being dialled and
    /// does not mutate the caller's options object.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ConnectAsync_TargetHostUnset_DefaultsToHostWithoutMutatingCallerOptions()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned("localhost");

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = serverCertificate });

        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            // The certificate is issued to "localhost". If TargetHost were left unset the platform would
            // have nothing to match the subject against and would report a name mismatch, so the absence
            // of that flag is the evidence that the host was defaulted through.
            SslPolicyErrors observedErrors = SslPolicyErrors.None;
            RemoteCertificateValidationCallback pinned = TestCertificates.PinnedTo(serverCertificate);
            var clientOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (sender, certificate, chain, errors) =>
                {
                    observedErrors = errors;
                    return pinned(sender, certificate, chain, errors);
                },
            };

            var connectTask = TcpTransport.ConnectAsync("localhost", port, clientOptions);
            var acceptTask = listener.AcceptAsync();

            await using TcpTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using ITransport serverTransport = await acceptTask.ConfigureAwait(false);

            Assert.False(observedErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch));
            Assert.Null(clientOptions.TargetHost);
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When a TLS listener is disposed while a client is waiting in AcceptAsync, the wait ends with an
    /// ObjectDisposedException rather than hanging, so a hub's accept loop terminates.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task DisposeAsync_TlsListenerWithPendingAccept_AcceptThrowsObjectDisposedException()
    {
        using X509Certificate2 serverCertificate = TestCertificates.CreateSelfSigned();

        var listener = new TcpTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = serverCertificate });

        await listener.StartAsync().ConfigureAwait(false);

        Task<ITransport> acceptTask = listener.AcceptAsync();

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => acceptTask);
    }

    /// <summary>
    /// A cleartext transport reports itself as unencrypted, so a deployment can assert that it really is
    /// running TLS.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task IsEncrypted_CleartextTransport_ReturnsFalse()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync().ConfigureAwait(false);
        int port = ((IPEndPoint)listener.LocalEndPoint!).Port;

        try
        {
            var connectTask = TcpTransport.ConnectAsync("127.0.0.1", port);
            var acceptTask = listener.AcceptAsync();

            await using TcpTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using ITransport serverTransport = await acceptTask.ConfigureAwait(false);

            Assert.False(clientTransport.IsEncrypted);
            Assert.False(Assert.IsType<TcpTransport>(serverTransport).IsEncrypted);
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }
}
