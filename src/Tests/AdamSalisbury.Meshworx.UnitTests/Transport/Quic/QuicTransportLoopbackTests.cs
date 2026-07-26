using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Quic;
using AdamSalisbury.Meshworx.Transport.Tcp;
using AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Quic;

/// <summary>
/// Loopback tests requiring a real QUIC handshake, skipped as a no-op wherever
/// <see cref="QuicListener.IsSupported"/>/<see cref="QuicConnection.IsSupported"/> is
/// <see langword="false"/> — typically meaning the native msquic library is not installed, or the
/// platform's TLS stack does not support TLS 1.3.
/// </summary>
public sealed class QuicTransportLoopbackTests
{
    private static bool IsQuicSupported => QuicListener.IsSupported && QuicConnection.IsSupported;

    private static (QuicTransportListener Listener, X509Certificate2 Certificate) CreateListener()
    {
        X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        var listener = new QuicTransportListener(
            new IPEndPoint(IPAddress.Loopback, 0),
            new SslServerAuthenticationOptions { ServerCertificate = certificate });
        return (listener, certificate);
    }

    /// <summary>
    /// When a client connects to a listener and sends a message, the accepted transport receives the
    /// same payload, and vice versa — confirming bidirectional communication over a real QUIC connection
    /// and stream.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ConnectAndAccept_SendBothWays_PayloadsRoundTrip()
    {
        if (!IsQuicSupported)
        {
            return;
        }

        (QuicTransportListener listener, X509Certificate2 certificate) = CreateListener();
        using (certificate)
        {
            await listener.StartAsync().ConfigureAwait(false);
            int port = GetPort(listener);

            try
            {
                await using QuicTransport clientTransport = await QuicTransport.ConnectAsync(
                    "127.0.0.1",
                    port,
                    new SslClientAuthenticationOptions
                    {
                        RemoteCertificateValidationCallback = TestCertificates.PinnedTo(certificate),
                    }).ConfigureAwait(false);

                // A QUIC stream does not exist to the peer until data (or a FIN) actually arrives on
                // it — opening it is a purely local operation — so the server's AcceptInboundStreamAsync
                // (behind AcceptAsync) cannot complete until the client has sent something. Send before
                // accepting rather than after, matching how MeshClient.ConnectAsync always sends the
                // registration frame immediately once it has a transport.
                var fromClient = new byte[] { 10, 20, 30 };
                await clientTransport.SendAsync(fromClient).ConfigureAwait(false);

                await using var serverTransport = await listener.AcceptAsync().ConfigureAwait(false);
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
    }

    /// <summary>
    /// The accepted transport reports the client's real remote endpoint, so it participates in the
    /// hub's per-remote-endpoint connection cap exactly as the TCP and WebSocket transports do.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task Accept_ReportsClientRemoteEndPoint()
    {
        if (!IsQuicSupported)
        {
            return;
        }

        (QuicTransportListener listener, X509Certificate2 certificate) = CreateListener();
        using (certificate)
        {
            await listener.StartAsync().ConfigureAwait(false);
            int port = GetPort(listener);

            try
            {
                await using QuicTransport clientTransport = await QuicTransport.ConnectAsync(
                    "127.0.0.1",
                    port,
                    new SslClientAuthenticationOptions
                    {
                        RemoteCertificateValidationCallback = TestCertificates.PinnedTo(certificate),
                    }).ConfigureAwait(false);

                // See ConnectAndAccept_SendBothWays_PayloadsRoundTrip for why the send must happen
                // before the accept: a QUIC stream is not visible to the peer until data arrives on it.
                await clientTransport.SendAsync(new byte[] { 1 }).ConfigureAwait(false);

                await using var serverTransport = await listener.AcceptAsync().ConfigureAwait(false);

                var remoteEndPointTransport = Assert.IsAssignableFrom<IRemoteEndPointTransport>(serverTransport);
                var remoteEndPoint = Assert.IsType<IPEndPoint>(remoteEndPointTransport.RemoteEndPoint);
                Assert.Equal(IPAddress.Loopback, remoteEndPoint.Address);
            }
            finally
            {
                await listener.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// When SendAsync is called with a payload larger than the maximum frame size, an ArgumentException
    /// is thrown up front rather than emitting a frame the peer would reject.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task SendAsync_PayloadExceedsMaxSize_ThrowsArgumentException()
    {
        if (!IsQuicSupported)
        {
            return;
        }

        (QuicTransportListener listener, X509Certificate2 certificate) = CreateListener();
        using (certificate)
        {
            await listener.StartAsync().ConfigureAwait(false);
            int port = GetPort(listener);

            try
            {
                // The oversized-payload check happens entirely client-side, before anything reaches
                // the wire, so this does not need — and deliberately does not wait for — a server-side
                // accept.
                await using QuicTransport clientTransport = await QuicTransport.ConnectAsync(
                    "127.0.0.1",
                    port,
                    new SslClientAuthenticationOptions
                    {
                        RemoteCertificateValidationCallback = TestCertificates.PinnedTo(certificate),
                    }).ConfigureAwait(false);

                var oversized = new byte[(1024 * 1024) + 1];
                await Assert.ThrowsAsync<ArgumentException>(() => clientTransport.SendAsync(oversized));
            }
            finally
            {
                await listener.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// The batched SendAsync (IBatchSendTransport) delivers each payload as its own length-prefixed
    /// frame, in order, and issues a single underlying write for the whole batch — the same behaviour
    /// TcpTransport gets from the same shared StreamFramer helper.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task SendAsync_Batch_DeliversEachPayloadAsIndividualFrameInOrder()
    {
        if (!IsQuicSupported)
        {
            return;
        }

        (QuicTransportListener listener, X509Certificate2 certificate) = CreateListener();
        using (certificate)
        {
            await listener.StartAsync().ConfigureAwait(false);
            int port = GetPort(listener);

            try
            {
                await using QuicTransport clientTransport = await QuicTransport.ConnectAsync(
                    "127.0.0.1",
                    port,
                    new SslClientAuthenticationOptions
                    {
                        RemoteCertificateValidationCallback = TestCertificates.PinnedTo(certificate),
                    }).ConfigureAwait(false);

                var batchTransport = Assert.IsAssignableFrom<IBatchSendTransport>(clientTransport);

                // Sent before the accept: a QUIC stream is not visible to the peer until data arrives
                // on it, so the server's AcceptAsync could not otherwise complete.
                byte[][] payloads = [[1, 2], [3], [4, 5, 6]];
                ReadOnlyMemory<byte>[] batch = [payloads[0], payloads[1], payloads[2]];
                await batchTransport.SendAsync(batch);

                await using var serverTransport = await listener.AcceptAsync().ConfigureAwait(false);

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
    }

    /// <summary>
    /// When the remote peer disposes its transport, ReceiveAsync returns null to signal disconnection.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task ReceiveAsync_RemoteDisposes_ReturnsNull()
    {
        if (!IsQuicSupported)
        {
            return;
        }

        (QuicTransportListener listener, X509Certificate2 certificate) = CreateListener();
        using (certificate)
        {
            await listener.StartAsync().ConfigureAwait(false);
            int port = GetPort(listener);

            try
            {
                QuicTransport clientTransport = await QuicTransport.ConnectAsync(
                    "127.0.0.1",
                    port,
                    new SslClientAuthenticationOptions
                    {
                        RemoteCertificateValidationCallback = TestCertificates.PinnedTo(certificate),
                    }).ConfigureAwait(false);

                // A throwaway message opens the stream from the peer's perspective — a QUIC stream is
                // not visible to the peer until data arrives on it — so the accept below can complete,
                // and gives the server transport something to read before the client disposes.
                await clientTransport.SendAsync(new byte[] { 1 }).ConfigureAwait(false);

                await using var serverTransport = await listener.AcceptAsync().ConfigureAwait(false);
                Assert.Equal(new byte[] { 1 }, await serverTransport.ReceiveAsync().ConfigureAwait(false));

                await clientTransport.DisposeAsync().ConfigureAwait(false);

                byte[]? received = await serverTransport.ReceiveAsync().ConfigureAwait(false);
                Assert.Null(received);
            }
            finally
            {
                await listener.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// A peer that completes the QUIC handshake but never opens a stream has its negotiation slot
    /// reclaimed once <c>streamOpenTimeout</c> elapses, and the listener goes on to serve a genuine
    /// client afterwards — rather than that slot being held indefinitely.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="TcpTransportListener"/>'s TLS handshake pump, this listener cannot tell a
    /// connection that will eventually open a stream apart from one that never will before actually
    /// waiting for it, so it cannot guarantee a genuine client is served <i>promptly</i> while a flood of
    /// silent peers is still occupying every slot — see the constructor's <c>maxConcurrentNegotiations</c>
    /// doc for why. What it does guarantee, and what this test proves, is that a silent peer's slot is
    /// not held forever: this test deliberately waits past <c>streamOpenTimeout</c> before connecting the
    /// genuine client, with <c>maxConcurrentNegotiations</c> set to exactly one, so the genuine
    /// connection can only succeed if the silent peer's slot was actually reclaimed.
    /// </remarks>
    [Fact(Timeout = 20000)]
    public async Task StreamOpenTimeout_SilentPeerAbandoned_SlotReclaimedForLaterClient()
    {
        if (!IsQuicSupported)
        {
            return;
        }

        X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        using (certificate)
        {
            var listener = new QuicTransportListener(
                new IPEndPoint(IPAddress.Loopback, 0),
                new SslServerAuthenticationOptions { ServerCertificate = certificate },
                streamOpenTimeout: TimeSpan.FromMilliseconds(500),
                maxConcurrentNegotiations: 1);

            await listener.StartAsync().ConfigureAwait(false);
            int port = GetPort(listener);

            // Windows/Linux/macOS-only APIs from here on: guarded at run time by the IsQuicSupported
            // check at the top of this test.
#pragma warning disable CA1416
            var silentOptions = new QuicClientConnectionOptions
            {
                RemoteEndPoint = new IPEndPoint(IPAddress.Loopback, port),
                ClientAuthenticationOptions = new SslClientAuthenticationOptions
                {
                    ApplicationProtocols = [QuicTransport.DefaultApplicationProtocol],
                    RemoteCertificateValidationCallback = TestCertificates.PinnedTo(certificate),
                },
                DefaultStreamErrorCode = 0,
                DefaultCloseErrorCode = 0,
            };

            try
            {
                // Completes the handshake and consumes the listener's one and only negotiation slot,
                // but never opens a stream.
                await using QuicConnection silentConnection =
                    await QuicConnection.ConnectAsync(silentOptions).ConfigureAwait(false);
#pragma warning restore CA1416

                // Comfortably past streamOpenTimeout, so the slot is certainly free again by the time
                // the genuine client below connects — the point being to prove reclamation, not to race
                // it.
                await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));

                await using QuicTransport clientTransport = await QuicTransport.ConnectAsync(
                    "127.0.0.1",
                    port,
                    new SslClientAuthenticationOptions
                    {
                        RemoteCertificateValidationCallback = TestCertificates.PinnedTo(certificate),
                    },
                    deadline.Token).ConfigureAwait(false);

                var payload = new byte[] { 9, 9, 9 };
                await clientTransport.SendAsync(payload, deadline.Token).ConfigureAwait(false);

                await using var serverTransport = await listener.AcceptAsync(deadline.Token).ConfigureAwait(false);
                Assert.Equal(payload, await serverTransport.ReceiveAsync(deadline.Token).ConfigureAwait(false));
            }
            finally
            {
                await listener.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private static int GetPort(QuicTransportListener listener)
    {
        return ((IPEndPoint)listener.LocalEndPoint!).Port;
    }
}
