using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Unix;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Unix;

public sealed class UnixSocketTransportLoopbackTests
{
    /// <summary>
    /// When a client connects to a listener over a Unix domain socket and sends a message, the accepted
    /// transport receives the same payload, and vice versa — confirming bidirectional communication over
    /// the shared length-prefixed framing.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task ConnectAndAccept_SendBothWays_PayloadsRoundTrip()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        try
        {
            var connectTask = UnixSocketTransport.ConnectAsync(path);
            var acceptTask = listener.AcceptAsync();

            await using UnixSocketTransport clientTransport = await connectTask.ConfigureAwait(false);
            await using var serverTransport = await acceptTask.ConfigureAwait(false);

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
    /// When SendAsync is called with a payload larger than the maximum frame size, an ArgumentException
    /// is thrown up front rather than emitting a frame the peer would reject.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_PayloadExceedsMaxSize_ThrowsArgumentException()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        try
        {
            var connectTask = UnixSocketTransport.ConnectAsync(path);
            var acceptTask = listener.AcceptAsync();

            await using UnixSocketTransport clientTransport = await connectTask.ConfigureAwait(false);
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
    /// When the remote peer disposes its transport, ReceiveAsync returns null to signal disconnection,
    /// matching the TCP transport's contract for a cleanly closed connection.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_RemoteDisposes_ReturnsNull()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        try
        {
            var connectTask = UnixSocketTransport.ConnectAsync(path);
            var acceptTask = listener.AcceptAsync();

            UnixSocketTransport clientTransport = await connectTask.ConfigureAwait(false);
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
    /// The batched SendAsync (IBatchSendTransport) delivers each payload as its own length-prefixed
    /// frame, in order, and issues a single underlying write for the whole batch — exactly the same
    /// behaviour TcpTransport gets from the same shared StreamFramer helper.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_Batch_DeliversEachPayloadAsIndividualFrameInOrder()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        try
        {
            var connectTask = UnixSocketTransport.ConnectAsync(path);
            var acceptTask = listener.AcceptAsync();

            await using UnixSocketTransport clientTransport = await connectTask.ConfigureAwait(false);
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
    /// Connecting to a path with nothing listening fails rather than hanging.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task ConnectAsync_NothingListening_Throws()
    {
        string path = TempSocketPath.Create();

        await Assert.ThrowsAnyAsync<Exception>(() => UnixSocketTransport.ConnectAsync(path));
    }
}
