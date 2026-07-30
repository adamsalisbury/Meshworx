using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.InMemory;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.InMemory;

public sealed class InMemoryTransportTests
{
    // InMemoryTransport

    /// <summary>
    /// When a message is sent on one endpoint of a pair, the other endpoint receives the same bytes.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendThenReceive_RoundTripsInBothDirections()
    {
        (InMemoryTransport first, InMemoryTransport second) = InMemoryTransport.CreatePair();

        await first.SendAsync(new byte[] { 1, 2, 3 });
        byte[]? receivedBySecond = await second.ReceiveAsync();

        await second.SendAsync(new byte[] { 4, 5 });
        byte[]? receivedByFirst = await first.ReceiveAsync();

        Assert.Equal(new byte[] { 1, 2, 3 }, receivedBySecond);
        Assert.Equal(new byte[] { 4, 5 }, receivedByFirst);
    }

    /// <summary>
    /// When a payload is mutated after being sent, the received copy is unaffected.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_CopiesPayload()
    {
        (InMemoryTransport first, InMemoryTransport second) = InMemoryTransport.CreatePair();

        var buffer = new byte[] { 1, 2, 3 };
        await first.SendAsync(buffer);
        buffer[0] = 99;

        byte[]? received = await second.ReceiveAsync();

        Assert.Equal(new byte[] { 1, 2, 3 }, received);
    }

    /// <summary>
    /// When the peer endpoint is disposed, ReceiveAsync returns null to signal a closed connection.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_PeerDisposed_ReturnsNull()
    {
        (InMemoryTransport first, InMemoryTransport second) = InMemoryTransport.CreatePair();

        await first.DisposeAsync();
        byte[]? received = await second.ReceiveAsync();

        Assert.Null(received);
    }

    /// <summary>
    /// When the peer is disposed after queueing a message, the buffered message is still received
    /// before null is returned.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_PeerDisposedAfterSend_DrainsBufferedMessageThenReturnsNull()
    {
        (InMemoryTransport first, InMemoryTransport second) = InMemoryTransport.CreatePair();

        await first.SendAsync(new byte[] { 7 });
        await first.DisposeAsync();

        Assert.Equal(new byte[] { 7 }, await second.ReceiveAsync());
        Assert.Null(await second.ReceiveAsync());
    }

    /// <summary>
    /// When SendAsync is called after the transport has been disposed, an ObjectDisposedException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        (InMemoryTransport first, _) = InMemoryTransport.CreatePair();

        await first.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => first.SendAsync(new byte[] { 1 }));
    }

    /// <summary>
    /// When the cancellation token is cancelled while ReceiveAsync is waiting, an OperationCanceledException
    /// is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_Cancelled_ThrowsOperationCanceledException()
    {
        (InMemoryTransport first, _) = InMemoryTransport.CreatePair();
        using var cts = new CancellationTokenSource();

        Task<byte[]?> receiveTask = first.ReceiveAsync(cts.Token);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => receiveTask);
    }

    // InMemoryTransportListener

    /// <summary>
    /// When StartAsync is called on a listener that is already running, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Listener_StartAsync_AlreadyRunning_Throws()
    {
        await using var listener = new InMemoryTransportListener();
        await listener.StartAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
    }

    /// <summary>
    /// When Connect is called before the listener is started, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Listener_Connect_NotStarted_Throws()
    {
        await using var listener = new InMemoryTransportListener();

        Assert.Throws<InvalidOperationException>(() => listener.Connect());
    }

    /// <summary>
    /// When AcceptAsync is called before the listener is started, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Listener_AcceptAsync_NotStarted_Throws()
    {
        await using var listener = new InMemoryTransportListener();

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// When a client connects, AcceptAsync returns the paired server endpoint, and messages flow between them.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Listener_ConnectAndAccept_PairsEndpoints()
    {
        await using var listener = new InMemoryTransportListener();
        await listener.StartAsync();

        ITransport client = listener.Connect();
        ITransport server = await listener.AcceptAsync();

        await client.SendAsync(new byte[] { 1, 2 });
        Assert.Equal(new byte[] { 1, 2 }, await server.ReceiveAsync());

        await server.SendAsync(new byte[] { 3 });
        Assert.Equal(new byte[] { 3 }, await client.ReceiveAsync());
    }

    /// <summary>
    /// When AcceptAsync is waiting and the listener is disposed, an ObjectDisposedException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task Listener_AcceptAsync_Disposed_Throws()
    {
        var listener = new InMemoryTransportListener();
        await listener.StartAsync();

        Task<ITransport> acceptTask = listener.AcceptAsync();
        await listener.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => acceptTask);
    }

    /// <summary>
    /// A payload over the frame cap is rejected, exactly as every stream transport rejects it.
    /// </summary>
    /// <remarks>
    /// This type stands in for the real transports across the hub's own test suite, so a cap it does not
    /// enforce is a defect the suite cannot see. A near-maximum fan-out builds a delivery frame sixteen
    /// bytes larger than the inbound one that produced it, and without this check that frame was
    /// delivered happily in-process while disconnecting every recipient over TCP.
    /// </remarks>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_PayloadOverTheFrameCap_Throws()
    {
        (InMemoryTransport first, InMemoryTransport _) = InMemoryTransport.CreatePair();

        await Assert.ThrowsAsync<ArgumentException>(
            () => first.SendAsync(new byte[(1024 * 1024) + 1]));
    }

    /// <summary>
    /// A payload at exactly the cap is accepted, so the check bounds the frame rather than shrinking it.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_PayloadAtTheFrameCap_IsDelivered()
    {
        (InMemoryTransport first, InMemoryTransport second) = InMemoryTransport.CreatePair();

        await first.SendAsync(new byte[1024 * 1024]);

        byte[]? received = await second.ReceiveAsync();

        Assert.Equal(1024 * 1024, received!.Length);
    }

    /// <summary>
    /// Receiving on a disposed endpoint throws rather than awaiting a channel nothing will complete.
    /// SendAsync has always been guarded; without the same guard here the type was asymmetric with
    /// itself as well as with every stream transport.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_AfterOwnDispose_ThrowsObjectDisposedException()
    {
        (InMemoryTransport first, InMemoryTransport _) = InMemoryTransport.CreatePair();

        await first.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => first.ReceiveAsync());
    }

    /// <summary>
    /// Sending to a departed peer fails rather than reporting success for ever into a channel with no
    /// reader, which is what a stream transport surfaces for a closed connection.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_AfterPeerDisposed_ThrowsIOException()
    {
        (InMemoryTransport first, InMemoryTransport second) = InMemoryTransport.CreatePair();

        await second.DisposeAsync();

        await Assert.ThrowsAsync<IOException>(() => first.SendAsync(new byte[] { 1 }));
    }
}
