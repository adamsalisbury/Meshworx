using System.Buffers.Binary;
using System.Reflection;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;

public sealed class TcpTransportTests
{
    // SendAsync — framing

    /// <summary>
    /// When SendAsync is called with a non-empty payload, the stream receives a 4-byte big-endian length header followed by the payload bytes.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_NonEmptyPayload_WritesLengthPrefixedFrame()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);

        await transport.SendAsync(new byte[] { 1, 2, 3 });

        byte[] written = stream.ToArray();
        Assert.Equal(7, written.Length);
        Assert.Equal(3, BinaryPrimitives.ReadInt32BigEndian(written.AsSpan(0, 4)));
        Assert.Equal(new byte[] { 1, 2, 3 }, written[4..]);
    }

    /// <summary>
    /// When SendAsync is called with an empty payload, the stream receives a 4-byte header encoding zero length and no payload bytes.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_EmptyPayload_WritesZeroLengthHeader()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);

        await transport.SendAsync(ReadOnlyMemory<byte>.Empty);

        byte[] written = stream.ToArray();
        Assert.Equal(4, written.Length);
        Assert.Equal(0, BinaryPrimitives.ReadInt32BigEndian(written));
    }

    /// <summary>
    /// When SendAsync is called with a 256-byte payload, the length header correctly encodes the value 256 in big-endian format.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_LargePayload_WritesCorrectLengthHeader()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);
        var payload = new byte[256];

        await transport.SendAsync(payload);

        byte[] written = stream.ToArray();
        Assert.Equal(260, written.Length);
        Assert.Equal(256, BinaryPrimitives.ReadInt32BigEndian(written.AsSpan(0, 4)));
    }

    /// <summary>
    /// When SendAsync is called with a payload larger than the maximum frame size, an ArgumentException
    /// is thrown up front rather than emitting a frame the peer would reject.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_PayloadExceedsMaxSize_ThrowsArgumentException()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);
        var oversized = new byte[(1024 * 1024) + 1];

        await Assert.ThrowsAsync<ArgumentException>(() => transport.SendAsync(oversized));
        Assert.Empty(stream.ToArray());
    }

    /// <summary>
    /// When SendAsync is called with a payload exactly at the maximum frame size, the frame is written
    /// successfully.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_PayloadAtMaxSize_WritesFrame()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);
        var atLimit = new byte[1024 * 1024];

        await transport.SendAsync(atLimit);

        Assert.Equal(4 + (1024 * 1024), stream.ToArray().Length);
    }

    // ReceiveAsync — framing

    /// <summary>
    /// When ReceiveAsync reads a valid length-prefixed frame from the stream, it returns the payload bytes without the header.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_ValidFrame_ReturnsPayload()
    {
        var stream = new MemoryStream();
        WriteFrame(stream, [1, 2, 3]);
        stream.Position = 0;

        var transport = new TcpTransport(stream);
        byte[]? result = await transport.ReceiveAsync();

        Assert.NotNull(result);
        Assert.Equal(new byte[] { 1, 2, 3 }, result);
    }

    /// <summary>
    /// When ReceiveAsync reads a frame with a zero-length payload, it returns an empty array rather than null.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_EmptyPayload_ReturnsEmptyArray()
    {
        var stream = new MemoryStream();
        WriteFrame(stream, []);
        stream.Position = 0;

        var transport = new TcpTransport(stream);
        byte[]? result = await transport.ReceiveAsync();

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    /// <summary>
    /// When ReceiveAsync encounters a closed stream before any header bytes can be read, it returns null to signal disconnection.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_StreamClosed_ReturnsNull()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);

        byte[]? result = await transport.ReceiveAsync();

        Assert.Null(result);
    }

    /// <summary>
    /// When the stream closes after the header is read but before the full payload is delivered, ReceiveAsync returns null.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_StreamClosedDuringPayload_ReturnsNull()
    {
        var stream = new MemoryStream();
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, 10);
        stream.Write(header);
        stream.Write(new byte[] { 1, 2, 3 }); // only 3 of the promised 10 bytes
        stream.Position = 0;

        var transport = new TcpTransport(stream);
        byte[]? result = await transport.ReceiveAsync();

        Assert.Null(result);
    }

    /// <summary>
    /// When ReceiveAsync reads a header containing a negative payload length, an IOException is thrown
    /// so receive loops treat the corrupt framing as a transport failure.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_NegativePayloadLength_ThrowsIOException()
    {
        var stream = new MemoryStream();
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, -1);
        stream.Write(header);
        stream.Position = 0;

        var transport = new TcpTransport(stream);

        await Assert.ThrowsAsync<IOException>(() => transport.ReceiveAsync());
    }

    /// <summary>
    /// When ReceiveAsync reads a header containing a payload length exceeding the maximum allowed size, an IOException is thrown
    /// so receive loops treat the corrupt framing as a transport failure.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_PayloadExceedsMaxSize_ThrowsIOException()
    {
        var stream = new MemoryStream();
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, 1_048_577); // 1MB + 1
        stream.Write(header);
        stream.Position = 0;

        var transport = new TcpTransport(stream);

        await Assert.ThrowsAsync<IOException>(() => transport.ReceiveAsync());
    }

    // Round-trip

    /// <summary>
    /// When a payload is sent and then received on the same stream, the received data matches the original payload exactly.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendThenReceive_RoundTrip_ReturnsOriginalPayload()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);
        var original = new byte[] { 10, 20, 30, 40, 50 };

        await transport.SendAsync(original);
        stream.Position = 0;
        byte[]? received = await transport.ReceiveAsync();

        Assert.Equal(original, received);
    }

    /// <summary>
    /// When multiple messages are sent sequentially, they can be received in the same order with correct payloads.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendThenReceive_MultipleMessages_ReturnsAllInOrder()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);

        await transport.SendAsync(new byte[] { 1, 2 });
        await transport.SendAsync(new byte[] { 3, 4, 5 });
        await transport.SendAsync(new byte[] { 6 });

        stream.Position = 0;

        byte[]? first = await transport.ReceiveAsync();
        byte[]? second = await transport.ReceiveAsync();
        byte[]? third = await transport.ReceiveAsync();

        Assert.Equal(new byte[] { 1, 2 }, first);
        Assert.Equal(new byte[] { 3, 4, 5 }, second);
        Assert.Equal(new byte[] { 6 }, third);
    }

    // DisposeAsync

    /// <summary>
    /// When DisposeAsync is called, the underlying stream is disposed so that further operations on it throw.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_DisposesUnderlyingStream()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);

        await transport.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => stream.ReadByte());
    }

    /// <summary>
    /// A SendAsync call queued behind the write lock when DisposeAsync runs completes rather than
    /// hanging for ever (issue #104). SemaphoreSlim.Dispose abandons a queued WaitAsync waiter without
    /// completing it, so if DisposeAsync disposed the write lock the queued send would never observe the
    /// disposal at all — not even as a thrown exception. Pinned by holding the lock to simulate an
    /// in-flight send, queuing a second send behind it, disposing the transport, then releasing the held
    /// lock and requiring the queued send to settle (however it settles) within a short bound.
    /// </summary>
    [Fact(Timeout = 2000)]
    public async Task DisposeAsync_SendQueuedOnWriteLock_CompletesRatherThanHangingForever()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);

        FieldInfo writeLockField = typeof(TcpTransport).GetField(
            "_writeLock", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var writeLock = (SemaphoreSlim)writeLockField.GetValue(transport)!;

        // Simulates another in-flight SendAsync already holding the lock.
        await writeLock.WaitAsync();

        Task queuedSend = transport.SendAsync(new byte[] { 1 });
        await Task.Delay(50);

        await transport.DisposeAsync();

        try
        {
            writeLock.Release();
        }
        catch (ObjectDisposedException)
        {
            // Expected on the unfixed code, where DisposeAsync has already disposed the write lock;
            // the queued send below is the thing actually under test.
        }

        Task settled = await Task.WhenAny(queuedSend, Task.Delay(TimeSpan.FromSeconds(1)));
        Assert.Same(queuedSend, settled);
    }

    // SendAsync — batched (IBatchSendTransport)

    /// <summary>
    /// When the batched SendAsync is called with several payloads, each is written as its own
    /// length-prefixed frame, in order, and reads back as separate messages.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_Batch_WritesEachPayloadAsIndividualFrameInOrder()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);

        byte[][] payloads = [[1, 2], [3], [4, 5, 6]];
        ReadOnlyMemory<byte>[] batch = [payloads[0], payloads[1], payloads[2]];
        await transport.SendAsync(batch);

        // Read the written bytes back through a receiving transport; each frame must round-trip
        // as a distinct message in the order it was sent.
        var readBack = new TcpTransport(new MemoryStream(stream.ToArray()));
        foreach (byte[] expected in payloads)
        {
            byte[]? received = await readBack.ReceiveAsync();
            Assert.Equal(expected, received);
        }

        Assert.Null(await readBack.ReceiveAsync());
    }

    /// <summary>
    /// The batched SendAsync issues a single write to the underlying stream for the whole batch,
    /// not one write per payload.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_Batch_WritesWholeBatchInOneWrite()
    {
        var stream = new CountingStream();
        var transport = new TcpTransport(stream);

        await transport.SendAsync(new ReadOnlyMemory<byte>[] { new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 } });

        Assert.Equal(1, stream.WriteCount);
    }

    /// <summary>
    /// When a batch contains a valid frame ahead of an oversize one, the valid frame is written before
    /// the batched send throws — matching the single-send path's deliver-then-fault behaviour rather
    /// than discarding the whole batch.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendAsync_Batch_OversizeFrameAfterValid_WritesValidPrefixThenThrows()
    {
        var stream = new MemoryStream();
        var transport = new TcpTransport(stream);

        var valid = new byte[] { 1, 2, 3 };
        var oversize = new byte[(1024 * 1024) + 1];
        ReadOnlyMemory<byte>[] batch = [valid, oversize];

        await Assert.ThrowsAsync<ArgumentException>(async () => await transport.SendAsync(batch));

        // The valid frame ahead of the oversize one must still have reached the stream.
        var readBack = new TcpTransport(new MemoryStream(stream.ToArray()));
        Assert.Equal(valid, await readBack.ReceiveAsync());
        Assert.Null(await readBack.ReceiveAsync());
    }

    private sealed class CountingStream : MemoryStream
    {
        public int WriteCount { get; private set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            WriteCount++;
            return base.WriteAsync(buffer, cancellationToken);
        }
    }

    private static void WriteFrame(MemoryStream stream, byte[] payload)
    {
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);
        stream.Write(header);
        if (payload.Length > 0)
        {
            stream.Write(payload);
        }
    }
}
