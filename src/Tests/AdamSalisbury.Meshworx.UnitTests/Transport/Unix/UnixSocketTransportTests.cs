using System.Buffers.Binary;
using AdamSalisbury.Meshworx.Transport.Unix;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Unix;

/// <summary>
/// Framing-level tests exercised directly against an in-memory stream via the internal
/// <see cref="UnixSocketTransport(Stream)"/> constructor, mirroring TcpTransportTests.cs's coverage of
/// the same shared StreamFramer code — both transports frame identically, so both need the same
/// malformed-input coverage rather than trusting the loopback tests alone to prove it.
/// </summary>
public sealed class UnixSocketTransportTests
{
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

        var transport = new UnixSocketTransport(stream);

        await Assert.ThrowsAsync<IOException>(() => transport.ReceiveAsync());
    }

    /// <summary>
    /// When ReceiveAsync reads a header containing a payload length exceeding the maximum allowed size,
    /// an IOException is thrown so receive loops treat the corrupt framing as a transport failure.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ReceiveAsync_PayloadExceedsMaxSize_ThrowsIOException()
    {
        var stream = new MemoryStream();
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, 1_048_577); // 1 MiB + 1
        stream.Write(header);
        stream.Position = 0;

        var transport = new UnixSocketTransport(stream);

        await Assert.ThrowsAsync<IOException>(() => transport.ReceiveAsync());
    }

    /// <summary>
    /// When the stream closes after the header is read but before the full payload is delivered,
    /// ReceiveAsync returns null rather than throwing.
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

        var transport = new UnixSocketTransport(stream);
        byte[]? result = await transport.ReceiveAsync();

        Assert.Null(result);
    }

    /// <summary>
    /// A payload sent and then received on the same stream round-trips exactly, confirming the shared
    /// framing behaves identically to TcpTransport's for this transport too.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task SendThenReceive_RoundTrip_ReturnsOriginalPayload()
    {
        var stream = new MemoryStream();
        var transport = new UnixSocketTransport(stream);
        var original = new byte[] { 10, 20, 30, 40, 50 };

        await transport.SendAsync(original);
        stream.Position = 0;
        byte[]? received = await transport.ReceiveAsync();

        Assert.Equal(original, received);
    }
}
