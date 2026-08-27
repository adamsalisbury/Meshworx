using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using AdamSalisbury.Meshworx.Compression;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Compression;

public sealed class MeshClientPayloadCompressionTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    // Acceptance criteria

    [Theory(Timeout = 10000)]
    [InlineData(null)]
    [InlineData(CompressionAlgorithms.Brotli)]
    [InlineData(CompressionAlgorithms.Deflate)]
    public async Task Compressed_BuiltInAlgorithm_IsSmallerOnTheWireAndByteIdenticalOnArrival(string? algorithmId)
    {
        ReadOnlyMemory<byte> payload = CompressiblePayload();
        DeliveryOptions options = algorithmId is null
            ? DeliveryOptions.Compressed()
            : DeliveryOptions.Compressed(algorithmId);

        (byte[] frame, byte[] uncompressedFrame) = await CaptureFramesAsync(payload, options);

        Assert.True(
            frame.Length < uncompressedFrame.Length,
            $"compressed frame was {frame.Length} bytes against {uncompressedFrame.Length} uncompressed");

        Assert.Equal(payload.ToArray(), await ReceiveAsync(frame));
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_CustomRegisteredAlgorithm_RoundTripsWithoutLibraryChanges()
    {
        // The whole point of resolving through the registry: an algorithm the library has never heard
        // of, on both endpoints, with no library code involved in either direction. Run-heavy rather
        // than the usual JSON payload, because run-length encoding would grow that one and the send
        // would correctly fall back to uncompressed, testing nothing.
        ReadOnlyMemory<byte> payload = RunHeavyPayload();

        CompressionStrategyRegistry senderRegistry =
            CompressionStrategyRegistry.CreateDefault().Register(new RunLengthCompressionStrategy());
        CompressionStrategyRegistry receiverRegistry =
            CompressionStrategyRegistry.CreateDefault().Register(new RunLengthCompressionStrategy());

        byte[] frame = await CaptureSentFrameAsync(
            payload,
            DeliveryOptions.Compressed(RunLengthCompressionStrategy.Id),
            senderRegistry);

        MessageHeaders headers = ReadHeaders(frame);

        Assert.Equal(RunLengthCompressionStrategy.Id, headers["mesh.compression"]);
        Assert.Equal(payload.ToArray(), await ReceiveAsync(frame, receiverRegistry));
    }

    [Fact(Timeout = 10000)]
    public async Task Send_WithoutCompressionRequested_IsUnchanged()
    {
        // The default must be byte-for-byte what it was before compression existed: no header block at
        // all, and the payload verbatim.
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        byte[] frame = await CaptureSentFrameAsync(payload, DeliveryOptions.None);

        Assert.Equal((byte)MessageType.SendMessage, frame[0]);
        Assert.Equal(payload.ToArray(), frame[17..]);
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_ReceiverMissingTheStrategy_DropsTheMessageAndKeepsTheConnection()
    {
        ReadOnlyMemory<byte> payload = RunHeavyPayload();

        CompressionStrategyRegistry senderRegistry =
            new CompressionStrategyRegistry().Register(new RunLengthCompressionStrategy());

        byte[] frame = await CaptureSentFrameAsync(
            payload, DeliveryOptions.Compressed(RunLengthCompressionStrategy.Id), senderRegistry);

        // The receiver holds only the built-ins, so it cannot read this body.
        (bool raised, MeshClientFixture receiver) = await TryReceiveAsync(frame);

        Assert.False(raised);
        Assert.True(receiver.Client.IsConnected);

        await receiver.Client.DisconnectAsync();
    }

    // Sender-side behaviour

    [Fact(Timeout = 10000)]
    public async Task Compressed_UnregisteredNamedAlgorithm_ThrowsBeforeSending()
    {
        // Naming an algorithm is a requirement, so this fails locally rather than putting a body on the
        // wire that the peer was never going to be able to read.
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        await ConnectAndCaptureAsync(fixture, sentFrames, AllAlgorithms);

        await Assert.ThrowsAsync<UnknownCompressionAlgorithmException>(
            () => fixture.Client.SendAsync(
                Guid.NewGuid(), CompressiblePayload(), DeliveryOptions.Compressed("zstd")));

        Assert.Empty(sentFrames);

        await fixture.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_EmptyRegistry_SendsUncompressedRatherThanFailing()
    {
        // Asking for the best available is a preference, not a requirement — the opposite of naming one.
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        byte[] frame = await CaptureSentFrameAsync(
            payload, DeliveryOptions.Compressed(), new CompressionStrategyRegistry());

        Assert.Equal((byte)MessageType.SendMessage, frame[0]);
        Assert.Equal(payload.ToArray(), frame[17..]);
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_PayloadBelowTheFloor_IsSentUntouched()
    {
        byte[] payload = Encoding.UTF8.GetBytes(new string('a', 128));

        byte[] frame = await CaptureSentFrameAsync(payload, DeliveryOptions.Compressed());

        Assert.Equal((byte)MessageType.SendMessage, frame[0]);
        Assert.Equal(payload, frame[17..]);
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_IncompressiblePayload_FallsBackToTheOriginal()
    {
        // Opting in must never make a message bigger. Random bytes cannot be compressed, so the
        // compressed form is larger and is discarded rather than sent.
        var payload = new byte[8 * 1024];
        new Random(20260827).NextBytes(payload);

        byte[] frame = await CaptureSentFrameAsync(payload, DeliveryOptions.Compressed());

        Assert.Equal((byte)MessageType.SendMessage, frame[0]);
        Assert.Equal(payload, frame[17..]);
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_SetsBothHeadersAndTheDeclaredLengthMatchesThePayload()
    {
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        byte[] frame = await CaptureSentFrameAsync(payload, DeliveryOptions.Compressed());
        MessageHeaders headers = ReadHeaders(frame);

        Assert.Equal(CompressionAlgorithms.Brotli, headers["mesh.compression"]);
        Assert.Equal(
            payload.Length.ToString(CultureInfo.InvariantCulture), headers["mesh.compression.length"]);
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_CombinedWithPriorityAndAwaitCapacity_CarriesEveryHeader()
    {
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        DeliveryOptions options = DeliveryOptions
            .AtPriority(MessagePriority.High)
            .WithAwaitCapacity()
            .WithCompression();

        byte[] frame = await CaptureSentFrameAsync(payload, options);
        MessageHeaders headers = ReadHeaders(frame);

        Assert.Equal(CompressionAlgorithms.Brotli, headers["mesh.compression"]);
        Assert.Equal("1", headers["mesh.await-capacity"]);
        Assert.True(headers.ContainsKey("mesh.priority"));
        Assert.Equal(payload.ToArray(), await ReceiveAsync(frame));
    }

    // Receive-side hostility

    [Fact(Timeout = 10000)]
    public async Task Receive_TruncatedCompressedBody_IsDroppedRatherThanDeliveredShort()
    {
        // KI-74 closed at the message layer: the declared length is what distinguishes a truncated body
        // from a complete one, since the decompressor itself returns a prefix without complaint.
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        byte[] frame = await CaptureSentFrameAsync(payload, DeliveryOptions.Compressed());
        int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(17, 2));
        int bodyStart = 19 + headerBlockLength;
        byte[] truncated = frame[..(bodyStart + ((frame.Length - bodyStart) / 2))];

        (bool raised, MeshClientFixture receiver) = await TryReceiveAsync(truncated);

        Assert.False(raised);
        Assert.True(receiver.Client.IsConnected);

        await receiver.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task Receive_DeclaredLengthPastTheCeiling_IsDroppedWithoutDecompressing()
    {
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        byte[] frame = await CaptureSentFrameAsync(payload, DeliveryOptions.Compressed());

        // A ceiling below the payload's real size: the declared length alone is enough to refuse it.
        (bool raised, MeshClientFixture receiver) = await TryReceiveAsync(
            frame, maxDecompressedBytes: payload.Length - 1);

        Assert.False(raised);
        Assert.True(receiver.Client.IsConnected);

        await receiver.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task Receive_CorruptCompressedBody_IsDroppedRatherThanThrowingIntoTheReceiveLoop()
    {
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        byte[] frame = await CaptureSentFrameAsync(payload, DeliveryOptions.Compressed());
        int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(17, 2));

        // Scramble the compressed body, leaving the headers intact.
        new Random(7).NextBytes(frame.AsSpan(19 + headerBlockLength));

        (bool raised, MeshClientFixture receiver) = await TryReceiveAsync(frame);

        Assert.False(raised);
        Assert.True(receiver.Client.IsConnected);

        await receiver.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task Receive_CompressedMessage_HidesTheCompressionHeadersFromTheSubscriber()
    {
        // Compression is meant to be invisible: a subscriber sees the headers the sender sent, and
        // echoing them back onto a reply must not send the far side off decompressing an ordinary body.
        ReadOnlyMemory<byte> payload = CompressiblePayload();

        byte[] frame = await CaptureSentFrameAsync(payload, DeliveryOptions.Compressed());

        var receiver = new MeshClientFixture();
        var received = new TaskCompletionSource<MessageHeaders>();

        receiver.SetupSuccessfulRegistration(ToDeliveryFrame(frame, Guid.NewGuid()));
        receiver.Client.MessageReceived += (_, e) => received.TrySetResult(e.Headers);
        await receiver.Client.ConnectAsync(receiver.Transport.Object, "Recipient");

        MessageHeaders headers = await received.Task.WaitAsync(WaitTimeout);

        Assert.False(headers.ContainsKey("mesh.compression"));
        Assert.False(headers.ContainsKey("mesh.compression.length"));

        await receiver.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task SendAsync_ApplicationHeaderNamedLikeACompressionHeader_IsRefused()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var headers = new MessageHeaders([new KeyValuePair<string, string>("mesh.compression", "br")]);

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[8], headers));

        await fixture.Client.DisconnectAsync();
    }

    // Helpers

    /// <summary>
    /// Every algorithm any test here sends with. The default peer set, so a test about compression
    /// mechanics is not also a test about negotiation — the two are separated deliberately, and the
    /// negotiation tests below pass their own set.
    /// </summary>
    private static readonly string[] AllAlgorithms =
        [CompressionAlgorithms.Brotli, CompressionAlgorithms.Deflate, RunLengthCompressionStrategy.Id];

    private static async Task<byte[]> CaptureSentFrameAsync(
        ReadOnlyMemory<byte> payload,
        DeliveryOptions options,
        ICompressionStrategyRegistry? registry = null,
        IReadOnlyList<string>? peerAlgorithmIds = null)
    {
        var fixture = new MeshClientFixture(compressionStrategies: registry);
        var sentFrames = new List<byte[]>();

        await ConnectAndCaptureAsync(fixture, sentFrames, peerAlgorithmIds ?? AllAlgorithms);
        await fixture.Client.SendAsync(Guid.NewGuid(), payload, options);

        // Read before disconnecting: teardown writes a frame of its own, which would otherwise land in
        // the capture and turn every single-frame assertion into a two-frame one.
        byte[] frame = Assert.Single(sentFrames);

        await fixture.Client.DisconnectAsync();

        return frame;
    }

    private static async Task<(byte[] Compressed, byte[] Uncompressed)> CaptureFramesAsync(
        ReadOnlyMemory<byte> payload, DeliveryOptions options)
    {
        return (
            await CaptureSentFrameAsync(payload, options),
            await CaptureSentFrameAsync(payload, DeliveryOptions.None));
    }

    /// <summary>
    /// Connects, then captures the frames the client sends — while standing in for the hub's side of
    /// capability negotiation, since a compressing send now asks what its recipient supports before
    /// choosing an algorithm.
    /// </summary>
    /// <param name="fixture">The sending client's fixture.</param>
    /// <param name="sentFrames">Receives every message frame the client sends.</param>
    /// <param name="peerAlgorithmIds">
    /// What the hub should report the recipient supports. <see langword="null"/> means the hub never
    /// answers at all, which is how a test models an unreachable or silent capability query.
    /// </param>
    private static async Task ConnectAndCaptureAsync(
        MeshClientFixture fixture, List<byte[]> sentFrames, IReadOnlyList<string>? peerAlgorithmIds)
    {
        fixture.SetupSuccessfulRegistration();
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Sender");

        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) =>
            {
                byte[] sent = frame.ToArray();

                if (sent.Length >= 21 && sent[0] == (byte)MessageType.CompressionCapabilityRequest)
                {
                    if (peerAlgorithmIds is not null)
                    {
                        fixture.Inbound!.TryWrite(
                            BuildCapabilityResponse(
                                BinaryPrimitives.ReadInt32BigEndian(sent.AsSpan(1, 4)), peerAlgorithmIds));
                    }

                    // Never captured: a capability query is negotiation, not one of the message frames
                    // these tests are counting and decoding.
                    return;
                }

                sentFrames.Add(sent);
            })
            .Returns(Task.CompletedTask);
    }

    private static byte[] BuildCapabilityResponse(int correlationId, IReadOnlyList<string> algorithmIds)
    {
        int blockLength = 1 + algorithmIds.Sum(id => 1 + Encoding.UTF8.GetByteCount(id));
        var response = new byte[6 + blockLength];
        response[0] = (byte)MessageType.CompressionCapabilityResponse;
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(1, 4), correlationId);
        response[5] = 0x01;
        response[6] = (byte)algorithmIds.Count;

        int offset = 7;

        foreach (string id in algorithmIds)
        {
            int written = Encoding.UTF8.GetBytes(id, response.AsSpan(offset + 1));
            response[offset] = (byte)written;
            offset += 1 + written;
        }

        return response;
    }

    private static async Task<byte[]> ReceiveAsync(
        byte[] sendFrame, ICompressionStrategyRegistry? registry = null)
    {
        var receiver = new MeshClientFixture(compressionStrategies: registry);
        var received = new TaskCompletionSource<byte[]>();

        receiver.SetupSuccessfulRegistration(ToDeliveryFrame(sendFrame, Guid.NewGuid()));
        receiver.Client.MessageReceived += (_, e) => received.TrySetResult(e.Data.ToArray());
        await receiver.Client.ConnectAsync(receiver.Transport.Object, "Recipient");

        byte[] data = await received.Task.WaitAsync(WaitTimeout);
        await receiver.Client.DisconnectAsync();

        return data;
    }

    /// <summary>
    /// Feeds a frame the receiver is expected to refuse, then proves it refused it by sending a second,
    /// perfectly ordinary message behind it and watching that one arrive. Waiting on a timeout alone
    /// would only show that nothing arrived quickly, not that the receive loop is still running.
    /// </summary>
    private static async Task<(bool Raised, MeshClientFixture Receiver)> TryReceiveAsync(
        byte[] sendFrame, int? maxDecompressedBytes = null)
    {
        var receiver = new MeshClientFixture(maxDecompressedBytes: maxDecompressedBytes);
        var senderId = Guid.NewGuid();
        byte[] sentinel = [0xAB, 0xCD];

        var raised = new List<byte[]>();
        var sentinelArrived = new TaskCompletionSource();

        receiver.SetupSuccessfulRegistration(
            ToDeliveryFrame(sendFrame, senderId),
            BuildPlainDeliveryFrame(senderId, sentinel));

        receiver.Client.MessageReceived += (_, e) =>
        {
            byte[] data = e.Data.ToArray();

            if (data.AsSpan().SequenceEqual(sentinel))
            {
                sentinelArrived.TrySetResult();
                return;
            }

            raised.Add(data);
        };

        await receiver.Client.ConnectAsync(receiver.Transport.Object, "Recipient");
        await sentinelArrived.Task.WaitAsync(WaitTimeout);

        return (raised.Count > 0, receiver);
    }

    private static byte[] ToDeliveryFrame(byte[] sendFrame, Guid senderId)
    {
        Assert.Equal((byte)MessageType.SendMessageWithHeaders, sendFrame[0]);

        var delivery = new byte[sendFrame.Length];
        sendFrame.CopyTo(delivery, 0);
        delivery[0] = (byte)MessageType.DeliverMessageWithHeaders;
        senderId.TryWriteBytes(delivery.AsSpan(1, 16));

        return delivery;
    }

    private static byte[] BuildPlainDeliveryFrame(Guid senderId, ReadOnlySpan<byte> body)
    {
        var frame = new byte[17 + body.Length];
        frame[0] = (byte)MessageType.DeliverMessage;
        senderId.TryWriteBytes(frame.AsSpan(1, 16));
        body.CopyTo(frame.AsSpan(17));

        return frame;
    }

    private static MessageHeaders ReadHeaders(byte[] sendFrame)
    {
        Assert.Equal((byte)MessageType.SendMessageWithHeaders, sendFrame[0]);

        int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(sendFrame.AsSpan(17, 2));

        return HeaderEnvelope.Read(sendFrame.AsSpan(19), headerBlockLength);
    }

    private static ReadOnlyMemory<byte> CompressiblePayload()
    {
        var builder = new StringBuilder();

        for (int i = 0; i < 200; i++)
        {
            builder.Append(
                CultureInfo.InvariantCulture,
                $"{{\"deviceId\":\"sensor-{i:D4}\",\"temperature\":21.5,\"humidity\":48,\"status\":\"nominal\"}},");
        }

        return Encoding.UTF8.GetBytes(builder.ToString());
    }

    /// <summary>
    /// A payload of long single-byte runs, which <see cref="RunLengthCompressionStrategy"/> compresses
    /// heavily and the built-ins compress too.
    /// </summary>
    private static ReadOnlyMemory<byte> RunHeavyPayload()
    {
        var payload = new byte[8 * 1024];

        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i / 512);
        }

        return payload;
    }

    /// <summary>
    /// A consumer's own algorithm the library has never heard of. Genuinely compresses runs, so a
    /// repetitive payload really does get smaller — and really does need the same strategy at the far
    /// end to be read back.
    /// </summary>
    private sealed class RunLengthCompressionStrategy : ICompressionStrategy
    {
        internal const string Id = "x-rle";

        public string AlgorithmId => Id;

        public ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte> payload)
        {
            var output = new List<byte>();
            ReadOnlySpan<byte> span = payload.Span;

            for (int i = 0; i < span.Length;)
            {
                byte value = span[i];
                int run = 1;

                while (i + run < span.Length && span[i + run] == value && run < byte.MaxValue)
                {
                    run++;
                }

                output.Add((byte)run);
                output.Add(value);
                i += run;
            }

            return output.ToArray();
        }

        public ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> payload, int maxDecompressedBytes)
        {
            var output = new List<byte>();
            ReadOnlySpan<byte> span = payload.Span;

            if ((span.Length % 2) != 0)
            {
                throw new InvalidDataException("Truncated run-length body.");
            }

            for (int i = 0; i < span.Length; i += 2)
            {
                int run = span[i];

                if (output.Count + run > maxDecompressedBytes)
                {
                    throw new InvalidDataException("Run-length body exceeded the limit.");
                }

                for (int j = 0; j < run; j++)
                {
                    output.Add(span[i + 1]);
                }
            }

            return output.ToArray();
        }
    }
}
