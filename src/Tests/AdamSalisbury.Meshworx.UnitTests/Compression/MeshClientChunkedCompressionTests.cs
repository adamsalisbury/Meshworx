using System.Buffers.Binary;
using AdamSalisbury.Meshworx.Compression;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Compression;

/// <summary>
/// Issue #76 — compression across a chunked transfer.
/// </summary>
/// <remarks>
/// The property under test throughout is that compression is applied <i>per chunk</i>, not per logical
/// message: each chunk carries a compressed slice of the message rather than a slice of the compressed
/// message. That is what keeps the compression machinery's working set at one chunk however large the
/// transfer is, and it is why the receiving client decompresses each frame before reassembling it.
/// <para>
/// Shares the chunking collection, and for the same reason: these tests move multiple megabytes each and
/// serialising them keeps that cost from pushing an unrelated test past its timeout.
/// </para>
/// </remarks>
[Collection(ChunkingCollectionDefinition.Name)]
public sealed class MeshClientChunkedCompressionTests
{
    /// <summary>The transport's single-frame cap, which no chunk body may exceed.</summary>
    private const int ChunkBodyCap = 1024 * 1024;

    /// <summary>
    /// What one chunk actually carries: the frame cap less the client's 4 KiB reserve for the message
    /// type, addressing and header block. Tests that need an exact chunk count size their payload from
    /// this rather than from the cap.
    /// </summary>
    private const int ChunkBodySize = ChunkBodyCap - (4 * 1024);

    /// <summary>
    /// Acceptance criterion: a multi-MiB compressible payload round-trips byte-identically over a
    /// chunked, compressed transfer, and arrives as one message with none of the machinery visible on it.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task SendLargeAsync_Compressed_RoundTripsAMultiMegabytePayloadByteIdentically()
    {
        byte[] payload = CompressiblePayload(5 * 1024 * 1024);

        List<byte[]> frames = await SendLargeAndCaptureAsync(payload, DeliveryOptions.Compressed());

        Assert.True(frames.Count > 1, $"expected chunking, got {frames.Count} frame(s)");
        Assert.All(frames, f => Assert.Contains(CompressionHeaderKeys.Algorithm, ReadHeaders(f)));

        (byte[] received, MessageHeaders headers, int raisedCount) = await ReceiveAsync(frames);

        Assert.Equal(1, raisedCount);
        Assert.True(payload.AsSpan().SequenceEqual(received), "reassembled payload differs");

        // Neither the chunking nor the compression is the subscriber's business: it sees the headers the
        // sender passed in, which here is none at all.
        Assert.Empty(headers);
    }

    /// <summary>
    /// Acceptance criterion: peak memory does not scale with the payload. Proven at the point it matters
    /// rather than by measuring allocations — the strategy records the largest buffer it is ever handed,
    /// on both sides, and for an 8 MiB transfer that stays within one chunk.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task SendLargeAsync_Compressed_NeverHandsAStrategyMoreThanOneChunk()
    {
        byte[] payload = CompressiblePayload(8 * 1024 * 1024);

        var senderStrategy = new RecordingCompressionStrategy();
        var receiverStrategy = new RecordingCompressionStrategy();

        List<byte[]> frames = await SendLargeAndCaptureAsync(
            payload, DeliveryOptions.Compressed(), senderStrategy);

        Assert.True(frames.Count >= 8, $"expected at least 8 chunks, got {frames.Count}");

        (byte[] received, _, _) = await ReceiveAsync(frames, receiverStrategy);

        Assert.True(payload.AsSpan().SequenceEqual(received), "reassembled payload differs");

        // Every chunk went through the strategy — this is not passing because compression was skipped.
        Assert.Equal(frames.Count, senderStrategy.CompressCallCount);
        Assert.Equal(frames.Count, receiverStrategy.DecompressCallCount);

        Assert.True(
            senderStrategy.LargestCompressInput <= ChunkBodyCap,
            $"compressed {senderStrategy.LargestCompressInput} bytes at once");
        Assert.True(
            receiverStrategy.LargestDecompressOutput <= ChunkBodyCap,
            $"decompressed to {receiverStrategy.LargestDecompressOutput} bytes at once");
    }

    /// <summary>
    /// The point of the exercise: the same payload costs materially fewer bytes on the wire compressed
    /// than it does uncompressed.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task SendLargeAsync_Compressed_SendsFarFewerBytesThanTheSameSendUncompressed()
    {
        byte[] payload = CompressiblePayload(3 * 1024 * 1024);

        List<byte[]> compressed = await SendLargeAndCaptureAsync(payload, DeliveryOptions.Compressed());
        List<byte[]> plain = await SendLargeAndCaptureAsync(payload, DeliveryOptions.None);

        int compressedBytes = compressed.Sum(f => f.Length);
        int plainBytes = plain.Sum(f => f.Length);

        Assert.True(
            compressedBytes < plainBytes / 2,
            $"compressed to {compressedBytes} bytes against {plainBytes} uncompressed");
    }

    /// <summary>
    /// Each chunk declares its own uncompressed length, because that is what its body decompresses to.
    /// A single per-message length would be wrong on every chunk but the one it was taken from, and is
    /// what makes a truncated chunk detectable rather than a silently short one.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task SendLargeAsync_Compressed_EachChunkDeclaresItsOwnUncompressedLength()
    {
        byte[] payload = CompressiblePayload((2 * ChunkBodySize) + 4096);

        List<byte[]> frames = await SendLargeAndCaptureAsync(payload, DeliveryOptions.Compressed());

        var declared = 0L;

        foreach (byte[] frame in frames)
        {
            MessageHeaders headers = ReadHeaders(frame);

            Assert.True(CompressionHeaderKeys.TryReadCompressionHeaders(
                headers, out string algorithmId, out int uncompressedLength));
            Assert.Equal(CompressionAlgorithms.Brotli, algorithmId);
            Assert.True(uncompressedLength <= ChunkBodyCap, $"chunk declares {uncompressedLength} bytes");

            declared += uncompressedLength;
        }

        // The declared lengths account for the whole message and nothing more.
        Assert.Equal(payload.Length, declared);
    }

    /// <summary>
    /// Compressing is decided per chunk, so an incompressible chunk is sent as it was even when the
    /// chunks around it compressed well. The alternative — committing the whole transfer to compression
    /// because its first chunk benefited — would send the rest of it larger than it needed to be.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task SendLargeAsync_Compressed_DecidesPerChunkRatherThanPerMessage()
    {
        // A compressible first chunk followed by an incompressible second one.
        var payload = new byte[2 * ChunkBodySize];
        CompressiblePayload(ChunkBodySize).CopyTo(payload, 0);
        new Random(20260827).NextBytes(payload.AsSpan(ChunkBodySize));

        List<byte[]> frames = await SendLargeAndCaptureAsync(payload, DeliveryOptions.Compressed());

        Assert.Equal(2, frames.Count);
        Assert.Contains(CompressionHeaderKeys.Algorithm, ReadHeaders(frames[0]));
        Assert.DoesNotContain(CompressionHeaderKeys.Algorithm, ReadHeaders(frames[1]));

        // And a transfer that is compressed only in part still round-trips whole.
        (byte[] received, _, _) = await ReceiveAsync(frames);
        Assert.True(payload.AsSpan().SequenceEqual(received), "reassembled payload differs");
    }

    /// <summary>
    /// A recipient below <see cref="Protocol.ChunkedCompressionMinVersion"/> reassembles before it
    /// decompresses, so it cannot read a per-chunk compressed transfer. Asking for compression is a
    /// preference, so the transfer goes uncompressed rather than failing.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task SendLargeAsync_Compressed_RecipientBelowMinVersion_SendsUncompressed()
    {
        byte[] payload = CompressiblePayload(2 * ChunkBodySize);

        List<byte[]> frames = await SendLargeAndCaptureAsync(
            payload,
            DeliveryOptions.Compressed(),
            recipientVersion: (byte)(Protocol.ChunkedCompressionMinVersion - 1));

        Assert.All(frames, f => Assert.DoesNotContain(CompressionHeaderKeys.Algorithm, ReadHeaders(f)));
    }

    /// <summary>
    /// A hub too old to report the recipient's version, or one that never answers, leaves the sender
    /// unable to establish that the recipient can read a per-chunk compressed transfer. Unknown counts as
    /// unsupported here, unlike an unknown algorithm set: getting this wrong produces a message the
    /// recipient accepts and mangles rather than one it drops.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task SendLargeAsync_Compressed_RecipientVersionUnknown_SendsUncompressed()
    {
        byte[] payload = CompressiblePayload(2 * ChunkBodySize);

        List<byte[]> frames = await SendLargeAndCaptureAsync(
            payload, DeliveryOptions.Compressed(), answerCapabilityQuery: false);

        Assert.All(frames, f => Assert.DoesNotContain(CompressionHeaderKeys.Algorithm, ReadHeaders(f)));
    }

    /// <summary>
    /// Naming an algorithm is a requirement rather than a preference — the same rule the unchunked path
    /// follows — so a recipient that cannot read a chunked compressed transfer at all fails the send
    /// rather than quietly downgrading it.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task SendLargeAsync_NamedAlgorithm_RecipientBelowMinVersion_ThrowsBeforeSendingAnything()
    {
        var fixture = new MeshClientFixture();
        var frames = new List<byte[]>();

        await ConnectAndAnswerAsync(
            fixture,
            frames,
            answerCapabilityQuery: true,
            recipientVersion: (byte)(Protocol.ChunkedCompressionMinVersion - 1));

        await Assert.ThrowsAsync<NotSupportedException>(
            () => fixture.Client.SendLargeAsync(
                Guid.NewGuid(),
                CompressiblePayload(2 * ChunkBodySize),
                null,
                DeliveryOptions.Compressed(CompressionAlgorithms.Brotli)));

        // The whole point of resolving the strategy once, up front: no chunk of a transfer that cannot
        // be completed reaches the wire.
        Assert.Empty(frames);

        await fixture.Client.DisconnectAsync();
    }

    /// <summary>
    /// The reassembly budget bounds what a receiver actually holds. Because decompression happens before
    /// reassembly, a compressed transfer is accounted at its restored size — a sender cannot slip past a
    /// receiver's budget by compressing, and then expand past it once admitted.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task ReceivingCompressedChunks_IsBoundedByTheReassemblyBudgetAtTheirRestoredSize()
    {
        byte[] payload = CompressiblePayload(3 * ChunkBodySize);

        List<byte[]> frames = await SendLargeAndCaptureAsync(payload, DeliveryOptions.Compressed());

        // Every frame is comfortably under the budget compressed; the restored transfer is not.
        Assert.All(frames, f => Assert.True(f.Length < ChunkBodyCap, $"frame of {f.Length} bytes"));

        var receiver = new MeshClientFixture(maxReassemblyBytes: 2 * ChunkBodySize);
        var raised = 0;

        receiver.SetupSuccessfulRegistration(
            [.. frames.Select(f => ToDeliveryFrame(f, Guid.NewGuid()))]);
        receiver.Client.MessageReceived += (_, _) => Interlocked.Increment(ref raised);

        await receiver.Client.ConnectAsync(receiver.Transport.Object, "Recipient");

        // Nothing is raised: the transfer is abandoned the moment it grows past the budget, exactly as an
        // uncompressed one of the same restored size would be.
        await Task.Delay(200);
        Assert.Equal(0, raised);

        await receiver.Client.DisconnectAsync();
    }

    /// <summary>
    /// Two compressed transfers interleaved on one connection stay separate, since compression changes
    /// nothing about how the reassembler keys a transfer.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task InterleavedCompressedTransfers_AreReassembledSeparately()
    {
        byte[] first = CompressiblePayload((2 * ChunkBodySize) + 17, seed: 3);
        byte[] second = CompressiblePayload((2 * ChunkBodySize) + 29, seed: 11);

        List<byte[]> firstFrames = await SendLargeAndCaptureAsync(first, DeliveryOptions.Compressed());
        List<byte[]> secondFrames = await SendLargeAndCaptureAsync(second, DeliveryOptions.Compressed());

        Assert.Equal(firstFrames.Count, secondFrames.Count);

        var senderId = Guid.NewGuid();
        List<byte[]> interleaved = [];

        for (int i = 0; i < firstFrames.Count; i++)
        {
            interleaved.Add(ToDeliveryFrame(firstFrames[i], senderId));
            interleaved.Add(ToDeliveryFrame(secondFrames[i], senderId));
        }

        var receiver = new MeshClientFixture();
        var messages = new List<byte[]>();
        var bothArrived = new TaskCompletionSource();

        receiver.SetupSuccessfulRegistration([.. interleaved]);
        receiver.Client.MessageReceived += (_, e) =>
        {
            lock (messages)
            {
                messages.Add(e.Data.ToArray());

                if (messages.Count == 2)
                {
                    bothArrived.TrySetResult();
                }
            }
        };

        await receiver.Client.ConnectAsync(receiver.Transport.Object, "Recipient");
        await bothArrived.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await receiver.Client.DisconnectAsync();

        Assert.Equal(2, messages.Count);
        Assert.Contains(messages, m => first.AsSpan().SequenceEqual(m));
        Assert.Contains(messages, m => second.AsSpan().SequenceEqual(m));
    }

    /// <summary>
    /// An unchunked compressed message still round-trips with decompression moved ahead of reassembly.
    /// There is one ordering in the receive loop, not one per shape of message, and this pins that the
    /// move did not cost the path issue #33 built.
    /// </summary>
    [Fact(Timeout = 30000)]
    public async Task SendLargeAsync_Compressed_SingleChunkPayload_StillRoundTrips()
    {
        byte[] payload = CompressiblePayload(64 * 1024);

        List<byte[]> frames = await SendLargeAndCaptureAsync(payload, DeliveryOptions.Compressed());

        byte[] frame = Assert.Single(frames);
        Assert.Contains(CompressionHeaderKeys.Algorithm, ReadHeaders(frame));

        (byte[] received, MessageHeaders headers, _) = await ReceiveAsync(frames);

        Assert.True(payload.AsSpan().SequenceEqual(received), "payload differs");
        Assert.Empty(headers);
    }

    /// <summary>
    /// The caller's own headers still travel, alongside the compression bookkeeping, and are the only
    /// ones the subscriber sees.
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task SendLargeAsync_Compressed_CallerHeadersSurviveAndTheBookkeepingDoesNot()
    {
        byte[] payload = CompressiblePayload(2 * ChunkBodySize);
        var callerHeaders = new MessageHeaders(new Dictionary<string, string> { ["content-type"] = "text/plain" });

        List<byte[]> frames = await SendLargeAndCaptureAsync(
            payload, DeliveryOptions.Compressed(), headers: callerHeaders);

        (byte[] received, MessageHeaders headers, _) = await ReceiveAsync(frames);

        Assert.True(payload.AsSpan().SequenceEqual(received), "reassembled payload differs");
        Assert.Equal("text/plain", headers["content-type"]);
        Assert.DoesNotContain(CompressionHeaderKeys.Algorithm, headers);
        Assert.DoesNotContain(CompressionHeaderKeys.UncompressedLength, headers);
        Assert.DoesNotContain(ChunkHeaderKeys.Id, headers);
        Assert.DoesNotContain(ChunkHeaderKeys.Index, headers);
        Assert.DoesNotContain(ChunkHeaderKeys.Count, headers);
    }

    /// <summary>
    /// A truncated chunk is caught rather than reassembled into a short message, because each chunk
    /// declares what it decompresses to (KI-74's mitigation, now per chunk).
    /// </summary>
    [Fact(Timeout = 60000)]
    public async Task ReceivingATruncatedCompressedChunk_DropsTheTransferRatherThanShorteningIt()
    {
        byte[] payload = CompressiblePayload(2 * ChunkBodySize);

        List<byte[]> frames = await SendLargeAndCaptureAsync(payload, DeliveryOptions.Compressed());
        Assert.Equal(2, frames.Count);

        // Lop the tail off the first chunk's compressed body, leaving its declared length untouched.
        byte[] truncated = frames[0][..(frames[0].Length - 64)];

        var receiver = new MeshClientFixture();
        var raised = 0;
        var senderId = Guid.NewGuid();

        receiver.SetupSuccessfulRegistration(
            ToDeliveryFrame(truncated, senderId), ToDeliveryFrame(frames[1], senderId));
        receiver.Client.MessageReceived += (_, _) => Interlocked.Increment(ref raised);

        await receiver.Client.ConnectAsync(receiver.Transport.Object, "Recipient");
        await Task.Delay(200);
        await receiver.Client.DisconnectAsync();

        Assert.Equal(0, raised);
    }

    /// <summary>
    /// A payload that produces a byte-identical pattern compresses well, and repeats often enough that a
    /// mis-ordered or truncated reassembly still fails the comparison.
    /// </summary>
    private static byte[] CompressiblePayload(int length, int seed = 0)
    {
        var payload = new byte[length];

        for (int i = 0; i < length; i++)
        {
            payload[i] = (byte)(((i / 64) + seed) % 17);
        }

        return payload;
    }

    private static MessageHeaders ReadHeaders(byte[] sendFrame)
    {
        Assert.Equal((byte)MessageType.SendMessageWithHeaders, sendFrame[0]);

        int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(sendFrame.AsSpan(17, 2));

        return HeaderEnvelope.Read(sendFrame.AsSpan(19), headerBlockLength);
    }

    /// <summary>
    /// Rewrites a client-to-hub send frame into the hub-to-client delivery frame the hub would forward,
    /// which differs only in the opcode and in carrying the sender's id rather than the recipient's.
    /// </summary>
    private static byte[] ToDeliveryFrame(byte[] sendFrame, Guid senderId)
    {
        Assert.Equal((byte)MessageType.SendMessageWithHeaders, sendFrame[0]);

        var delivery = new byte[sendFrame.Length];
        sendFrame.CopyTo(delivery, 0);
        delivery[0] = (byte)MessageType.DeliverMessageWithHeaders;
        senderId.TryWriteBytes(delivery.AsSpan(1, 16));

        return delivery;
    }

    private static async Task<List<byte[]>> SendLargeAndCaptureAsync(
        byte[] payload,
        DeliveryOptions options,
        ICompressionStrategy? strategy = null,
        MessageHeaders? headers = null,
        byte recipientVersion = Protocol.MaxSupportedVersion,
        bool answerCapabilityQuery = true)
    {
        CompressionStrategyRegistry? registry = null;

        if (strategy is not null)
        {
            registry = new CompressionStrategyRegistry();
            registry.Register(strategy);
        }

        var fixture = new MeshClientFixture(compressionStrategies: registry);
        var frames = new List<byte[]>();

        await ConnectAndAnswerAsync(
            fixture,
            frames,
            answerCapabilityQuery,
            recipientVersion,
            strategy?.AlgorithmId);

        await fixture.Client.SendLargeAsync(Guid.NewGuid(), payload, headers, options);
        await fixture.Client.DisconnectAsync();

        return frames;
    }

    /// <summary>
    /// Replays captured send frames into a second client as if the hub had forwarded them, and returns
    /// what its subscriber saw.
    /// </summary>
    private static async Task<(byte[] Data, MessageHeaders Headers, int RaisedCount)> ReceiveAsync(
        List<byte[]> frames, ICompressionStrategy? strategy = null)
    {
        CompressionStrategyRegistry? registry = null;

        if (strategy is not null)
        {
            registry = new CompressionStrategyRegistry();
            registry.Register(strategy);
        }

        var receiver = new MeshClientFixture(compressionStrategies: registry);
        var received = new TaskCompletionSource<(byte[], MessageHeaders)>();
        var raisedCount = 0;
        var senderId = Guid.NewGuid();

        receiver.SetupSuccessfulRegistration([.. frames.Select(f => ToDeliveryFrame(f, senderId))]);

        receiver.Client.MessageReceived += (_, e) =>
        {
            Interlocked.Increment(ref raisedCount);
            received.TrySetResult((e.Data.ToArray(), e.Headers));
        };

        await receiver.Client.ConnectAsync(receiver.Transport.Object, "Recipient");

        (byte[] data, MessageHeaders headers) =
            await received.Task.WaitAsync(TimeSpan.FromSeconds(30));

        await receiver.Client.DisconnectAsync();

        return (data, headers, raisedCount);
    }

    /// <summary>
    /// Connects a client and stands in for the hub's half of capability negotiation, so a compressing
    /// send resolves immediately instead of waiting out its query timeout.
    /// </summary>
    /// <param name="fixture">The client under test.</param>
    /// <param name="frames">Receives every message frame, with negotiation traffic filtered out.</param>
    /// <param name="answerCapabilityQuery">
    /// Whether to answer the capability query at all. Not answering models a hub that does not relay
    /// capabilities, and leaves the recipient's version unknown.
    /// </param>
    /// <param name="recipientVersion">The protocol version the hub reports for the recipient.</param>
    /// <param name="algorithmId">The single algorithm the recipient advertises. Defaults to Brotli.</param>
    private static async Task ConnectAndAnswerAsync(
        MeshClientFixture fixture,
        List<byte[]> frames,
        bool answerCapabilityQuery = true,
        byte recipientVersion = Protocol.MaxSupportedVersion,
        string? algorithmId = null)
    {
        IReadOnlyList<string> advertised = [algorithmId ?? CompressionAlgorithms.Brotli];

        fixture.SetupSuccessfulRegistration();
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Sender");

        frames.Clear();

        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) =>
            {
                byte[] sent = frame.ToArray();

                if (sent.Length >= 21 && sent[0] == (byte)MessageType.CompressionCapabilityRequest)
                {
                    if (answerCapabilityQuery)
                    {
                        fixture.Inbound!.TryWrite(BuildCapabilityResponse(
                            BinaryPrimitives.ReadInt32BigEndian(sent.AsSpan(1, 4)),
                            advertised,
                            recipientVersion));
                    }

                    return;
                }

                if (sent[0] == (byte)MessageType.SendMessageWithHeaders)
                {
                    lock (frames)
                    {
                        frames.Add(sent);
                    }
                }
            })
            .Returns(Task.CompletedTask);
    }

    private static byte[] BuildCapabilityResponse(
        int correlationId, IReadOnlyList<string> algorithmIds, byte subjectVersion)
    {
        int blockLength = CompressionCapabilityEnvelope.GetEncodedLength(algorithmIds);
        var response = new byte[7 + blockLength];
        response[0] = (byte)MessageType.CompressionCapabilityResponse;
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(1, 4), correlationId);
        response[5] = 0x01;
        response[6] = subjectVersion;
        CompressionCapabilityEnvelope.Write(algorithmIds, response.AsSpan(7));

        return response;
    }

    /// <summary>
    /// Brotli, with a note kept of the largest buffer it was ever handed in each direction. That number is
    /// the acceptance criterion: it is what "peak memory does not scale with the payload" means for the
    /// compression machinery, measured where it is decided rather than inferred from a GC counter.
    /// </summary>
    private sealed class RecordingCompressionStrategy : ICompressionStrategy
    {
        private readonly BrotliCompressionStrategy _inner = BrotliCompressionStrategy.Default;
        private int _largestCompressInput;
        private int _largestDecompressOutput;
        private int _compressCallCount;
        private int _decompressCallCount;

        public string AlgorithmId => _inner.AlgorithmId;

        public int LargestCompressInput => Volatile.Read(ref _largestCompressInput);

        public int LargestDecompressOutput => Volatile.Read(ref _largestDecompressOutput);

        public int CompressCallCount => Volatile.Read(ref _compressCallCount);

        public int DecompressCallCount => Volatile.Read(ref _decompressCallCount);

        public ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte> payload)
        {
            Interlocked.Increment(ref _compressCallCount);
            Record(ref _largestCompressInput, payload.Length);

            return _inner.Compress(payload);
        }

        public ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> payload, int maxDecompressedBytes)
        {
            Interlocked.Increment(ref _decompressCallCount);

            ReadOnlyMemory<byte> restored = _inner.Decompress(payload, maxDecompressedBytes);
            Record(ref _largestDecompressOutput, restored.Length);

            return restored;
        }

        private static void Record(ref int highWater, int observed)
        {
            int current = Volatile.Read(ref highWater);

            while (observed > current)
            {
                int previous = Interlocked.CompareExchange(ref highWater, observed, current);

                if (previous == current)
                {
                    return;
                }

                current = previous;
            }
        }
    }
}
