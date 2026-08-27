using System.Buffers.Binary;
using System.Text;
using AdamSalisbury.Meshworx.Compression;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Compression;

public sealed class MeshClientCompressionNegotiationTests
{
    // Advertising

    [Fact(Timeout = 10000)]
    public async Task Connect_AdvertisesEveryRegisteredAlgorithmInPreferenceOrder()
    {
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        // Capture installed after the registration setup, which installs a SendAsync stub of its own,
        // and before connecting, since the advertisement is sent from inside ConnectAsync.
        fixture.SetupSuccessfulRegistration();
        CaptureFrames(fixture, sentFrames);
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Sender");

        byte[] advertisement = Assert.Single(
            sentFrames.FindAll(f => f[0] == (byte)MessageType.AdvertiseCompression));

        Assert.True(CompressionCapabilityEnvelope.TryRead(
            advertisement.AsSpan(1), out IReadOnlyList<string> advertised));
        Assert.Equal([CompressionAlgorithms.Brotli, CompressionAlgorithms.Deflate], advertised);
    }

    [Fact(Timeout = 10000)]
    public async Task Connect_EmptyRegistry_AdvertisesNothing()
    {
        // Nothing to say, so nothing is said — rather than an advertisement of an empty set, which would
        // cost a frame to convey what a peer already assumes.
        var fixture = new MeshClientFixture(compressionStrategies: new CompressionStrategyRegistry());
        var sentFrames = new List<byte[]>();

        fixture.SetupSuccessfulRegistration();
        CaptureFrames(fixture, sentFrames);
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Sender");

        // ConnectAsync awaits the advertisement, so by the time it returns any frame there was to send
        // has been sent — nothing to wait for.
        Assert.DoesNotContain(sentFrames, f => f[0] == (byte)MessageType.AdvertiseCompression);
    }

    [Fact(Timeout = 10000)]
    public async Task Connect_BelowTheNegotiationVersion_AdvertisesNothing()
    {
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion(
            (byte)(Protocol.CompressionNegotiationMinVersion - 1));
        CaptureFrames(fixture, sentFrames);
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Sender");

        Assert.DoesNotContain(sentFrames, f => f[0] == (byte)MessageType.AdvertiseCompression);
    }

    [Fact(Timeout = 10000)]
    public async Task Connect_AdvertisementFails_StillConnects()
    {
        // An optimisation must never be a reason not to connect.
        var fixture = new MeshClientFixture();

        fixture.SetupSuccessfulRegistration();
        fixture.Transport
            .Setup(t => t.SendAsync(
                It.Is<ReadOnlyMemory<byte>>(
                    f => f.Length > 0 && f.ToArray()[0] == (byte)MessageType.AdvertiseCompression),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("advertisement failed"));

        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Sender");

        Assert.True(fixture.Client.IsConnected);

        await fixture.Client.DisconnectAsync();
    }

    // Selection

    [Fact(Timeout = 10000)]
    public async Task Compressed_PeerSupportsOnlyTheSecondChoice_UsesIt()
    {
        // The acceptance criterion: two endpoints with overlapping sets settle on a shared algorithm with
        // no configuration on either side. Brotli is this client's first choice and the peer cannot read
        // it, so Deflate is chosen — automatically, and without the send failing.
        MessageHeaders headers = await SendAndReadHeadersAsync(
            DeliveryOptions.Compressed(), peerAlgorithmIds: [CompressionAlgorithms.Deflate]);

        Assert.Equal(CompressionAlgorithms.Deflate, headers["mesh.compression"]);
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_PeerSupportsBoth_UsesTheLocalFirstChoice()
    {
        MessageHeaders headers = await SendAndReadHeadersAsync(
            DeliveryOptions.Compressed(),
            peerAlgorithmIds: [CompressionAlgorithms.Deflate, CompressionAlgorithms.Brotli]);

        // Local order wins the tie: the peer listing Deflate first does not override this client's own
        // preference among algorithms they both hold.
        Assert.Equal(CompressionAlgorithms.Brotli, headers["mesh.compression"]);
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_NoSharedAlgorithm_SendsUncompressedWithoutThrowing()
    {
        // The other acceptance criterion: no overlap is not an error.
        byte[] frame = await SendAndCaptureAsync(
            DeliveryOptions.Compressed(), peerAlgorithmIds: ["zstd"]);

        Assert.Equal((byte)MessageType.SendMessage, frame[0]);
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_PeerAdvertisedNothing_SendsUncompressed()
    {
        byte[] frame = await SendAndCaptureAsync(DeliveryOptions.Compressed(), peerAlgorithmIds: []);

        Assert.Equal((byte)MessageType.SendMessage, frame[0]);
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_NamedAlgorithmThePeerDoesNotSupport_ThrowsBeforeSending()
    {
        // Naming an algorithm is a requirement, so this fails locally rather than putting a body on the
        // wire the recipient was always going to drop.
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        await ConnectAndAnswerAsync(fixture, sentFrames, [CompressionAlgorithms.Deflate]);

        var exception = await Assert.ThrowsAsync<UnknownCompressionAlgorithmException>(
            () => fixture.Client.SendAsync(
                Guid.NewGuid(), Payload(), DeliveryOptions.Compressed(CompressionAlgorithms.Brotli)));

        Assert.Equal(CompressionAlgorithms.Brotli, exception.AlgorithmId);
        Assert.NotNull(exception.PeerId);
        Assert.Empty(sentFrames);

        await fixture.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_NamedAlgorithmThePeerSupports_IsUsed()
    {
        MessageHeaders headers = await SendAndReadHeadersAsync(
            DeliveryOptions.Compressed(CompressionAlgorithms.Deflate),
            peerAlgorithmIds: [CompressionAlgorithms.Brotli, CompressionAlgorithms.Deflate]);

        Assert.Equal(CompressionAlgorithms.Deflate, headers["mesh.compression"]);
    }

    // Falling back to pre-negotiation behaviour

    [Fact(Timeout = 10000)]
    public async Task Compressed_BelowTheNegotiationVersion_CompressesWithoutAsking()
    {
        // Negotiation is an optimisation over what a compressing send already did, never a precondition
        // for it: an older hub costs the send nothing.
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion(
            (byte)(Protocol.CompressionNegotiationMinVersion - 1));
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Sender");
        CaptureFrames(fixture, sentFrames);

        await fixture.Client.SendAsync(Guid.NewGuid(), Payload(), DeliveryOptions.Compressed());

        byte[] frame = Assert.Single(sentFrames);

        Assert.Equal((byte)MessageType.SendMessageWithHeaders, frame[0]);
        Assert.Equal(CompressionAlgorithms.Brotli, ReadHeaders(frame)["mesh.compression"]);
        Assert.DoesNotContain(
            sentFrames, f => f[0] == (byte)MessageType.CompressionCapabilityRequest);

        await fixture.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_CapabilityQueryFails_FallsBackToLocalChoiceRatherThanFailing()
    {
        // Exercises the same fallback the inline query's timeout reaches, by failing the query outright
        // instead of letting it expire. Deliberately not written as a timeout test: waiting out the real
        // five-second bound would hold a slot for the whole suite's duration, and this repository already
        // has timing-sensitive end-to-end tests that a loaded runner pushes over their budgets.
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        fixture.SetupSuccessfulRegistration();
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Sender");

        // Capture first, then the narrower throwing setup: Moq resolves a call against the last matching
        // setup, so a broad one installed afterwards would shadow this and the query would go unanswered
        // instead of failing — passing the test for the wrong reason, and taking the full timeout to do it.
        CaptureFrames(fixture, sentFrames);

        fixture.Transport
            .Setup(t => t.SendAsync(
                It.Is<ReadOnlyMemory<byte>>(
                    f => f.Length > 0
                        && f.ToArray()[0] == (byte)MessageType.CompressionCapabilityRequest),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("the capability query could not be sent"));

        await fixture.Client.SendAsync(Guid.NewGuid(), Payload(), DeliveryOptions.Compressed());

        byte[] frame = Assert.Single(
            sentFrames.FindAll(f => f[0] == (byte)MessageType.SendMessageWithHeaders));

        Assert.Equal(CompressionAlgorithms.Brotli, ReadHeaders(frame)["mesh.compression"]);

        await fixture.Client.DisconnectAsync();
    }

    // Caching and refresh

    [Fact(Timeout = 10000)]
    public async Task Compressed_SecondSendToTheSamePeer_ReusesTheCachedAnswer()
    {
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();
        int queries = 0;

        await ConnectAndAnswerAsync(
            fixture, sentFrames, [CompressionAlgorithms.Brotli], () => Interlocked.Increment(ref queries));

        var recipientId = Guid.NewGuid();
        await fixture.Client.SendAsync(recipientId, Payload(), DeliveryOptions.Compressed());
        await fixture.Client.SendAsync(recipientId, Payload(), DeliveryOptions.Compressed());

        Assert.Equal(1, queries);
        Assert.Equal(2, sentFrames.Count);

        await fixture.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_DifferentPeers_AreQueriedSeparately()
    {
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();
        int queries = 0;

        await ConnectAndAnswerAsync(
            fixture, sentFrames, [CompressionAlgorithms.Brotli], () => Interlocked.Increment(ref queries));

        await fixture.Client.SendAsync(Guid.NewGuid(), Payload(), DeliveryOptions.Compressed());
        await fixture.Client.SendAsync(Guid.NewGuid(), Payload(), DeliveryOptions.Compressed());

        Assert.Equal(2, queries);

        await fixture.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task Compressed_CachedAnswerPastItsLifetime_IsFetchedAgain()
    {
        // The acceptance criterion about capability sets updating: a peer that changed what it registers
        // must not be talked to on last hour's information indefinitely.
        var timeProvider = new ControllableTimeProvider();
        var fixture = new MeshClientFixture(timeProvider: timeProvider);
        var sentFrames = new List<byte[]>();
        int queries = 0;

        await ConnectAndAnswerAsync(
            fixture, sentFrames, [CompressionAlgorithms.Brotli], () => Interlocked.Increment(ref queries));

        var recipientId = Guid.NewGuid();
        await fixture.Client.SendAsync(recipientId, Payload(), DeliveryOptions.Compressed());

        timeProvider.Advance(TimeSpan.FromMinutes(6));

        await fixture.Client.SendAsync(recipientId, Payload(), DeliveryOptions.Compressed());

        Assert.Equal(2, queries);

        await fixture.Client.DisconnectAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task Disconnect_ForgetsWhatPeersAdvertised()
    {
        // The ids a set is keyed by are only meaningful within the connection they were learned over.
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();
        int queries = 0;

        await ConnectAndAnswerAsync(
            fixture, sentFrames, [CompressionAlgorithms.Brotli], () => Interlocked.Increment(ref queries));

        var recipientId = Guid.NewGuid();
        await fixture.Client.SendAsync(recipientId, Payload(), DeliveryOptions.Compressed());
        await fixture.Client.DisconnectAsync();

        await ConnectAndAnswerAsync(
            fixture, sentFrames, [CompressionAlgorithms.Brotli], () => Interlocked.Increment(ref queries));

        await fixture.Client.SendAsync(recipientId, Payload(), DeliveryOptions.Compressed());

        Assert.Equal(2, queries);

        await fixture.Client.DisconnectAsync();
    }

    // Helpers

    private static ReadOnlyMemory<byte> Payload()
    {
        return Encoding.UTF8.GetBytes(new string('a', 4096));
    }

    private static void CaptureFrames(MeshClientFixture fixture, List<byte[]> sentFrames)
    {
        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) =>
            {
                lock (sentFrames)
                {
                    sentFrames.Add(frame.ToArray());
                }
            })
            .Returns(Task.CompletedTask);
    }

    /// <summary>
    /// Connects, then stands in for the hub's half of negotiation: answers capability queries with
    /// <paramref name="peerAlgorithmIds"/> and captures the message frames the client sends.
    /// </summary>
    /// <param name="fixture">The sending client's fixture.</param>
    /// <param name="sentFrames">Receives every message frame, with negotiation traffic filtered out.</param>
    /// <param name="peerAlgorithmIds">
    /// What the hub reports the recipient supports, or <see langword="null"/> to never answer at all.
    /// </param>
    /// <param name="onQuery">Invoked once per capability query the client actually sends.</param>
    private static async Task ConnectAndAnswerAsync(
        MeshClientFixture fixture,
        List<byte[]> sentFrames,
        IReadOnlyList<string>? peerAlgorithmIds,
        Action? onQuery = null)
    {
        fixture.SetupSuccessfulRegistration();
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "Sender");

        sentFrames.Clear();

        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) =>
            {
                byte[] sent = frame.ToArray();

                if (sent.Length >= 21 && sent[0] == (byte)MessageType.CompressionCapabilityRequest)
                {
                    onQuery?.Invoke();

                    if (peerAlgorithmIds is not null)
                    {
                        fixture.Inbound!.TryWrite(BuildCapabilityResponse(
                            BinaryPrimitives.ReadInt32BigEndian(sent.AsSpan(1, 4)), peerAlgorithmIds));
                    }

                    return;
                }

                if (sent[0] is (byte)MessageType.SendMessage or (byte)MessageType.SendMessageWithHeaders)
                {
                    lock (sentFrames)
                    {
                        sentFrames.Add(sent);
                    }
                }
            })
            .Returns(Task.CompletedTask);
    }

    private static async Task<byte[]> SendAndCaptureAsync(
        DeliveryOptions options, IReadOnlyList<string>? peerAlgorithmIds)
    {
        var fixture = new MeshClientFixture();
        var sentFrames = new List<byte[]>();

        await ConnectAndAnswerAsync(fixture, sentFrames, peerAlgorithmIds);
        await fixture.Client.SendAsync(Guid.NewGuid(), Payload(), options);

        byte[] frame = Assert.Single(sentFrames);
        await fixture.Client.DisconnectAsync();

        return frame;
    }

    private static async Task<MessageHeaders> SendAndReadHeadersAsync(
        DeliveryOptions options, IReadOnlyList<string>? peerAlgorithmIds)
    {
        return ReadHeaders(await SendAndCaptureAsync(options, peerAlgorithmIds));
    }

    private static MessageHeaders ReadHeaders(byte[] sendFrame)
    {
        Assert.Equal((byte)MessageType.SendMessageWithHeaders, sendFrame[0]);

        int headerBlockLength = BinaryPrimitives.ReadUInt16BigEndian(sendFrame.AsSpan(17, 2));

        return HeaderEnvelope.Read(sendFrame.AsSpan(19), headerBlockLength);
    }

    private static byte[] BuildCapabilityResponse(int correlationId, IReadOnlyList<string> algorithmIds)
    {
        int blockLength = CompressionCapabilityEnvelope.GetEncodedLength(algorithmIds);
        var response = new byte[6 + blockLength];
        response[0] = (byte)MessageType.CompressionCapabilityResponse;
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(1, 4), correlationId);
        response[5] = 0x01;
        CompressionCapabilityEnvelope.Write(algorithmIds, response.AsSpan(6));

        return response;
    }

    /// <summary>
    /// A hand-rolled controllable clock, matching <c>ChunkReassemblerTests</c>: the client only ever asks
    /// this for the current instant, so a package reference for one overridable member would cost the
    /// solution a dependency it does not otherwise need.
    /// </summary>
    private sealed class ControllableTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan delta) => _utcNow += delta;
    }
}
