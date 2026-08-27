using System.Buffers.Binary;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;

namespace AdamSalisbury.Meshworx.UnitTests.Compression;

/// <summary>
/// The hub's half of capability negotiation: it holds what each client advertised and hands it back on
/// request. It never interprets an algorithm id, and cannot compress or decompress anything.
/// </summary>
public sealed class MeshHubCompressionCapabilityTests
{
    [Fact(Timeout = 15000)]
    public async Task AdvertisedAlgorithms_AreReturnedToAnotherClientInOrder()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        MultiMessageRegisteredClient subject = await RegisterAsync(fixture, "Subject");
        (MultiMessageRegisteredClient asker, FrameRecorder frames) = await RegisterWithRecorderAsync(fixture, "Asker");

        subject.EnqueueMessage(BuildAdvertisement(["br", "deflate", "x-rle"]));
        await WaitForAdvertisementAsync(fixture, subject.Id, expectedCount: 3);

        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 7, subject.Id));

        byte[] response = await frames
            .WaitForAsync(f => f.Length >= 8 && f[0] == (byte)MessageType.CompressionCapabilityResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.Equal(7, BinaryPrimitives.ReadInt32BigEndian(response.AsSpan(1, 4)));
        Assert.Equal(0x01, response[5]);
        Assert.True(CompressionCapabilityEnvelope.TryRead(
            response.AsSpan(7), out IReadOnlyList<string> advertised));
        Assert.Equal(["br", "deflate", "x-rle"], advertised);
    }

    [Fact(Timeout = 15000)]
    public async Task ClientThatNeverAdvertised_IsReportedAsFoundWithAnEmptySet()
    {
        // Found, but with nothing to offer — which is a different answer from "no such client", and a
        // sender can act on either the same way.
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        MultiMessageRegisteredClient subject = await RegisterAsync(fixture, "Subject");
        (MultiMessageRegisteredClient asker, FrameRecorder frames) = await RegisterWithRecorderAsync(fixture, "Asker");

        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 1, subject.Id));

        byte[] response = await frames
            .WaitForAsync(f => f.Length >= 8 && f[0] == (byte)MessageType.CompressionCapabilityResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.Equal(0x01, response[5]);
        Assert.True(CompressionCapabilityEnvelope.TryRead(response.AsSpan(7), out IReadOnlyList<string> advertised));
        Assert.Empty(advertised);
    }

    [Fact(Timeout = 15000)]
    public async Task UnknownClient_IsAnsweredRatherThanIgnored()
    {
        // Answering "not found" is what keeps the asking client from waiting out its own timeout.
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        (MultiMessageRegisteredClient asker, FrameRecorder frames) = await RegisterWithRecorderAsync(fixture, "Asker");

        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 3, Guid.NewGuid()));

        byte[] response = await frames
            .WaitForAsync(f => f.Length >= 8 && f[0] == (byte)MessageType.CompressionCapabilityResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.Equal(3, BinaryPrimitives.ReadInt32BigEndian(response.AsSpan(1, 4)));
        Assert.Equal(0x00, response[5]);
        Assert.True(CompressionCapabilityEnvelope.TryRead(response.AsSpan(7), out IReadOnlyList<string> advertised));
        Assert.Empty(advertised);
    }

    /// <summary>
    /// Issue #76: the reply carries the subject's own negotiated version, because what a sender needs to
    /// know about a recipient is not only which algorithms it can read but which shapes of compressed
    /// message it can read them in — and a sender's own negotiated version answers only for its link to
    /// this hub.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task CapabilityResponse_CarriesTheSubjectsNegotiatedVersion()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        byte subjectVersion = (byte)(Protocol.ChunkedCompressionMinVersion - 1);
        MultiMessageRegisteredClient subject = await RegisterAsync(fixture, "Subject", subjectVersion);
        (MultiMessageRegisteredClient asker, FrameRecorder frames) = await RegisterWithRecorderAsync(fixture, "Asker");

        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 11, subject.Id));

        byte[] response = await frames
            .WaitForAsync(f => f.Length >= 8 && f[0] == (byte)MessageType.CompressionCapabilityResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.Equal(0x01, response[5]);

        // The subject's version, not the asker's — the two differ here deliberately.
        Assert.Equal(subjectVersion, response[6]);
    }

    /// <summary>
    /// A subject the hub does not hold has no version to report, so the byte is zero. Zero means unknown
    /// rather than "too old", which is the same thing <c>found = 0</c> already says.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task CapabilityResponse_ForAnUnknownClient_ReportsNoVersion()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        (MultiMessageRegisteredClient asker, FrameRecorder frames) = await RegisterWithRecorderAsync(fixture, "Asker");

        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 12, Guid.NewGuid()));

        byte[] response = await frames
            .WaitForAsync(f => f.Length >= 8 && f[0] == (byte)MessageType.CompressionCapabilityResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.Equal(0x00, response[5]);
        Assert.Equal(0, response[6]);
    }

    /// <summary>
    /// An asker below <see cref="Protocol.ChunkedCompressionMinVersion"/> gets the reply its version
    /// defines, byte for byte: no version field, and the envelope where it has always been. Which shape a
    /// reply takes is decided by the asking connection's version rather than by the subject's, since the
    /// asker has no other way to know how to read it.
    /// </summary>
    [Fact(Timeout = 15000)]
    public async Task CapabilityResponse_ToAnAskerBelowTheMinVersion_KeepsTheOlderShape()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        MultiMessageRegisteredClient subject = await RegisterAsync(fixture, "Subject");
        (MultiMessageRegisteredClient asker, FrameRecorder frames) = await RegisterWithRecorderAsync(
            fixture, "Asker", (byte)(Protocol.ChunkedCompressionMinVersion - 1));

        subject.EnqueueMessage(BuildAdvertisement(["br", "deflate"]));
        await WaitForAdvertisementAsync(fixture, subject.Id, expectedCount: 2);

        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 13, subject.Id));

        byte[] response = await frames
            .WaitForAsync(f => f.Length >= 7 && f[0] == (byte)MessageType.CompressionCapabilityResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.Equal(0x01, response[5]);
        Assert.True(CompressionCapabilityEnvelope.TryRead(
            response.AsSpan(6), out IReadOnlyList<string> advertised));
        Assert.Equal(["br", "deflate"], advertised);
    }

    [Fact(Timeout = 15000)]
    public async Task ReAdvertising_ReplacesTheSetWholesale()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        MultiMessageRegisteredClient subject = await RegisterAsync(fixture, "Subject");
        (MultiMessageRegisteredClient asker, FrameRecorder frames) = await RegisterWithRecorderAsync(fixture, "Asker");

        subject.EnqueueMessage(BuildAdvertisement(["br", "deflate"]));
        await WaitForAdvertisementAsync(fixture, subject.Id, expectedCount: 2);

        subject.EnqueueMessage(BuildAdvertisement(["zstd"]));
        await WaitForAdvertisementAsync(fixture, subject.Id, expectedCount: 1);

        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 9, subject.Id));

        byte[] response = await frames
            .WaitForAsync(f => f.Length >= 8 && f[0] == (byte)MessageType.CompressionCapabilityResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.True(CompressionCapabilityEnvelope.TryRead(response.AsSpan(7), out IReadOnlyList<string> advertised));
        Assert.Equal(["zstd"], advertised);
    }

    [Fact(Timeout = 15000)]
    public async Task MalformedAdvertisement_IsIgnoredWholesaleAndTheConnectionSurvives()
    {
        // Rejected in its entirety rather than partially applied, and silently — mirroring
        // SetClientAttributes, which has no reply either.
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        MultiMessageRegisteredClient subject = await RegisterAsync(fixture, "Subject");
        (MultiMessageRegisteredClient asker, FrameRecorder frames) = await RegisterWithRecorderAsync(fixture, "Asker");

        subject.EnqueueMessage(BuildAdvertisement(["br", "deflate"]));
        await WaitForAdvertisementAsync(fixture, subject.Id, expectedCount: 2);

        // Claims three ids but carries one: truncated, so none of it is believed.
        subject.EnqueueMessage([(byte)MessageType.AdvertiseCompression, 3, 2, (byte)'b', (byte)'r']);

        // A valid advertisement behind it proves the malformed one was processed and discarded rather
        // than merely still in flight.
        subject.EnqueueMessage(BuildAdvertisement(["br", "deflate", "x-rle"]));
        await WaitForAdvertisementAsync(fixture, subject.Id, expectedCount: 3);

        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 11, subject.Id));

        byte[] response = await frames
            .WaitForAsync(f => f.Length >= 8 && f[0] == (byte)MessageType.CompressionCapabilityResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.True(CompressionCapabilityEnvelope.TryRead(response.AsSpan(7), out IReadOnlyList<string> advertised));
        Assert.Equal(["br", "deflate", "x-rle"], advertised);
    }

    [Fact(Timeout = 15000)]
    public async Task OversizedAdvertisement_IsIgnored()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        MultiMessageRegisteredClient subject = await RegisterAsync(fixture, "Subject");
        (MultiMessageRegisteredClient asker, FrameRecorder frames) = await RegisterWithRecorderAsync(fixture, "Asker");

        string[] tooMany = [.. Enumerable.Range(0, Protocol.MaxAdvertisedCompressionAlgorithms + 1)
            .Select(i => $"alg-{i:D2}")];

        subject.EnqueueMessage(BuildAdvertisement(tooMany));
        subject.EnqueueMessage(BuildAdvertisement(["br"]));
        await WaitForAdvertisementAsync(fixture, subject.Id, expectedCount: 1);

        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 13, subject.Id));

        byte[] response = await frames
            .WaitForAsync(f => f.Length >= 8 && f[0] == (byte)MessageType.CompressionCapabilityResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.True(CompressionCapabilityEnvelope.TryRead(response.AsSpan(7), out IReadOnlyList<string> advertised));
        Assert.Equal(["br"], advertised);
    }

    [Fact(Timeout = 15000)]
    public async Task BelowTheNegotiationVersion_BothFramesAreIgnored()
    {
        // Gated from the outset, as client attributes and presence were: an older connection gets the
        // unrecognised-opcode treatment, not a reply.
        byte older = (byte)(Protocol.CompressionNegotiationMinVersion - 1);

        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        MultiMessageRegisteredClient subject = await RegisterAsync(fixture, "Subject", older);
        (MultiMessageRegisteredClient asker, FrameRecorder frames) =
            await RegisterWithRecorderAsync(fixture, "Asker", older);

        subject.EnqueueMessage(BuildAdvertisement(["br"]));
        asker.EnqueueMessage(BuildCapabilityRequest(correlationId: 5, subject.Id));

        // No capability response is ever sent. Proven by a lookup on the same connection afterwards:
        // outbound frames drain in order, so its arrival means nothing was queued ahead of it.
        asker.EnqueueMessage(MeshHubFixture.CreateLookupRequest(correlationId: 6, "Subject"));

        byte[] lookup = await frames
            .WaitForAsync(f => f.Length >= 6 && f[0] == (byte)MessageType.ClientLookupResponse)
            .WaitAsync(TestTimeouts.Wait);

        Assert.Equal(6, BinaryPrimitives.ReadInt32BigEndian(lookup.AsSpan(1, 4)));
        Assert.DoesNotContain(
            frames.Frames, f => f.Length > 0 && f[0] == (byte)MessageType.CompressionCapabilityResponse);
    }

    // Helpers

    private static Task<MultiMessageRegisteredClient> RegisterAsync(
        MeshHubFixture fixture, string name, byte? version = null)
    {
        byte negotiated = version ?? Protocol.MaxSupportedVersion;

        return fixture.RegisterMultiMessageClientAsync(name, versionMin: negotiated, versionMax: negotiated);
    }

    private static async Task<(MultiMessageRegisteredClient Client, FrameRecorder Frames)>
        RegisterWithRecorderAsync(MeshHubFixture fixture, string name, byte? version = null)
    {
        MultiMessageRegisteredClient client = await RegisterAsync(fixture, name, version);
        var recorder = new FrameRecorder(client.Transport);

        return (client, recorder);
    }

    /// <summary>
    /// Waits until the hub has processed an advertisement, by asking it what the subject now advertises
    /// through a query of its own — the frame has no reply, so there is nothing else to wait on.
    /// </summary>
    private static async Task WaitForAdvertisementAsync(
        MeshHubFixture fixture, Guid subjectId, int expectedCount)
    {
        for (int attempt = 0; attempt < 500; attempt++)
        {
            if (fixture.Hub.GetAdvertisedCompressionAlgorithmsForTesting(subjectId)?.Count == expectedCount)
            {
                return;
            }

            await Task.Delay(10);
        }

        Assert.Fail($"Client {subjectId} never advertised {expectedCount} algorithm(s).");
    }

    private static byte[] BuildAdvertisement(IReadOnlyList<string> algorithmIds)
    {
        var frame = new byte[1 + CompressionCapabilityEnvelope.GetEncodedLength(algorithmIds)];
        frame[0] = (byte)MessageType.AdvertiseCompression;
        CompressionCapabilityEnvelope.Write(algorithmIds, frame.AsSpan(1));

        return frame;
    }

    private static byte[] BuildCapabilityRequest(int correlationId, Guid subjectId)
    {
        var frame = new byte[21];
        frame[0] = (byte)MessageType.CompressionCapabilityRequest;
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(1, 4), correlationId);
        subjectId.TryWriteBytes(frame.AsSpan(5));

        return frame;
    }
}
