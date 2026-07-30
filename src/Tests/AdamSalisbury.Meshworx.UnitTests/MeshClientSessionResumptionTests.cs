using System.Buffers.Binary;
using System.Text;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Client-side tests for session resumption (issue #43): holding the token a hub issues, presenting it
/// on the next connect, and — whatever the answer — ending up connected either way.
/// </summary>
/// <remarks>
/// The hub's reply is scripted <em>in response to observing the client's <c>ResumeSession</c> send</em>
/// rather than queued ahead of time. That mirrors what a real hub does, and it is what makes these
/// deterministic: a pre-queued reply could be read by the receive loop before <c>ConnectAsync</c> has
/// registered its pending-resume completion, which no real hub can do because it has not been asked yet.
/// </remarks>
public sealed class MeshClientSessionResumptionTests
{
    private static readonly byte[] IssuedToken =
        [.. Enumerable.Range(0, Protocol.SessionTokenLength).Select(i => (byte)i)];

    private static readonly byte[] RenewedToken =
        [.. Enumerable.Range(0, Protocol.SessionTokenLength).Select(i => (byte)(255 - i))];

    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task ConnectAsync_HubIssuesAToken_StillConnectsAndDoesNotReportAResumption()
    {
        var harness = new ResumptionHarness(issuedToken: IssuedToken);

        await harness.Client.ConnectAsync(harness.Transport.Object, "Worker");

        Assert.True(harness.Client.IsConnected);
        Assert.False(harness.Client.SessionResumed);
        Assert.Empty(harness.ResumeAttempts);

        await harness.Client.DisposeAsync();
    }

    /// <summary>
    /// The token from the first connection is presented on the second, and the hub's acceptance replaces
    /// the client's id with the reclaimed one.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ConnectAsync_AfterATokenWasIssued_PresentsItAndAdoptsTheResumedId()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var first = new ResumptionHarness(issuedToken: IssuedToken, client: client);

        await client.ConnectAsync(first.Transport.Object, "Worker");
        Guid firstId = client.Id;
        await client.DisconnectAsync();

        Guid resumedId = firstId;
        var second = new ResumptionHarness(
            issuedToken: IssuedToken, client: client, resumeReply: ResumeReply.Accept(resumedId, RenewedToken));

        await client.ConnectAsync(second.Transport.Object, "Worker");

        Assert.Equal([IssuedToken], second.ResumeAttempts);
        Assert.Equal(resumedId, client.Id);
        Assert.True(client.SessionResumed);

        await client.DisposeAsync();
    }

    /// <summary>
    /// A refusal is not a failure: the connect succeeds, the client keeps the identity it just
    /// registered with, and <see cref="IMeshClient.SessionResumed"/> reports that nothing was reclaimed.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ConnectAsync_HubRefusesTheResume_StaysConnectedOnTheFreshIdentity()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var first = new ResumptionHarness(issuedToken: IssuedToken, client: client);

        await client.ConnectAsync(first.Transport.Object, "Worker");
        await client.DisconnectAsync();

        var second = new ResumptionHarness(
            issuedToken: IssuedToken, client: client, resumeReply: ResumeReply.Refuse());

        await client.ConnectAsync(second.Transport.Object, "Worker");

        Assert.Single(second.ResumeAttempts);
        Assert.True(client.IsConnected);
        Assert.False(client.SessionResumed);
        Assert.Equal(second.AssignedId, client.Id);

        await client.DisposeAsync();
    }

    /// <summary>
    /// A token belongs to the name it was issued to. Connecting under a different name must not present
    /// it — the hub would refuse it anyway, but the client should not be offering one identity's
    /// credential while claiming to be another.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ConnectAsync_UnderADifferentName_DoesNotPresentTheToken()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var first = new ResumptionHarness(issuedToken: IssuedToken, client: client);

        await client.ConnectAsync(first.Transport.Object, "Worker");
        await client.DisconnectAsync();

        var second = new ResumptionHarness(issuedToken: IssuedToken, client: client);
        await client.ConnectAsync(second.Transport.Object, "SomebodyElse");

        Assert.Empty(second.ResumeAttempts);

        await client.DisposeAsync();
    }

    /// <summary>
    /// A hub that issues no token — resumption switched off, or a build that predates it — leaves the
    /// client with nothing to present, so the reconnect costs no extra round trip.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ConnectAsync_HubIssuedNoToken_AttemptsNoResumption()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var first = new ResumptionHarness(issuedToken: null, client: client);

        await client.ConnectAsync(first.Transport.Object, "Worker");
        await client.DisconnectAsync();

        var second = new ResumptionHarness(issuedToken: null, client: client);
        await client.ConnectAsync(second.Transport.Object, "Worker");

        Assert.Empty(second.ResumeAttempts);
        Assert.False(client.SessionResumed);

        await client.DisposeAsync();
    }

    /// <summary>
    /// Below protocol version 6 there is no resumption to attempt, so no token is retained even if one
    /// somehow arrived — both ends gate on the negotiated version, not on whether a token is in hand.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ConnectAsync_NegotiatedBelowVersionSix_AttemptsNoResumption()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var first = new ResumptionHarness(issuedToken: IssuedToken, client: client, negotiatedVersion: 0x05);

        await client.ConnectAsync(first.Transport.Object, "Worker");
        await client.DisconnectAsync();

        var second = new ResumptionHarness(issuedToken: IssuedToken, client: client, negotiatedVersion: 0x05);
        await client.ConnectAsync(second.Transport.Object, "Worker");

        Assert.Empty(second.ResumeAttempts);

        await client.DisposeAsync();
    }

    /// <summary>
    /// A version-7 hub's acceptance carries the group memberships it restored, and the client repopulates
    /// <see cref="IMeshClient.JoinedGroups"/> from them directly, without re-joining anything (issue #109).
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ConnectAsync_ResumeAcceptedWithRestoredGroups_RepopulatesJoinedGroups()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var first = new ResumptionHarness(issuedToken: IssuedToken, client: client);

        await client.ConnectAsync(first.Transport.Object, "Worker");
        await client.DisconnectAsync();

        // Nothing left over from before the drop — the resumed membership must come entirely from the
        // reply, not from anything CleanUpAsync failed to clear.
        Assert.Empty(client.JoinedGroups);

        var second = new ResumptionHarness(
            issuedToken: IssuedToken,
            client: client,
            resumeReply: ResumeReply.Accept(Guid.NewGuid(), RenewedToken, ["news", "alerts"]));

        await client.ConnectAsync(second.Transport.Object, "Worker");

        Assert.True(client.SessionResumed);
        Assert.Equal(["alerts", "news"], client.JoinedGroups.Order(StringComparer.Ordinal));

        await client.DisposeAsync();
    }

    /// <summary>
    /// A resume that restores no memberships at all still repopulates cleanly to an empty set — the
    /// distinction is not "the block was absent" versus "the block listed nothing", it is real absence of
    /// membership either way.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ConnectAsync_ResumeAcceptedWithNoRestoredGroups_LeavesJoinedGroupsEmpty()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var first = new ResumptionHarness(issuedToken: IssuedToken, client: client);

        await client.ConnectAsync(first.Transport.Object, "Worker");
        await client.DisconnectAsync();

        var second = new ResumptionHarness(
            issuedToken: IssuedToken,
            client: client,
            resumeReply: ResumeReply.Accept(Guid.NewGuid(), RenewedToken, []));

        await client.ConnectAsync(second.Transport.Object, "Worker");

        Assert.True(client.SessionResumed);
        Assert.Empty(client.JoinedGroups);

        await client.DisposeAsync();
    }

    /// <summary>
    /// A group block truncated mid-name — the kind of malformed frame a well-behaved hub never sends, but
    /// nothing on the wire between them guarantees — is not acted on at all: the resume itself still
    /// succeeds, and <see cref="IMeshClient.JoinedGroups"/> is left exactly as it was rather than being
    /// partially populated from a read that ran off the end of the frame.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ConnectAsync_ResumeAcceptedWithATruncatedGroupBlock_LeavesJoinedGroupsUntouched()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var first = new ResumptionHarness(issuedToken: IssuedToken, client: client);

        await client.ConnectAsync(first.Transport.Object, "Worker");
        await client.DisconnectAsync();

        // A well-formed SessionResumed reply — id, token length, token — followed by a group block that
        // claims two entries but supplies only one, and that one truncated before its own name bytes end.
        byte[] renewedToken = RenewedToken;
        var truncated = new byte[1 + 16 + 2 + renewedToken.Length + 2 + 2 + 3];
        truncated[0] = 0x17; // SessionResumed
        Guid.NewGuid().TryWriteBytes(truncated.AsSpan(1, 16));
        BinaryPrimitives.WriteUInt16BigEndian(truncated.AsSpan(17, 2), (ushort)renewedToken.Length);
        renewedToken.CopyTo(truncated, 19);

        int groupsOffset = 19 + renewedToken.Length;
        BinaryPrimitives.WriteUInt16BigEndian(truncated.AsSpan(groupsOffset, 2), 2); // claims two groups
        BinaryPrimitives.WriteUInt16BigEndian(truncated.AsSpan(groupsOffset + 2, 2), 10); // first name is 10 bytes
        // ...but only 3 bytes of that name, and no second entry, actually follow.
        Encoding.UTF8.GetBytes("abc").CopyTo(truncated, groupsOffset + 4);

        var second = new ResumptionHarness(issuedToken: IssuedToken, client: client, rawResumeReply: truncated);

        await client.ConnectAsync(second.Transport.Object, "Worker");

        Assert.True(client.SessionResumed);
        Assert.Empty(client.JoinedGroups);

        await client.DisposeAsync();
    }

    /// <summary>
    /// A hub that only negotiated version 6 sends the reply shape version 6 always produced — no group
    /// block at all. <see cref="IMeshClient.JoinedGroups"/> is left exactly as it was, which is empty:
    /// the pre-#109 behaviour for a peer this old, not a regression this fix introduces for it.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ConnectAsync_ResumeAcceptedByAVersionSixHub_LeavesJoinedGroupsEmpty()
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var first = new ResumptionHarness(issuedToken: IssuedToken, client: client, negotiatedVersion: 0x06);

        await client.ConnectAsync(first.Transport.Object, "Worker");
        await client.DisconnectAsync();

        var second = new ResumptionHarness(
            issuedToken: IssuedToken,
            client: client,
            resumeReply: ResumeReply.Accept(Guid.NewGuid(), RenewedToken, groups: null),
            negotiatedVersion: 0x06);

        await client.ConnectAsync(second.Transport.Object, "Worker");

        Assert.True(client.SessionResumed);
        Assert.Empty(client.JoinedGroups);

        await client.DisposeAsync();
    }

    private sealed record ResumeReply(bool Accepted, Guid ResumedId, byte[] RenewedToken, IReadOnlyList<string>? Groups)
    {
        public static ResumeReply Accept(Guid resumedId, byte[] renewedToken, IReadOnlyList<string>? groups = null) =>
            new(true, resumedId, renewedToken, groups);

        public static ResumeReply Refuse() => new(false, Guid.Empty, [], Groups: null);
    }

    /// <summary>
    /// A mock transport that completes a registration handshake and, when it sees a
    /// <c>ResumeSession</c> frame go out, answers it the way a hub would.
    /// </summary>
    private sealed class ResumptionHarness
    {
        private readonly Channel<byte[]?> _inbound = Channel.CreateUnbounded<byte[]?>();
        private readonly List<byte[]> _resumeAttempts = [];
        private readonly Lock _lock = new();

        public Mock<ITransport> Transport { get; } = new();

        public MeshClient Client { get; }

        public Guid AssignedId { get; } = Guid.NewGuid();

        public IReadOnlyList<byte[]> ResumeAttempts
        {
            get
            {
                lock (_lock)
                {
                    return [.. _resumeAttempts];
                }
            }
        }

        public ResumptionHarness(
            byte[]? issuedToken,
            MeshClient? client = null,
            ResumeReply? resumeReply = null,
            byte negotiatedVersion = Protocol.MaxSupportedVersion,
            byte[]? rawResumeReply = null)
        {
            Client = client ?? new MeshClient(new Mock<ILogger<MeshClient>>().Object);

            Transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);

            var registrationResponse = new byte[issuedToken is null ? 18 : 20 + issuedToken.Length];
            registrationResponse[0] = 0x01; // RegistrationComplete
            AssignedId.TryWriteBytes(registrationResponse.AsSpan(1, 16));
            registrationResponse[17] = negotiatedVersion;

            if (issuedToken is not null)
            {
                System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(
                    registrationResponse.AsSpan(18, 2), (ushort)issuedToken.Length);
                issuedToken.CopyTo(registrationResponse, 20);
            }

            _inbound.Writer.TryWrite(registrationResponse);

            Transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
                .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) =>
                {
                    if (data.Length < 1 || data.Span[0] != 0x16)
                    {
                        return;
                    }

                    lock (_lock)
                    {
                        _resumeAttempts.Add(data.Span[1..].ToArray());
                    }

                    if (rawResumeReply is not null)
                    {
                        _inbound.Writer.TryWrite(rawResumeReply);
                        return;
                    }

                    if (resumeReply is null)
                    {
                        return;
                    }

                    _inbound.Writer.TryWrite(resumeReply.Accepted
                        ? BuildResumed(resumeReply.ResumedId, resumeReply.RenewedToken, resumeReply.Groups)
                        : [0x18]);
                })
                .Returns(Task.CompletedTask);

            Transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(async ct => await _inbound.Reader.ReadAsync(ct).ConfigureAwait(false));
        }

        private static byte[] BuildResumed(Guid resumedId, byte[] renewedToken, IReadOnlyList<string>? groups)
        {
            if (groups is null)
            {
                var payloadWithoutGroups = new byte[1 + 16 + 2 + renewedToken.Length];
                payloadWithoutGroups[0] = 0x17; // SessionResumed
                resumedId.TryWriteBytes(payloadWithoutGroups.AsSpan(1, 16));
                BinaryPrimitives.WriteUInt16BigEndian(
                    payloadWithoutGroups.AsSpan(17, 2), (ushort)renewedToken.Length);
                renewedToken.CopyTo(payloadWithoutGroups, 19);
                return payloadWithoutGroups;
            }

            byte[][] groupNameBytes = [.. groups.Select(Encoding.UTF8.GetBytes)];
            int groupsBlockLength = 2 + groupNameBytes.Sum(nameBytes => 2 + nameBytes.Length);

            var payload = new byte[1 + 16 + 2 + renewedToken.Length + groupsBlockLength];
            payload[0] = 0x17; // SessionResumed
            resumedId.TryWriteBytes(payload.AsSpan(1, 16));
            BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(17, 2), (ushort)renewedToken.Length);
            renewedToken.CopyTo(payload, 19);

            int offset = 19 + renewedToken.Length;
            BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(offset, 2), (ushort)groupNameBytes.Length);
            offset += 2;

            foreach (byte[] nameBytes in groupNameBytes)
            {
                BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(offset, 2), (ushort)nameBytes.Length);
                offset += 2;
                nameBytes.CopyTo(payload, offset);
                offset += nameBytes.Length;
            }

            return payload;
        }
    }
}
