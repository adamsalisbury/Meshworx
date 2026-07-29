using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Hub-level tests for session resumption (issue #43): a client that reconnects within the resumption
/// window and presents the token it was issued reclaims its previous id and group memberships, rather
/// than being assigned a fresh identity that leaves every peer's cached id pointing at nothing.
/// </summary>
/// <remarks>
/// These drive the wire protocol directly through the mock transport rather than through
/// <see cref="MeshClient"/>, so they pin the hub's half of the contract on its own — including the
/// refusal paths, which a well-behaved client would never provoke.
/// </remarks>
public sealed class MeshHubSessionResumptionTests
{
    private static readonly TimeSpan WaitTimeout = TestTimeouts.Wait;
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(5);

    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task Registration_ResumptionEnabled_IssuesAToken()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: Window);
        await fixture.Hub.StartAsync();

        RegisteredClient client = await fixture.RegisterClientAsync("Worker", versionMax: 0x06);

        Assert.Equal(0x06, client.NegotiatedProtocolVersion);
        Assert.NotNull(client.SessionToken);
        Assert.Equal(Protocol.SessionTokenLength, client.SessionToken!.Length);

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// With resumption switched off — the default — the registration reply is byte-for-byte the 18-byte
    /// frame every version before 6 produced, whatever version was negotiated.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task Registration_ResumptionDisabled_IssuesNoTokenAndKeepsTheOriginalReplyShape()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        RegisteredClient client = await fixture.RegisterClientAsync("Worker", versionMax: 0x06);

        Assert.Equal(18, client.RegistrationResponse.Length);
        Assert.Null(client.SessionToken);

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A connection that negotiated below version 6 is never issued a token, even on a hub with
    /// resumption enabled — the opcodes that would spend it are gated on the same version.
    /// </summary>
    [Fact(Timeout = TestTimeouts.Harness)]
    public async Task Registration_NegotiatedBelowVersionSix_IssuesNoToken()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: Window);
        await fixture.Hub.StartAsync();

        RegisteredClient client = await fixture.RegisterClientAsync("Worker", versionMax: 0x05);

        Assert.Equal(0x05, client.NegotiatedProtocolVersion);
        Assert.Equal(18, client.RegistrationResponse.Length);

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ResumeSession_ValidToken_ReclaimsThePreviousId()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: Window);
        await fixture.Hub.StartAsync();

        (Guid originalId, byte[] token) = await RegisterThenDisconnectAsync(fixture, "Worker");

        var returning = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        var frames = new FrameRecorder(returning.Transport);

        Assert.NotEqual(originalId, returning.Id);

        returning.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(token));

        byte[] resumed = await frames.WaitForAsync(f => f[0] == 0x17).WaitAsync(WaitTimeout);

        Assert.Equal(originalId, new Guid(resumed.AsSpan(1, 16)));
        Assert.True(fixture.Hub.IsClientRegistered(originalId));
        Assert.False(fixture.Hub.IsClientRegistered(returning.Id));

        returning.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// The reclaimed id is not just echoed back — it routes. A peer holding the id from before the drop
    /// reaches the resumed client without ever looking it up again, which is the entire point of the
    /// feature.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ResumeSession_ValidToken_PeersCachedIdStillDelivers()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: Window);
        await fixture.Hub.StartAsync();

        var peer = await fixture.RegisterMultiMessageClientAsync("Peer", versionMax: 0x06);
        (Guid originalId, byte[] token) = await RegisterThenDisconnectAsync(fixture, "Worker");

        var returning = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        var frames = new FrameRecorder(returning.Transport);

        returning.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(token));
        await frames.WaitForAsync(f => f[0] == 0x17).WaitAsync(WaitTimeout);

        peer.EnqueueMessage(MeshHubFixture.CreateDirectMessage(originalId, [1, 2, 3]));

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);

        Assert.Equal(peer.Id, new Guid(delivered.AsSpan(1, 16)));
        Assert.Equal([1, 2, 3], delivered.AsSpan(17).ToArray());

        returning.Disconnect();
        peer.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// Group memberships come back with the identity, so a group message reaches the resumed client
    /// without it re-joining anything.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ResumeSession_ValidToken_RestoresGroupMembership()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: Window);
        await fixture.Hub.StartAsync();

        var member = await fixture.RegisterMultiMessageClientAsync("Member", versionMax: 0x06);
        member.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("news"));

        var worker = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        byte[] token = ExtractToken(worker.RegistrationResponse);
        worker.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("news"));

        // A lookup on the same connection is a barrier: frames from one client are processed in order,
        // so its response proves the join above has already been applied.
        await AwaitLookupBarrierAsync(fixture, worker, "Member");

        var disconnected = new TaskCompletionSource();
        void OnDisconnected(object? _, ClientConnectionEventArgs e)
        {
            if (e.ClientName == "Worker")
            {
                disconnected.TrySetResult();
            }
        }

        fixture.Hub.ClientDisconnected += OnDisconnected;
        worker.Disconnect();
        await disconnected.Task.WaitAsync(WaitTimeout);
        fixture.Hub.ClientDisconnected -= OnDisconnected;

        var returning = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        var frames = new FrameRecorder(returning.Transport);
        returning.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(token));
        await frames.WaitForAsync(f => f[0] == 0x17).WaitAsync(WaitTimeout);

        member.EnqueueMessage(MeshHubFixture.CreateGroupMessage("news", [9]));

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x0F).WaitAsync(WaitTimeout);
        Assert.Equal(member.Id, new Guid(delivered.AsSpan(1, 16)));

        returning.Disconnect();
        member.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A restore runs every group back through the authoriser rather than reinstating it, so an
    /// authoriser that has since changed its mind is obeyed. This is the resumption-shaped counterpart to
    /// <c>JoinGroup_AfterReconnect_IsAuthorisedAgainRatherThanRestored</c>, and it exists because
    /// resumption is exactly the sort of state-reinstating shortcut that would otherwise become the back
    /// door around that rule.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ResumeSession_AuthoriserNowRefuses_DoesNotRestoreThatGroup()
    {
        var allowJoins = true;
        GroupAuthoriser authoriser = (_, _) => ValueTask.FromResult(allowJoins);

        var fixture = new MeshHubFixture(sessionResumptionWindow: Window, groupAuthoriser: authoriser);
        await fixture.Hub.StartAsync();

        var member = await fixture.RegisterMultiMessageClientAsync("Member", versionMax: 0x06);
        member.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("news"));

        var worker = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        byte[] token = ExtractToken(worker.RegistrationResponse);
        worker.EnqueueMessage(MeshHubFixture.CreateJoinGroupRequest("news"));
        await AwaitLookupBarrierAsync(fixture, worker, "Member");

        var disconnected = new TaskCompletionSource();
        void OnDisconnected(object? _, ClientConnectionEventArgs e)
        {
            if (e.ClientName == "Worker")
            {
                disconnected.TrySetResult();
            }
        }

        fixture.Hub.ClientDisconnected += OnDisconnected;
        worker.Disconnect();
        await disconnected.Task.WaitAsync(WaitTimeout);
        fixture.Hub.ClientDisconnected -= OnDisconnected;

        allowJoins = false;

        var returning = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        var frames = new FrameRecorder(returning.Transport);
        returning.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(token));
        await frames.WaitForAsync(f => f[0] == 0x17).WaitAsync(WaitTimeout);

        // The identity came back; the membership did not. Paired with a direct message to the resumed
        // client, which the hub certainly will deliver — its arrival proves the group message that was
        // sent first was never queued.
        member.EnqueueMessage(MeshHubFixture.CreateGroupMessage("news", [9]));
        member.EnqueueMessage(MeshHubFixture.CreateDirectMessage(
            new Guid(frames.Frames.First(f => f[0] == 0x17).AsSpan(1, 16)), [7]));

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);
        Assert.Equal([7], delivered.AsSpan(17).ToArray());
        Assert.DoesNotContain(frames.Frames, f => f[0] == 0x0F);

        returning.Disconnect();
        member.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// The token is single-use: the fresh one issued alongside the resumption is the only one that works
    /// afterwards, so a token captured off the wire cannot be replayed to steal the identity later.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ResumeSession_TokenPresentedTwice_IsRefusedTheSecondTime()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: Window);
        await fixture.Hub.StartAsync();

        (_, byte[] token) = await RegisterThenDisconnectAsync(fixture, "Worker");

        var first = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        var firstFrames = new FrameRecorder(first.Transport);
        first.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(token));
        await firstFrames.WaitForAsync(f => f[0] == 0x17).WaitAsync(WaitTimeout);

        var disconnected = new TaskCompletionSource();
        void OnDisconnected(object? _, ClientConnectionEventArgs e)
        {
            if (e.ClientName == "Worker")
            {
                disconnected.TrySetResult();
            }
        }

        fixture.Hub.ClientDisconnected += OnDisconnected;
        first.Disconnect();
        await disconnected.Task.WaitAsync(WaitTimeout);
        fixture.Hub.ClientDisconnected -= OnDisconnected;

        var second = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        var secondFrames = new FrameRecorder(second.Transport);
        second.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(token));

        await secondFrames.WaitForAsync(f => f[0] == 0x18).WaitAsync(WaitTimeout);

        second.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A token only reclaims the name it was issued to. Without this, a client that can obtain any
    /// token — by capturing one, or by being handed one — could take over an identity belonging to a
    /// name it has no claim to.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ResumeSession_TokenBelongsToADifferentName_IsRefused()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: Window);
        await fixture.Hub.StartAsync();

        (_, byte[] workerToken) = await RegisterThenDisconnectAsync(fixture, "Worker");

        var impostor = await fixture.RegisterMultiMessageClientAsync("Impostor", versionMax: 0x06);
        var frames = new FrameRecorder(impostor.Transport);

        impostor.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(workerToken));

        await frames.WaitForAsync(f => f[0] == 0x18).WaitAsync(WaitTimeout);
        Assert.True(fixture.Hub.IsClientRegistered(impostor.Id));

        impostor.Disconnect();
        await fixture.Hub.StopAsync();
    }

    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ResumeSession_UnknownToken_IsRefusedAndLeavesTheFreshIdentityInPlace()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: Window);
        await fixture.Hub.StartAsync();

        var client = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        var frames = new FrameRecorder(client.Transport);

        client.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(new byte[Protocol.SessionTokenLength]));

        await frames.WaitForAsync(f => f[0] == 0x18).WaitAsync(WaitTimeout);
        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A session whose connection is still live is not resumable. A token reclaims an identity nobody is
    /// using; it is never a way to take one off somebody who is.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ResumeSession_SessionStillHeldByALiveConnection_IsRefused()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: Window);
        await fixture.Hub.StartAsync();

        var live = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        byte[] token = ExtractToken(live.RegistrationResponse);
        var frames = new FrameRecorder(live.Transport);

        live.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(token));

        await frames.WaitForAsync(f => f[0] == 0x18).WaitAsync(WaitTimeout);

        live.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// After the resumption window closes the identity is gone; the client keeps the fresh one it
    /// registered with. Driven by a one-tick window rather than by waiting out a realistic one.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task ResumeSession_WindowHasClosed_IsRefused()
    {
        var fixture = new MeshHubFixture(sessionResumptionWindow: TimeSpan.FromTicks(1));
        await fixture.Hub.StartAsync();

        (_, byte[] token) = await RegisterThenDisconnectAsync(fixture, "Worker");

        var returning = await fixture.RegisterMultiMessageClientAsync("Worker", versionMax: 0x06);
        var frames = new FrameRecorder(returning.Transport);

        returning.EnqueueMessage(MeshHubFixture.CreateResumeSessionRequest(token));

        await frames.WaitForAsync(f => f[0] == 0x18).WaitAsync(WaitTimeout);
        Assert.True(fixture.Hub.IsClientRegistered(returning.Id));

        returning.Disconnect();
        await fixture.Hub.StopAsync();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_NonPositiveResumptionWindow_ThrowsArgumentOutOfRangeException(int seconds)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new MeshHubFixture(sessionResumptionWindow: TimeSpan.FromSeconds(seconds)));
    }

    private static byte[] ExtractToken(byte[] registrationResponse)
    {
        int tokenLength = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(
            registrationResponse.AsSpan(18, 2));
        return registrationResponse.AsSpan(20, tokenLength).ToArray();
    }

    /// <summary>
    /// Sends a lookup on the given client's own connection and waits for the reply, as a barrier proving
    /// every frame that client sent beforehand has already been processed.
    /// </summary>
    private static async Task AwaitLookupBarrierAsync(
        MeshHubFixture fixture, MultiMessageRegisteredClient client, string nameToLookUp)
    {
        var frames = new FrameRecorder(client.Transport);
        client.EnqueueMessage(MeshHubFixture.CreateLookupRequest(1234, nameToLookUp));
        await frames.WaitForAsync(f => f[0] == 0x07).WaitAsync(WaitTimeout);
    }

    /// <summary>
    /// Registers a client at version 6, captures its id and resumption token, then disconnects it and
    /// waits until the hub has finished retiring the connection — the point at which its session becomes
    /// dormant and therefore resumable.
    /// </summary>
    private static async Task<(Guid Id, byte[] Token)> RegisterThenDisconnectAsync(
        MeshHubFixture fixture, string name)
    {
        var disconnected = new TaskCompletionSource();
        void OnDisconnected(object? sender, ClientConnectionEventArgs e)
        {
            if (e.ClientName == name)
            {
                disconnected.TrySetResult();
            }
        }

        fixture.Hub.ClientDisconnected += OnDisconnected;
        try
        {
            RegisteredClient client = await fixture.RegisterClientAsync(name, versionMax: 0x06);
            byte[] token = client.SessionToken!;
            client.Disconnect();
            await disconnected.Task.WaitAsync(WaitTimeout);
            return (client.Id, token);
        }
        finally
        {
            fixture.Hub.ClientDisconnected -= OnDisconnected;
        }
    }
}
