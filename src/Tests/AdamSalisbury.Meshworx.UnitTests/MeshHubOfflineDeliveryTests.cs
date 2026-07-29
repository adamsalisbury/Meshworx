using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Hub-level tests for store-and-forward (issue #28): a direct message addressed to a client that is not
/// connected is held against that client's <em>name</em> and delivered when the name next registers,
/// instead of being dropped.
/// </summary>
/// <remarks>
/// Two synchronisation points make these deterministic rather than timing-dependent. The hub raises
/// <see cref="MeshHub.ClientDisconnected"/> only after it has finished retiring a connection —
/// <em>including</em> recording the name that id was reachable by — so waiting on that event is what
/// guarantees a subsequent send sees the recipient as offline rather than merely missing. And
/// <see cref="ObservableOfflineStore"/> signals each accepted message, so a test reconnects the recipient
/// only once the hub has actually stored what it is about to drain.
/// </remarks>
public sealed class MeshHubOfflineDeliveryTests
{
    private static readonly TimeSpan WaitTimeout = TestTimeouts.Wait;

    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task DirectMessage_ToADisconnectedClient_IsHeldAndDeliveredWhenItRegistersAgain()
    {
        var store = new ObservableOfflineStore(new InMemoryOfflineStore());
        var fixture = new MeshHubFixture(offlineStore: store);
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        Guid recipientId = await RegisterThenDisconnectAsync(fixture, "Recipient");

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(recipientId, [1, 2, 3]));
        await store.WaitForAcceptedAsync(1).WaitAsync(WaitTimeout);

        (RegisteredClient returned, FrameRecorder frames) =
            await fixture.RegisterClientWithRecorderAsync("Recipient");

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);

        Assert.Equal(sender.Id, new Guid(delivered.AsSpan(1, 16)));
        Assert.Equal([1, 2, 3], delivered.AsSpan(17).ToArray());

        returned.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task HeldMessages_AreDeliveredInArrivalOrder()
    {
        var store = new ObservableOfflineStore(new InMemoryOfflineStore());
        var fixture = new MeshHubFixture(offlineStore: store);
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        Guid recipientId = await RegisterThenDisconnectAsync(fixture, "Recipient");

        for (byte i = 0; i < 5; i++)
        {
            sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(recipientId, [i]));
        }

        await store.WaitForAcceptedAsync(5).WaitAsync(WaitTimeout);

        (RegisteredClient returned, FrameRecorder frames) =
            await fixture.RegisterClientWithRecorderAsync("Recipient");

        // Wait for the last one, then assert the whole sequence: a client's outbound queue is drained in
        // order, so the arrival of the fifth proves the four before it already landed.
        await frames.WaitForAsync(f => f[0] == 0x03 && f.Length == 18 && f[17] == 4).WaitAsync(WaitTimeout);

        byte[] bodies = [.. frames.Frames.Where(f => f[0] == 0x03).Select(f => f[17])];
        Assert.Equal([0, 1, 2, 3, 4], bodies);

        returned.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// With no store configured the hub behaves exactly as it did before the feature existed: the message
    /// is dropped, and reconnecting under the same name delivers nothing. Pinned by pairing the absence
    /// with a live message sent afterwards to the reconnected client's new id — the outbound queue is
    /// drained in order, so that message arriving first proves nothing was held.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task DirectMessage_ToADisconnectedClient_WithNoStoreConfigured_IsDroppedAsBefore()
    {
        var fixture = new MeshHubFixture();
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        Guid recipientId = await RegisterThenDisconnectAsync(fixture, "Recipient");

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(recipientId, [0xDE, 0xAD]));

        (RegisteredClient returned, FrameRecorder frames) =
            await fixture.RegisterClientWithRecorderAsync("Recipient");

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(returned.Id, [0xBE, 0xEF]));

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);

        Assert.Equal([0xBE, 0xEF], delivered.AsSpan(17).ToArray());

        returned.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// The stored message keeps its header block, and the frame shape is chosen from the version the
    /// <em>returning</em> connection negotiates — which is why the store holds the message's parts rather
    /// than a finished frame.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task HeldMessageWithHeaders_RecipientReturnsAtVersionFive_KeepsItsHeaderBlock()
    {
        var store = new ObservableOfflineStore(new InMemoryOfflineStore());
        var fixture = new MeshHubFixture(offlineStore: store);
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender", versionMax: 0x05);
        Guid recipientId = await RegisterThenDisconnectAsync(fixture, "Recipient", versionMax: 0x05);

        var headers = new MessageHeaders(new Dictionary<string, string> { ["trace"] = "abc" });
        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessageWithHeaders(recipientId, headers, [7]));
        await store.WaitForAcceptedAsync(1).WaitAsync(WaitTimeout);

        (RegisteredClient returned, FrameRecorder frames) =
            await fixture.RegisterClientWithRecorderAsync("Recipient", versionMax: 0x05);

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x12).WaitAsync(WaitTimeout);

        int headerLength = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(delivered.AsSpan(17, 2));
        MessageHeaders received = HeaderEnvelope.Read(delivered.AsSpan(19, headerLength), headerLength);

        Assert.Equal("abc", received["trace"]);
        Assert.Equal([7], delivered.AsSpan(19 + headerLength).ToArray());

        returned.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// The same stored message delivered to a client that comes back negotiating version 4 has its header
    /// block stripped and arrives as the plain frame — the recipient could not parse the header-bearing
    /// opcode at all.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task HeldMessageWithHeaders_RecipientReturnsAtVersionFour_HasItsHeaderBlockStripped()
    {
        var store = new ObservableOfflineStore(new InMemoryOfflineStore());
        var fixture = new MeshHubFixture(offlineStore: store);
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender", versionMax: 0x05);
        Guid recipientId = await RegisterThenDisconnectAsync(fixture, "Recipient", versionMax: 0x05);

        var headers = new MessageHeaders(new Dictionary<string, string> { ["trace"] = "abc" });
        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessageWithHeaders(recipientId, headers, [7]));
        await store.WaitForAcceptedAsync(1).WaitAsync(WaitTimeout);

        (RegisteredClient returned, FrameRecorder frames) =
            await fixture.RegisterClientWithRecorderAsync("Recipient", versionMax: 0x04);

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);

        Assert.Equal([7], delivered.AsSpan(17).ToArray());

        returned.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// Once the name is back — under a new id the sender does not know — its old id stops resolving.
    /// Holding a message for a client that is sitting there connected would put it somewhere only the
    /// <em>next</em> reconnect would drain, which is worse than telling the truth about the id being stale.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task DirectMessage_ToAStaleIdAfterTheNameRegisteredAgain_IsDroppedNotHeld()
    {
        var store = new ObservableOfflineStore(new InMemoryOfflineStore());
        var fixture = new MeshHubFixture(offlineStore: store);
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        Guid staleId = await RegisterThenDisconnectAsync(fixture, "Recipient");

        (RegisteredClient returned, FrameRecorder frames) =
            await fixture.RegisterClientWithRecorderAsync("Recipient");

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(staleId, [0xDE, 0xAD]));
        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(returned.Id, [0xBE, 0xEF]));

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);

        Assert.Equal([0xBE, 0xEF], delivered.AsSpan(17).ToArray());
        Assert.Equal(0, store.AcceptedCount);

        returned.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A store that refuses — full, or declining that name — drops the message, exactly as if
    /// store-and-forward were switched off. Pinned with the same paired-send technique as the
    /// no-store-configured case.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task DirectMessage_StoreRefusesIt_IsDropped()
    {
        var fixture = new MeshHubFixture(offlineStore: new RefusingOfflineStore());
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        Guid recipientId = await RegisterThenDisconnectAsync(fixture, "Recipient");

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(recipientId, [0xDE, 0xAD]));

        (RegisteredClient returned, FrameRecorder frames) =
            await fixture.RegisterClientWithRecorderAsync("Recipient");

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(returned.Id, [0xBE, 0xEF]));

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);

        Assert.Equal([0xBE, 0xEF], delivered.AsSpan(17).ToArray());

        returned.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A throwing store is a callback boundary like every other integrator seam on this hub: the message
    /// is dropped and the sender's connection carries on, rather than the exception faulting its receive
    /// loop.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task DirectMessage_StoreThrows_IsDroppedAndTheSenderStaysConnected()
    {
        var fixture = new MeshHubFixture(offlineStore: new ThrowingOfflineStore());
        await fixture.Hub.StartAsync();

        var sender = await fixture.RegisterMultiMessageClientAsync("Sender");
        Guid recipientId = await RegisterThenDisconnectAsync(fixture, "Recipient");

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(recipientId, [0xDE, 0xAD]));

        (RegisteredClient returned, FrameRecorder frames) =
            await fixture.RegisterClientWithRecorderAsync("Recipient");

        sender.EnqueueMessage(MeshHubFixture.CreateDirectMessage(returned.Id, [0xBE, 0xEF]));

        byte[] delivered = await frames.WaitForAsync(f => f[0] == 0x03).WaitAsync(WaitTimeout);

        Assert.Equal([0xBE, 0xEF], delivered.AsSpan(17).ToArray());
        Assert.True(fixture.Hub.IsClientRegistered(sender.Id));

        returned.Disconnect();
        sender.Disconnect();
        await fixture.Hub.StopAsync();
    }

    /// <summary>
    /// A store that throws on the drain must not stop the client connecting — registration has already
    /// completed by the time the drain runs, and failing it here would turn a store outage into an outage
    /// for every client that reconnects during it.
    /// </summary>
    [Fact(Timeout = TestTimeouts.ExtendedHarness)]
    public async Task Registration_StoreThrowsWhileDraining_ClientStillConnects()
    {
        var fixture = new MeshHubFixture(offlineStore: new ThrowingOfflineStore());
        await fixture.Hub.StartAsync();

        var client = await fixture.RegisterMultiMessageClientAsync("Worker");

        Assert.True(fixture.Hub.IsClientRegistered(client.Id));

        client.Disconnect();
        await fixture.Hub.StopAsync();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_NonPositiveOfflineStoreTimeout_ThrowsArgumentOutOfRangeException(int seconds)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new MeshHubFixture(
                offlineStore: new InMemoryOfflineStore(),
                offlineStoreTimeout: TimeSpan.FromSeconds(seconds)));
    }

    /// <summary>
    /// Registers a client, disconnects it, and waits until the hub has finished retiring the connection —
    /// which is the point at which the id it held becomes resolvable to its name for storage.
    /// </summary>
    private static async Task<Guid> RegisterThenDisconnectAsync(
        MeshHubFixture fixture, string name, byte versionMax = 0x04)
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
            RegisteredClient client = await fixture.RegisterClientAsync(name, versionMax: versionMax);
            client.Disconnect();
            await disconnected.Task.WaitAsync(WaitTimeout);
            return client.Id;
        }
        finally
        {
            fixture.Hub.ClientDisconnected -= OnDisconnected;
        }
    }

    /// <summary>
    /// Wraps a real store and lets a test wait until a given number of messages have been accepted, so a
    /// recipient is only reconnected once the hub has actually stored what it is about to drain.
    /// </summary>
    private sealed class ObservableOfflineStore(IOfflineStore inner) : IOfflineStore
    {
        private readonly Lock _lock = new();
        private readonly List<(int Target, TaskCompletionSource Completion)> _waiters = [];
        private int _accepted;

        public int AcceptedCount
        {
            get
            {
                lock (_lock)
                {
                    return _accepted;
                }
            }
        }

        public async ValueTask<bool> TryEnqueueAsync(
            string clientName, OfflineMessage message, CancellationToken cancellationToken = default)
        {
            bool stored = await inner.TryEnqueueAsync(clientName, message, cancellationToken)
                .ConfigureAwait(false);

            if (stored)
            {
                lock (_lock)
                {
                    _accepted++;
                    for (int i = _waiters.Count - 1; i >= 0; i--)
                    {
                        if (_accepted >= _waiters[i].Target)
                        {
                            _waiters[i].Completion.TrySetResult();
                            _waiters.RemoveAt(i);
                        }
                    }
                }
            }

            return stored;
        }

        public ValueTask<IReadOnlyList<OfflineMessage>> TakeAllAsync(
            string clientName, CancellationToken cancellationToken = default)
        {
            return inner.TakeAllAsync(clientName, cancellationToken);
        }

        public Task WaitForAcceptedAsync(int count)
        {
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            lock (_lock)
            {
                if (_accepted >= count)
                {
                    completion.SetResult();
                    return completion.Task;
                }

                _waiters.Add((count, completion));
            }

            return completion.Task;
        }
    }

    private sealed class RefusingOfflineStore : IOfflineStore
    {
        public ValueTask<bool> TryEnqueueAsync(
            string clientName, OfflineMessage message, CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(false);
        }

        public ValueTask<IReadOnlyList<OfflineMessage>> TakeAllAsync(
            string clientName, CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult<IReadOnlyList<OfflineMessage>>([]);
        }
    }

    private sealed class ThrowingOfflineStore : IOfflineStore
    {
        public ValueTask<bool> TryEnqueueAsync(
            string clientName, OfflineMessage message, CancellationToken cancellationToken = default)
        {
            throw new InvalidOperationException("The offline store is unavailable.");
        }

        public ValueTask<IReadOnlyList<OfflineMessage>> TakeAllAsync(
            string clientName, CancellationToken cancellationToken = default)
        {
            throw new InvalidOperationException("The offline store is unavailable.");
        }
    }
}
