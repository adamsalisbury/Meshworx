using StackExchange.Redis;

namespace AdamSalisbury.Meshworx.Backplane.Redis.UnitTests;

/// <summary>
/// Exercises <see cref="RedisHubBackplane"/> against a real local Redis server
/// (<c>localhost:6379</c>) — the CI workflow runs one as a service container for exactly this. Each test
/// uses its own randomly-named channel/directory key so tests never see each other's pub/sub traffic or
/// directory entries even when run in parallel against the same server.
/// </summary>
public sealed class RedisHubBackplaneTests : IAsyncLifetime
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private ConnectionMultiplexer _connection = null!;

    public async Task InitializeAsync()
    {
        _connection = await ConnectionMultiplexer.ConnectAsync("localhost:6379,abortConnect=false");
    }

    public async Task DisposeAsync()
    {
        await _connection.DisposeAsync();
    }

    private RedisHubBackplane CreateBackplane()
    {
        string suffix = Guid.NewGuid().ToString("N");
        return new RedisHubBackplane(_connection, $"meshworx:test:{suffix}", $"meshworx:test:dir:{suffix}");
    }

    [Fact]
    public async Task PublishAsync_DeliversToAStartedInstance()
    {
        await using RedisHubBackplane backplane = CreateBackplane();

        var receivedTcs = new TaskCompletionSource<BackplaneMessage>();
        await backplane.StartAsync(Guid.NewGuid(), (m, _) => { receivedTcs.TrySetResult(m); return Task.CompletedTask; });

        var message = new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Direct,
            RecipientId = Guid.NewGuid(),
            SenderId = Guid.NewGuid(),
            Body = new byte[] { 42 },
        };

        await backplane.PublishAsync(message);

        BackplaneMessage received = await receivedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal(message.RecipientId, received.RecipientId);
        Assert.Equal(message.SenderId, received.SenderId);
        Assert.Equal(message.Body.ToArray(), received.Body.ToArray());
    }

    [Fact]
    public async Task TwoBackplaneInstances_SameChannel_BothReceivePublishedMessages()
    {
        // Two separate RedisHubBackplane objects sharing the same channel/directory key names, the way
        // two different hub processes actually would — not the same in-process object, unlike
        // InMemoryHubBackplane's own equivalent test.
        string channelSuffix = Guid.NewGuid().ToString("N");
        string channel = $"meshworx:test:{channelSuffix}";
        string directoryKey = $"meshworx:test:dir:{channelSuffix}";

        await using var backplaneA = new RedisHubBackplane(_connection, channel, directoryKey);
        await using var backplaneB = new RedisHubBackplane(_connection, channel, directoryKey);

        var receivedByA = new TaskCompletionSource<BackplaneMessage>();
        var receivedByB = new TaskCompletionSource<BackplaneMessage>();
        await backplaneA.StartAsync(Guid.NewGuid(), (m, _) => { receivedByA.TrySetResult(m); return Task.CompletedTask; });
        await backplaneB.StartAsync(Guid.NewGuid(), (m, _) => { receivedByB.TrySetResult(m); return Task.CompletedTask; });

        await backplaneA.PublishAsync(new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Topic,
            Topic = "orders.created",
            SenderId = Guid.NewGuid(),
            Body = ReadOnlyMemory<byte>.Empty,
        });

        await receivedByA.Task.WaitAsync(WaitTimeout);
        await receivedByB.Task.WaitAsync(WaitTimeout);
    }

    [Fact]
    public async Task StopAsync_StopsReceivingWithoutAffectingOtherInstances()
    {
        await using RedisHubBackplane backplane = CreateBackplane();
        Guid stoppedId = Guid.NewGuid();

        var stoppedReceived = new TaskCompletionSource<BackplaneMessage>();
        var stillReceived = new TaskCompletionSource<BackplaneMessage>();
        await backplane.StartAsync(stoppedId, (m, _) => { stoppedReceived.TrySetResult(m); return Task.CompletedTask; });
        await backplane.StartAsync(Guid.NewGuid(), (m, _) => { stillReceived.TrySetResult(m); return Task.CompletedTask; });

        await backplane.StopAsync(stoppedId);

        await backplane.PublishAsync(new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Direct,
            SenderId = Guid.NewGuid(),
            Body = ReadOnlyMemory<byte>.Empty,
        });

        await stillReceived.Task.WaitAsync(WaitTimeout);
        Assert.False(stoppedReceived.Task.IsCompleted, "A stopped instance still received a published message.");
    }

    [Fact]
    public async Task RegisterClientAsync_ThenTryResolveClientAsync_ReturnsTheId()
    {
        await using RedisHubBackplane backplane = CreateBackplane();
        Guid clientId = Guid.NewGuid();

        await backplane.RegisterClientAsync("Alice", clientId);
        Guid? resolved = await backplane.TryResolveClientAsync("Alice");

        Assert.Equal(clientId, resolved);
    }

    [Fact]
    public async Task UnregisterClientAsync_RemovesTheDirectoryEntry()
    {
        await using RedisHubBackplane backplane = CreateBackplane();
        await backplane.RegisterClientAsync("Alice", Guid.NewGuid());

        await backplane.UnregisterClientAsync("Alice");

        Assert.Null(await backplane.TryResolveClientAsync("Alice"));
    }

    [Fact]
    public async Task TryResolveClientAsync_UnknownName_ReturnsNull()
    {
        await using RedisHubBackplane backplane = CreateBackplane();

        Assert.Null(await backplane.TryResolveClientAsync("Nobody"));
    }

    [Fact]
    public async Task DirectoryEntries_AreVisibleAcrossTwoBackplaneInstances_SameDirectoryKey()
    {
        string directoryKey = $"meshworx:test:dir:{Guid.NewGuid():N}";
        await using var backplaneA = new RedisHubBackplane(_connection, $"meshworx:test:{Guid.NewGuid():N}", directoryKey);
        await using var backplaneB = new RedisHubBackplane(_connection, $"meshworx:test:{Guid.NewGuid():N}", directoryKey);

        Guid clientId = Guid.NewGuid();
        await backplaneA.RegisterClientAsync("Bob", clientId);

        Guid? resolvedFromB = await backplaneB.TryResolveClientAsync("Bob");

        Assert.Equal(clientId, resolvedFromB);
    }
}
