using AdamSalisbury.Meshworx.Backplane;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Unit-level coverage of <see cref="InMemoryHubBackplane"/> in isolation, independent of
/// <see cref="MeshHub"/> — the pub/sub fan-out, the directory, and the multi-instance semantics
/// (starting twice under the same id, one subscriber's failure not affecting another's delivery).
/// </summary>
public sealed class InMemoryHubBackplaneTests
{
    [Fact]
    public async Task PublishAsync_DeliversToEveryStartedInstance()
    {
        await using var backplane = new InMemoryHubBackplane();

        var receivedByA = new List<BackplaneMessage>();
        var receivedByB = new List<BackplaneMessage>();
        await backplane.StartAsync(Guid.NewGuid(), (m, _) => { receivedByA.Add(m); return Task.CompletedTask; });
        await backplane.StartAsync(Guid.NewGuid(), (m, _) => { receivedByB.Add(m); return Task.CompletedTask; });

        var message = new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Direct,
            RecipientId = Guid.NewGuid(),
            SenderId = Guid.NewGuid(),
            Body = new byte[] { 1, 2, 3 },
        };

        await backplane.PublishAsync(message);

        Assert.Single(receivedByA);
        Assert.Single(receivedByB);
        Assert.Equal(message.RecipientId, receivedByA[0].RecipientId);
    }

    [Fact]
    public async Task StopAsync_StopsOnlyThatInstance()
    {
        await using var backplane = new InMemoryHubBackplane();

        var stillReceiving = new List<BackplaneMessage>();
        var stopped = new List<BackplaneMessage>();
        Guid stoppedId = Guid.NewGuid();

        await backplane.StartAsync(Guid.NewGuid(), (m, _) => { stillReceiving.Add(m); return Task.CompletedTask; });
        await backplane.StartAsync(stoppedId, (m, _) => { stopped.Add(m); return Task.CompletedTask; });

        await backplane.StopAsync(stoppedId);

        await backplane.PublishAsync(new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Direct,
            SenderId = Guid.NewGuid(),
            Body = ReadOnlyMemory<byte>.Empty,
        });

        Assert.Single(stillReceiving);
        Assert.Empty(stopped);
    }

    [Fact]
    public async Task PublishAsync_OneSubscriberThrowing_StillReachesTheOthers()
    {
        await using var backplane = new InMemoryHubBackplane();

        var received = new List<BackplaneMessage>();
        await backplane.StartAsync(Guid.NewGuid(), (_, _) => throw new InvalidOperationException("boom"));
        await backplane.StartAsync(Guid.NewGuid(), (m, _) => { received.Add(m); return Task.CompletedTask; });

        await backplane.PublishAsync(new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Topic,
            Topic = "orders.created",
            SenderId = Guid.NewGuid(),
            Body = ReadOnlyMemory<byte>.Empty,
        });

        Assert.Single(received);
    }

    [Fact]
    public async Task StartAsync_SameInstanceIdTwice_ThrowsInvalidOperationException()
    {
        await using var backplane = new InMemoryHubBackplane();
        Guid instanceId = Guid.NewGuid();

        await backplane.StartAsync(instanceId, (_, _) => Task.CompletedTask);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => backplane.StartAsync(instanceId, (_, _) => Task.CompletedTask));
    }

    [Fact]
    public async Task RegisterClientAsync_ThenTryResolveClientAsync_ReturnsTheId()
    {
        await using var backplane = new InMemoryHubBackplane();
        Guid clientId = Guid.NewGuid();

        await backplane.RegisterClientAsync("Alice", clientId);

        Guid? resolved = await backplane.TryResolveClientAsync("Alice");

        Assert.Equal(clientId, resolved);
    }

    [Fact]
    public async Task TryResolveClientAsync_UnknownName_ReturnsNull()
    {
        await using var backplane = new InMemoryHubBackplane();

        Guid? resolved = await backplane.TryResolveClientAsync("Nobody");

        Assert.Null(resolved);
    }

    [Fact]
    public async Task UnregisterClientAsync_RemovesTheDirectoryEntry()
    {
        await using var backplane = new InMemoryHubBackplane();
        await backplane.RegisterClientAsync("Alice", Guid.NewGuid());

        await backplane.UnregisterClientAsync("Alice");

        Assert.Null(await backplane.TryResolveClientAsync("Alice"));
    }

    [Fact]
    public async Task RegisterClientAsync_SameNameTwice_ReplacesTheEntry()
    {
        await using var backplane = new InMemoryHubBackplane();
        Guid firstId = Guid.NewGuid();
        Guid secondId = Guid.NewGuid();

        await backplane.RegisterClientAsync("Alice", firstId);
        await backplane.RegisterClientAsync("Alice", secondId);

        Assert.Equal(secondId, await backplane.TryResolveClientAsync("Alice"));
    }

    [Fact]
    public async Task DisposeAsync_ThenStartAsync_ThrowsObjectDisposedException()
    {
        var backplane = new InMemoryHubBackplane();
        await backplane.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => backplane.StartAsync(Guid.NewGuid(), (_, _) => Task.CompletedTask));
    }
}
