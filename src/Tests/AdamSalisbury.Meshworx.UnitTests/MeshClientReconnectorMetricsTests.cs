using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.InMemory;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

public sealed class MeshClientReconnectorMetricsTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private static MeshClient CreateClient()
    {
        return new MeshClient(new Mock<ILogger<MeshClient>>().Object);
    }

    private static MeshHub CreateHub(ITransportListener listener)
    {
        return new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
    }

    /// <summary>
    /// When the reconnector re-establishes a dropped connection, its reconnects counter is incremented —
    /// but not by the initial connection StartAsync makes, which is not a reconnect.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task Reconnects_AfterConnectionLost_IncrementsReconnectsCounter()
    {
        var firstListener = new InMemoryTransportListener();
        var firstHub = CreateHub(firstListener);
        await firstHub.StartAsync();

        // The factory targets whichever listener the single-element holder points at.
        var listenerHolder = new[] { firstListener };

        await using var client = CreateClient();
        await using var reconnector = new MeshClientReconnector(
            client,
            "Alice",
            _ => Task.FromResult<ITransport>(Volatile.Read(ref listenerHolder[0]).Connect()),
            retryDelay: TimeSpan.FromMilliseconds(50),
            connectTimeout: TimeSpan.FromSeconds(2));

        using var capture = new MetricsCapture<long>(
            reconnector.GetMeterForTesting(), "meshworx.client.reconnects");

        var reconnectedTcs = new TaskCompletionSource();
        reconnector.Reconnected += (_, _) => reconnectedTcs.TrySetResult();

        await reconnector.StartAsync();
        Assert.True(client.IsConnected);

        // The initial connection StartAsync just made is not itself a reconnect.
        Assert.Empty(capture.Values);

        // Stand up a replacement hub and point the factory at it before dropping the first connection.
        var secondListener = new InMemoryTransportListener();
        await using var secondHub = CreateHub(secondListener);
        await secondHub.StartAsync();
        Volatile.Write(ref listenerHolder[0], secondListener);

        // Dropping the first hub disconnects the client, triggering reconnection to the second hub.
        await firstHub.StopAsync();
        await firstHub.DisposeAsync();

        await reconnectedTcs.Task.WaitAsync(WaitTimeout);

        Assert.Equal([1L], capture.Values);

        await secondHub.StopAsync();
    }
}
