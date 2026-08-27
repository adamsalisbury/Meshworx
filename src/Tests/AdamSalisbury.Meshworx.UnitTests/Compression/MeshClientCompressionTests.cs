using AdamSalisbury.Meshworx.Compression;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Compression;

public sealed class MeshClientCompressionTests
{
    [Fact(Timeout = 1000)]
    public async Task CompressionStrategies_NotSupplied_HoldsTheBuiltIns()
    {
        await using var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);

        Assert.Equal(
            [CompressionAlgorithms.Brotli, CompressionAlgorithms.Deflate],
            client.CompressionStrategies.AlgorithmIds);
    }

    [Fact(Timeout = 1000)]
    public async Task CompressionStrategies_Supplied_IsTheRegistryTheClientExposes()
    {
        var registry = new CompressionStrategyRegistry();

        await using var client = new MeshClient(
            new Mock<ILogger<MeshClient>>().Object,
            compressionStrategies: registry);

        Assert.Same(registry, client.CompressionStrategies);
        Assert.Empty(client.CompressionStrategies.AlgorithmIds);
    }

    [Fact(Timeout = 1000)]
    public async Task CompressionStrategies_RegisteredAfterConstruction_IsResolvable()
    {
        // Endpoint state, not connection state — a consumer can add an algorithm without rebuilding the
        // client, and without ever having connected.
        await using var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        var registry = (CompressionStrategyRegistry)client.CompressionStrategies;

        registry.Register(DeflateCompressionStrategy.Default);

        Assert.True(client.CompressionStrategies.Contains(CompressionAlgorithms.Deflate));
    }

    [Fact(Timeout = 1000)]
    public async Task CompressionStrategies_TwoClients_AreIndependent()
    {
        await using var first = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        await using var second = new MeshClient(new Mock<ILogger<MeshClient>>().Object);

        ((CompressionStrategyRegistry)first.CompressionStrategies).Clear();

        Assert.Empty(first.CompressionStrategies.AlgorithmIds);
        Assert.Equal(2, second.CompressionStrategies.AlgorithmIds.Count);
    }
}
