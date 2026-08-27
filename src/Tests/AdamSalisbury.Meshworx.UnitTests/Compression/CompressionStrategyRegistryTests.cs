using System.Text;
using AdamSalisbury.Meshworx.Compression;

namespace AdamSalisbury.Meshworx.UnitTests.Compression;

public sealed class CompressionStrategyRegistryTests
{
    [Fact]
    public void Constructor_CreatesAnEmptyRegistry()
    {
        var registry = new CompressionStrategyRegistry();

        Assert.Empty(registry.AlgorithmIds);
        Assert.False(registry.Contains(CompressionAlgorithms.Brotli));
    }

    [Fact]
    public void CreateDefault_RegistersBothBuiltInsBrotliFirst()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        Assert.Equal([CompressionAlgorithms.Brotli, CompressionAlgorithms.Deflate], registry.AlgorithmIds);
        Assert.Same(BrotliCompressionStrategy.Default, registry.Resolve(CompressionAlgorithms.Brotli));
        Assert.Same(DeflateCompressionStrategy.Default, registry.Resolve(CompressionAlgorithms.Deflate));
    }

    [Fact]
    public void CreateDefault_ReturnsADistinctRegistryEachTime()
    {
        // Two endpoints in one process must be able to configure compression independently.
        CompressionStrategyRegistry first = CompressionStrategyRegistry.CreateDefault();
        CompressionStrategyRegistry second = CompressionStrategyRegistry.CreateDefault();

        first.Remove(CompressionAlgorithms.Brotli);

        Assert.True(second.Contains(CompressionAlgorithms.Brotli));
    }

    [Theory]
    [InlineData(CompressionAlgorithms.Brotli)]
    [InlineData(CompressionAlgorithms.Deflate)]
    public void Resolve_BuiltIn_RoundTripsAPayload(string algorithmId)
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();
        byte[] payload = Encoding.UTF8.GetBytes(new string('a', 4096));

        ICompressionStrategy strategy = registry.Resolve(algorithmId);
        ReadOnlyMemory<byte> restored = strategy.Decompress(strategy.Compress(payload), payload.Length);

        Assert.Equal(payload, restored.ToArray());
    }

    [Fact]
    public void Register_CustomStrategy_IsResolvedAndUsed()
    {
        // The point of the whole abstraction: an algorithm the library has never heard of, selected and
        // used with no library change.
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();
        var custom = new ReversingCompressionStrategy("x-reverse");

        registry.Register(custom);

        byte[] payload = [1, 2, 3, 4, 5];
        ICompressionStrategy resolved = registry.Resolve("x-reverse");

        Assert.Same(custom, resolved);
        Assert.Equal([5, 4, 3, 2, 1], resolved.Compress(payload).ToArray());
        Assert.Equal(payload, resolved.Decompress(resolved.Compress(payload), payload.Length).ToArray());
    }

    [Fact]
    public void Register_NewStrategies_AppendsThemInRegistrationOrder()
    {
        var registry = new CompressionStrategyRegistry();

        registry.Register(new ReversingCompressionStrategy("second"));
        registry.Register(new ReversingCompressionStrategy("first"));

        Assert.Equal(["second", "first"], registry.AlgorithmIds);
    }

    [Fact]
    public void Register_ExistingId_ReplacesInPlaceWithoutReordering()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();
        var replacement = new ReversingCompressionStrategy(CompressionAlgorithms.Brotli);

        registry.Register(replacement);

        Assert.Equal([CompressionAlgorithms.Brotli, CompressionAlgorithms.Deflate], registry.AlgorithmIds);
        Assert.Same(replacement, registry.Resolve(CompressionAlgorithms.Brotli));
    }

    [Fact]
    public void Register_ExistingIdInDifferentCase_KeepsTheOriginalCasing()
    {
        // The id peers have already been told about must not be restyled underneath them.
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        registry.Register(new ReversingCompressionStrategy("BR"));

        Assert.Equal([CompressionAlgorithms.Brotli, CompressionAlgorithms.Deflate], registry.AlgorithmIds);
    }

    [Fact]
    public void Register_ReturnsTheRegistryForChaining()
    {
        var registry = new CompressionStrategyRegistry();

        CompressionStrategyRegistry chained = registry
            .Register(new ReversingCompressionStrategy("one"))
            .Register(new ReversingCompressionStrategy("two"));

        Assert.Same(registry, chained);
        Assert.Equal(["one", "two"], registry.AlgorithmIds);
    }

    [Fact]
    public void Register_NullStrategy_ThrowsArgumentNullException()
    {
        var registry = new CompressionStrategyRegistry();

        Assert.Throws<ArgumentNullException>(() => registry.Register(null!));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("has space")]
    [InlineData("semi;colon")]
    [InlineData("comma,separated")]
    [InlineData("new\nline")]
    [InlineData("non-ascii-é")]
    [InlineData("thirty-three-characters-long-abcd")]
    public void Register_UnusableAlgorithmId_ThrowsArgumentExceptionAtRegistrationTime(string algorithmId)
    {
        // Rejected while the endpoint is being configured, not when a send has already been handed a
        // payload it can no longer do anything useful with.
        var registry = new CompressionStrategyRegistry();

        Assert.Throws<ArgumentException>(() => registry.Register(new ReversingCompressionStrategy(algorithmId)));
        Assert.Empty(registry.AlgorithmIds);
    }

    [Theory]
    [InlineData("zstd")]
    [InlineData("x-custom.v2")]
    [InlineData("lz4+block")]
    [InlineData("my_codec")]
    [InlineData("thirty-two-characters-long-abcd1")]
    public void Register_UsableAlgorithmId_IsAccepted(string algorithmId)
    {
        var registry = new CompressionStrategyRegistry();

        registry.Register(new ReversingCompressionStrategy(algorithmId));

        Assert.Equal([algorithmId], registry.AlgorithmIds);
    }

    [Fact]
    public void Resolve_UnknownId_ThrowsUnknownCompressionAlgorithmException()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        var exception = Assert.Throws<UnknownCompressionAlgorithmException>(() => registry.Resolve("zstd"));

        Assert.Equal("zstd", exception.AlgorithmId);
        Assert.Contains("zstd", exception.Message, StringComparison.Ordinal);
        Assert.Contains("br, deflate", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Resolve_UnknownIdOnAnEmptyRegistry_SaysNothingIsRegistered()
    {
        var registry = new CompressionStrategyRegistry();

        var exception = Assert.Throws<UnknownCompressionAlgorithmException>(() => registry.Resolve("br"));

        Assert.Contains("none", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Resolve_AndContains_MatchTheIdCaseInsensitively()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        Assert.True(registry.Contains("BR"));
        Assert.Same(BrotliCompressionStrategy.Default, registry.Resolve("Br"));
    }

    [Fact]
    public void TryResolve_KnownId_ReturnsTheStrategy()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        Assert.True(registry.TryResolve(CompressionAlgorithms.Deflate, out ICompressionStrategy? strategy));
        Assert.Same(DeflateCompressionStrategy.Default, strategy);
    }

    [Fact]
    public void TryResolve_UnknownId_ReturnsFalseWithoutThrowing()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        Assert.False(registry.TryResolve("zstd", out ICompressionStrategy? strategy));
        Assert.Null(strategy);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void Lookup_EmptyId_ThrowsArgumentException(string? algorithmId)
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        // ThrowsAny because null is an ArgumentNullException, which is the right answer for null and a
        // derived type rather than an exact match.
        Assert.ThrowsAny<ArgumentException>(() => registry.Contains(algorithmId!));
        Assert.ThrowsAny<ArgumentException>(() => registry.Resolve(algorithmId!));
        Assert.ThrowsAny<ArgumentException>(() => registry.TryResolve(algorithmId!, out _));
        Assert.ThrowsAny<ArgumentException>(() => registry.Remove(algorithmId!));
    }

    [Fact]
    public void Remove_RegisteredId_DropsItFromLookupAndOrder()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        Assert.True(registry.Remove("BR"));
        Assert.Equal([CompressionAlgorithms.Deflate], registry.AlgorithmIds);
        Assert.False(registry.Contains(CompressionAlgorithms.Brotli));
    }

    [Fact]
    public void Remove_UnregisteredId_ReturnsFalseAndChangesNothing()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        Assert.False(registry.Remove("zstd"));
        Assert.Equal([CompressionAlgorithms.Brotli, CompressionAlgorithms.Deflate], registry.AlgorithmIds);
    }

    [Fact]
    public void Clear_RemovesEverythingAndAllowsARebuild()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        registry.Clear().Register(new ReversingCompressionStrategy("x-only-mine"));

        Assert.Equal(["x-only-mine"], registry.AlgorithmIds);
    }

    [Fact]
    public void AlgorithmIds_IsASnapshotUnaffectedByLaterRegistration()
    {
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        IReadOnlyList<string> snapshot = registry.AlgorithmIds;
        registry.Register(new ReversingCompressionStrategy("zstd"));

        Assert.Equal(2, snapshot.Count);
        Assert.Equal(3, registry.AlgorithmIds.Count);
    }

    [Fact]
    public async Task Registry_RegisteredAndResolvedConcurrently_StaysConsistent()
    {
        // Registration takes a lock and rebuilds; resolution takes none and reads a published snapshot.
        // Interleaving the two must never surface a half-built map.
        CompressionStrategyRegistry registry = CompressionStrategyRegistry.CreateDefault();

        Task[] registrars = [.. Enumerable.Range(0, 16).Select(i =>
            Task.Run(() => registry.Register(new ReversingCompressionStrategy($"x-{i:D2}"))))];

        Task[] resolvers = [.. Enumerable.Range(0, 16).Select(_ => Task.Run(() =>
        {
            for (int i = 0; i < 200; i++)
            {
                Assert.Same(BrotliCompressionStrategy.Default, registry.Resolve(CompressionAlgorithms.Brotli));
                Assert.NotEmpty(registry.AlgorithmIds);
            }
        }))];

        await Task.WhenAll([.. registrars, .. resolvers]);

        Assert.Equal(18, registry.AlgorithmIds.Count);
        Assert.Equal(registry.AlgorithmIds.Count, registry.AlgorithmIds.Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    /// <summary>
    /// A stand-in for a consumer's own algorithm: not a real codec, but a genuine
    /// <see cref="ICompressionStrategy"/> the library has never heard of.
    /// </summary>
    private sealed class ReversingCompressionStrategy(string algorithmId) : ICompressionStrategy
    {
        public string AlgorithmId { get; } = algorithmId;

        public ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte> payload)
        {
            byte[] reversed = payload.ToArray();
            Array.Reverse(reversed);

            return reversed;
        }

        public ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> payload, int maxDecompressedBytes)
        {
            if (payload.Length > maxDecompressedBytes)
            {
                throw new InvalidDataException("Too big.");
            }

            return Compress(payload);
        }
    }
}
