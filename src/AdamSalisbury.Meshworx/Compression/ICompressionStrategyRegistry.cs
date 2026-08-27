namespace AdamSalisbury.Meshworx.Compression;

/// <summary>
/// Resolves an algorithm id, as it travels on the wire, to the <see cref="ICompressionStrategy"/> that
/// implements it.
/// </summary>
/// <remarks>
/// The registry is what makes compression pluggable: a consumer registers a strategy for an algorithm the
/// library has never heard of, both endpoints resolve it by id, and no library code changes. Built-in
/// Brotli and Deflate strategies are present by default (see
/// <see cref="CompressionStrategyRegistry.CreateDefault"/>) and can be replaced or removed like any other
/// entry.
/// </remarks>
public interface ICompressionStrategyRegistry
{
    /// <summary>
    /// Gets the ids of the registered strategies, in the order they were registered.
    /// </summary>
    /// <remarks>
    /// The order is preserved, and is the preference order: the first entry is the one an endpoint would
    /// rather use. It is a snapshot taken when the property is read, so enumerating it is safe while
    /// another thread registers a strategy.
    /// </remarks>
    IReadOnlyList<string> AlgorithmIds { get; }

    /// <summary>
    /// Determines whether a strategy is registered under the given algorithm id.
    /// </summary>
    /// <param name="algorithmId">The algorithm id to look for. Matched case-insensitively.</param>
    /// <returns><see langword="true"/> if a strategy is registered under that id; otherwise <see langword="false"/>.</returns>
    bool Contains(string algorithmId);

    /// <summary>
    /// Attempts to resolve the strategy registered under the given algorithm id.
    /// </summary>
    /// <param name="algorithmId">The algorithm id to resolve. Matched case-insensitively.</param>
    /// <param name="strategy">
    /// When this method returns, the resolved strategy, or <see langword="null"/> if none is registered.
    /// </param>
    /// <returns><see langword="true"/> if a strategy was resolved; otherwise <see langword="false"/>.</returns>
    bool TryResolve(string algorithmId, out ICompressionStrategy? strategy);

    /// <summary>
    /// Resolves the strategy registered under the given algorithm id.
    /// </summary>
    /// <param name="algorithmId">The algorithm id to resolve. Matched case-insensitively.</param>
    /// <returns>The registered strategy.</returns>
    /// <exception cref="ArgumentException"><paramref name="algorithmId"/> is empty or whitespace.</exception>
    /// <exception cref="UnknownCompressionAlgorithmException">No strategy is registered under that id.</exception>
    ICompressionStrategy Resolve(string algorithmId);
}
