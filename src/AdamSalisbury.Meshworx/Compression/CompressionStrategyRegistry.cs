using System.Collections.ObjectModel;
using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx.Compression;

/// <summary>
/// The default <see cref="ICompressionStrategyRegistry"/>: an ordered, case-insensitive map from
/// algorithm id to <see cref="ICompressionStrategy"/> that a consumer populates at configuration time and
/// endpoints resolve against thereafter.
/// </summary>
/// <remarks>
/// <para>
/// Registration is expected to happen once, while the endpoint is being configured; resolution happens on
/// every compressed message. The two are weighted accordingly — a registration takes a lock and rebuilds
/// the whole map, while a resolution takes no lock at all and reads a published snapshot. Both are
/// nonetheless safe to interleave: a strategy registered while another thread is resolving is picked up
/// on the next resolution, and never leaves a caller looking at a half-built map.
/// </para>
/// <para>
/// Registration order is the preference order, and is preserved deliberately rather than incidentally:
/// choosing "the best algorithm both endpoints understand" needs the two sides to be ranking candidates,
/// not enumerating a hash table.
/// </para>
/// </remarks>
public sealed class CompressionStrategyRegistry : ICompressionStrategyRegistry
{
    private readonly Lock _gate = new();

    private volatile Snapshot _snapshot = Snapshot.Empty;

    /// <summary>
    /// Initialises a new, empty instance of the <see cref="CompressionStrategyRegistry"/> class.
    /// </summary>
    /// <remarks>
    /// Empty, not defaulted — use <see cref="CreateDefault"/> for the built-ins. An endpoint that wants
    /// only its own algorithms, and specifically wants a peer never to be offered Brotli or Deflate,
    /// starts here rather than having to remove things it never asked for.
    /// </remarks>
    public CompressionStrategyRegistry()
    {
    }

    /// <summary>
    /// Creates a registry holding the built-in strategies: Brotli first, then Deflate.
    /// </summary>
    /// <returns>A new registry. Each call returns a distinct instance, so configuring one endpoint's registry never affects another's.</returns>
    /// <remarks>
    /// Brotli leads because it compresses this library's typical payloads better, and the order is the
    /// preference order. Either can be replaced with <see cref="Register"/> — registering a strategy
    /// under an existing id substitutes it in place, keeping its position — or dropped with
    /// <see cref="Remove"/>.
    /// </remarks>
    public static CompressionStrategyRegistry CreateDefault()
    {
        var registry = new CompressionStrategyRegistry();
        registry.Register(BrotliCompressionStrategy.Default);
        registry.Register(DeflateCompressionStrategy.Default);

        return registry;
    }

    /// <inheritdoc/>
    public IReadOnlyList<string> AlgorithmIds => _snapshot.Ids;

    /// <summary>
    /// Registers a strategy under its own <see cref="ICompressionStrategy.AlgorithmId"/>.
    /// </summary>
    /// <param name="strategy">The strategy to register.</param>
    /// <returns>This registry, so registrations can be chained.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="strategy"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// The strategy's algorithm id is empty, longer than 32 characters, or contains a character outside
    /// the ASCII letters, digits and <c>-</c> <c>+</c> <c>.</c> <c>_</c>.
    /// </exception>
    /// <remarks>
    /// A strategy registered under an id that is already present replaces the existing one and keeps its
    /// position in the preference order, which is what makes the built-ins substitutable. The id is
    /// validated here, at configuration time, rather than when a message is sent: an id that could not
    /// survive a round-trip through a message header is a startup mistake, and it should fail like one
    /// instead of surfacing mid-flight on a send that has already been handed a payload.
    /// </remarks>
    public CompressionStrategyRegistry Register(ICompressionStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(strategy);

        string algorithmId = strategy.AlgorithmId;
        ValidateAlgorithmId(algorithmId, $"{nameof(strategy)}.{nameof(ICompressionStrategy.AlgorithmId)}");

        lock (_gate)
        {
            Snapshot current = _snapshot;
            var byId = new Dictionary<string, ICompressionStrategy>(current.ById, StringComparer.OrdinalIgnoreCase);
            bool replacing = byId.ContainsKey(algorithmId);

            // Assigning through the indexer keeps the key already in the dictionary, so a replacement
            // registered as "BR" does not silently restyle an id peers have already been told is "br".
            byId[algorithmId] = strategy;

            string[] ids = replacing ? current.Ids.ToArray() : [.. current.Ids, algorithmId];
            _snapshot = new Snapshot(byId, ids);
        }

        return this;
    }

    /// <summary>
    /// Removes the strategy registered under the given algorithm id, if there is one.
    /// </summary>
    /// <param name="algorithmId">The algorithm id to remove. Matched case-insensitively.</param>
    /// <returns><see langword="true"/> if a strategy was removed; otherwise <see langword="false"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="algorithmId"/> is empty or whitespace.</exception>
    public bool Remove(string algorithmId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(algorithmId);

        lock (_gate)
        {
            Snapshot current = _snapshot;

            if (!current.ById.ContainsKey(algorithmId))
            {
                return false;
            }

            var byId = new Dictionary<string, ICompressionStrategy>(current.ById, StringComparer.OrdinalIgnoreCase);
            byId.Remove(algorithmId);

            string[] ids = current.Ids
                .Where(id => !string.Equals(id, algorithmId, StringComparison.OrdinalIgnoreCase))
                .ToArray();

            _snapshot = new Snapshot(byId, ids);
        }

        return true;
    }

    /// <summary>
    /// Removes every registered strategy.
    /// </summary>
    /// <returns>This registry, so it can be chained into a replacement set of registrations.</returns>
    public CompressionStrategyRegistry Clear()
    {
        lock (_gate)
        {
            _snapshot = Snapshot.Empty;
        }

        return this;
    }

    /// <inheritdoc/>
    public bool Contains(string algorithmId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(algorithmId);

        return _snapshot.ById.ContainsKey(algorithmId);
    }

    /// <inheritdoc/>
    public bool TryResolve(string algorithmId, out ICompressionStrategy? strategy)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(algorithmId);

        return _snapshot.ById.TryGetValue(algorithmId, out strategy);
    }

    /// <inheritdoc/>
    public ICompressionStrategy Resolve(string algorithmId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(algorithmId);

        Snapshot snapshot = _snapshot;

        if (!snapshot.ById.TryGetValue(algorithmId, out ICompressionStrategy? strategy))
        {
            throw new UnknownCompressionAlgorithmException(algorithmId, snapshot.Ids);
        }

        return strategy;
    }

    private static void ValidateAlgorithmId(string algorithmId, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(algorithmId))
        {
            throw new ArgumentException("The algorithm id must not be empty.", parameterName);
        }

        if (algorithmId.Length > Protocol.MaxCompressionAlgorithmIdLength)
        {
            throw new ArgumentException(
                $"The algorithm id '{algorithmId}' is longer than the {Protocol.MaxCompressionAlgorithmIdLength}-character limit.",
                parameterName);
        }

        foreach (char character in algorithmId)
        {
            if (!IsAlgorithmIdCharacter(character))
            {
                throw new ArgumentException(
                    $"The algorithm id '{algorithmId}' contains '{character}', which is not an ASCII letter, digit, '-', '+', '.' or '_'.",
                    parameterName);
            }
        }
    }

    private static bool IsAlgorithmIdCharacter(char character)
    {
        return char.IsAsciiLetterOrDigit(character)
            || character is '-' or '+' or '.' or '_';
    }

    private sealed class Snapshot
    {
        internal static readonly Snapshot Empty = new(new Dictionary<string, ICompressionStrategy>(StringComparer.OrdinalIgnoreCase), []);

        internal Snapshot(Dictionary<string, ICompressionStrategy> byId, string[] ids)
        {
            ById = byId;
            Ids = new ReadOnlyCollection<string>(ids);
        }

        internal Dictionary<string, ICompressionStrategy> ById { get; }

        internal ReadOnlyCollection<string> Ids { get; }
    }
}
