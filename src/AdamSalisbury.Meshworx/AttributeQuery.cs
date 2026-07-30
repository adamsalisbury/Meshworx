using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace AdamSalisbury.Meshworx;

/// <summary>
/// A directory query that matches every currently-registered client whose attribute bag holds an exact
/// key/value pair for each criterion in this query.
/// </summary>
/// <remarks>
/// Every criterion must match — there is no "or" — mirroring the issue's own worked example, "all
/// clients with <c>role=worker</c> and <c>region=eu</c>". A client whose attribute bag has additional
/// keys the query does not mention still matches, as long as every criterion the query does specify is
/// present with an equal value. An empty query matches every connected client.
/// </remarks>
[SuppressMessage(
    "Naming",
    "CA1710:Identifiers should have correct suffix",
    Justification = "AttributeQuery names what this type is for — a query — not what it happens to be "
        + "implemented with; renaming it to end in Dictionary or Collection would obscure that for no "
        + "benefit, exactly the trade-off this library's own MessageHeaders already accepts.")]
public sealed class AttributeQuery : IReadOnlyDictionary<string, string>
{
    private readonly IReadOnlyDictionary<string, string> _criteria;

    /// <summary>
    /// Initialises a new instance of <see cref="AttributeQuery"/>, copying the supplied criteria into it.
    /// </summary>
    /// <param name="criteria">The key/value pairs a matching client's attributes must all contain.</param>
    public AttributeQuery(IEnumerable<KeyValuePair<string, string>> criteria)
    {
        ArgumentNullException.ThrowIfNull(criteria);
        _criteria = new Dictionary<string, string>(criteria, StringComparer.Ordinal);
    }

    /// <inheritdoc/>
    public int Count => _criteria.Count;

    /// <inheritdoc/>
    public IEnumerable<string> Keys => _criteria.Keys;

    /// <inheritdoc/>
    public IEnumerable<string> Values => _criteria.Values;

    /// <inheritdoc/>
    public string this[string key] => _criteria[key];

    /// <inheritdoc/>
    public bool ContainsKey(string key)
    {
        return _criteria.ContainsKey(key);
    }

    /// <inheritdoc/>
    public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value)
    {
        return _criteria.TryGetValue(key, out value);
    }

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
    {
        return _criteria.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
