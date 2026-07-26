using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// A small, string-keyed set of metadata values that travels alongside a message's body without the
/// hub ever interpreting the body itself.
/// </summary>
/// <remarks>
/// Headers exist for cross-cutting concerns — a correlation identifier, a content-type hint, trace
/// context, and the like — that routing or observability code may want to read without decoding an
/// application's opaque payload. The hub passes header content it cannot act on straight through
/// unchanged and never deserialises or otherwise interprets a header value.
/// <para>
/// A message sent with no headers, or with an empty <see cref="MessageHeaders"/>, costs nothing extra
/// on the wire beyond today's frames: the header block is only emitted when it actually carries at
/// least one entry.
/// </para>
/// </remarks>
[SuppressMessage(
    "Naming",
    "CA1710:Identifiers should have correct suffix",
    Justification = "MessageHeaders is the domain vocabulary this library and its documentation use "
        + "throughout; renaming it to end in Dictionary or Collection would obscure its purpose for no "
        + "benefit. .NET itself accepts the same trade-off for System.Net.Http.Headers.HttpHeaders.")]
public sealed class MessageHeaders : IReadOnlyDictionary<string, string>
{
    /// <summary>
    /// A shared, empty instance of <see cref="MessageHeaders"/>, used whenever no headers are set.
    /// </summary>
    public static readonly MessageHeaders Empty = new(new Dictionary<string, string>(0, StringComparer.Ordinal));

    private readonly IReadOnlyDictionary<string, string> _values;

    /// <summary>
    /// Initialises a new instance of <see cref="MessageHeaders"/>, copying the supplied key/value
    /// pairs into it.
    /// </summary>
    /// <param name="values">The header values to copy into the new instance.</param>
    public MessageHeaders(IEnumerable<KeyValuePair<string, string>> values)
    {
        ArgumentNullException.ThrowIfNull(values);
        _values = new Dictionary<string, string>(values, StringComparer.Ordinal);
    }

    private MessageHeaders(Dictionary<string, string> values)
    {
        _values = values;
    }

    /// <summary>
    /// Wraps a dictionary the caller guarantees is freshly built and will not be mutated or shared
    /// afterwards, without copying it.
    /// </summary>
    /// <remarks>
    /// A named factory rather than a second constructor overload deliberately: a
    /// <see cref="Dictionary{TKey, TValue}"/> also satisfies the public
    /// <see cref="MessageHeaders(IEnumerable{KeyValuePair{string, string}})"/> constructor's parameter
    /// type, so an overload taking one directly would be silently preferred by overload resolution for
    /// <i>any</i> same-assembly caller passing a dictionary — including one that still holds a
    /// reference to it and expects the usual copying behaviour. Used only by the wire-format decoder,
    /// which builds a dictionary for this purpose alone and never touches it again.
    /// </remarks>
    internal static MessageHeaders FromOwnedDictionary(Dictionary<string, string> values)
    {
        return new MessageHeaders(values);
    }

    /// <inheritdoc/>
    public int Count => _values.Count;

    /// <inheritdoc/>
    public IEnumerable<string> Keys => _values.Keys;

    /// <inheritdoc/>
    public IEnumerable<string> Values => _values.Values;

    /// <inheritdoc/>
    public string this[string key] => _values[key];

    /// <inheritdoc/>
    public bool ContainsKey(string key)
    {
        return _values.ContainsKey(key);
    }

    /// <inheritdoc/>
    public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value)
    {
        return _values.TryGetValue(key, out value);
    }

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
    {
        return _values.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
