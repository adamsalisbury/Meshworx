using System.Text.Json;

namespace AdamSalisbury.Meshworx.Serialization;

/// <summary>
/// An <see cref="IMessageSerializer"/> backed by <see cref="System.Text.Json"/>.
/// </summary>
/// <remarks>
/// The out-of-the-box codec: JSON is in the framework, needs no extra package, and is readable on the
/// wire while debugging. Swap it for a denser one — MessagePack, Protobuf, or anything else — by
/// implementing <see cref="IMessageSerializer"/>; nothing in the core library or the hub changes.
/// <para>
/// Thread-safe. <see cref="JsonSerializerOptions"/> is itself safe for concurrent use once it has been
/// used to (de)serialize once, and this type holds no other state.
/// </para>
/// <para>
/// Serialization is reflection-based unless the supplied <see cref="JsonSerializerOptions"/> carry a
/// source-generated <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>. Supply one via
/// <see cref="JsonSerializerOptions.TypeInfoResolver"/> if the consuming application is trimmed or
/// ahead-of-time compiled.
/// </para>
/// </remarks>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    /// <summary>
    /// A shared instance using the default <see cref="JsonSerializerOptions"/>.
    /// </summary>
    /// <remarks>
    /// Safe to use from anywhere, and the sensible default for an application that has no reason to
    /// customise its JSON. Construct your own instance to supply options.
    /// </remarks>
    public static JsonMessageSerializer Default { get; } = new();

    private readonly JsonSerializerOptions? _options;

    /// <summary>
    /// Initialises a new instance of <see cref="JsonMessageSerializer"/>.
    /// </summary>
    /// <param name="options">
    /// The options to serialize with, or <see langword="null"/> to use <see cref="System.Text.Json"/>'s
    /// own defaults.
    /// </param>
    public JsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = options;
    }

    /// <inheritdoc/>
    public string ContentType => "application/json";

    /// <inheritdoc/>
    public ReadOnlyMemory<byte> Serialize<TValue>(TValue value)
    {
        return JsonSerializer.SerializeToUtf8Bytes(value, _options);
    }

    /// <inheritdoc/>
    /// <exception cref="JsonException">
    /// The body is not valid JSON, or does not describe a <typeparamref name="TValue"/>.
    /// </exception>
    public TValue? Deserialize<TValue>(ReadOnlySpan<byte> data)
    {
        return JsonSerializer.Deserialize<TValue>(data, _options);
    }
}
