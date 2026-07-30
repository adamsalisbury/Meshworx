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
/// ahead-of-time compiled — and, if any value ever passed to <see cref="Serialize{TValue}"/> is declared
/// as an interface or an abstract type, register every concrete type that can actually appear behind it,
/// not just the interface or abstract type itself; see <see cref="Serialize{TValue}"/> for why.
/// </para>
/// <para>
/// Polymorphism is one-directional, and only at the top level. A <em>value</em> passed to
/// <see cref="Serialize{TValue}"/> as an interface- or abstract-declared <c>TValue</c>
/// serializes against its own runtime type, so nothing on that value is lost. This does not reach further
/// down the object graph: a concrete type with an interface- or abstract-typed <em>property</em> still has
/// that property serialized by its declared type, exactly as before — <see cref="System.Text.Json"/> makes
/// that decision per property, not per top-level call. This codec has no way to widen it there without a
/// custom converter, and does not attempt to. It is also not a substitute for an explicit contract type: a
/// caller that types a payload as an interface specifically to keep members off the wire — data
/// minimisation, not merely convenience — must not rely on that interface to do so, since this codec will
/// write the runtime type's full contract regardless. There is no equivalent fallback on the way back in:
/// reconstructing the right concrete type from a body alone needs a type discriminator this codec does not
/// set up, so deserializing into an interface or abstract type throws rather than guessing — see
/// <see cref="Deserialize{TValue}"/>.
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
    /// <remarks>
    /// When <typeparamref name="TValue"/> is an interface or an abstract type, this serializes against
    /// <c>value.GetType()</c> — the value's runtime type — rather than the declared one.
    /// <see cref="System.Text.Json"/> otherwise writes only the contract of the declared type, silently
    /// dropping every member the concrete instance carries beyond it.
    /// </remarks>
    /// <exception cref="NotSupportedException">
    /// The supplied <see cref="JsonSerializerOptions.TypeInfoResolver"/> is a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> that has metadata for the
    /// declared type but not for <c>value.GetType()</c>. A trimmed or ahead-of-time-compiled caller that
    /// passes an interface- or abstract-declared value must register every concrete type that can appear
    /// behind it, not just the declared one — see the class remarks.
    /// </exception>
    public ReadOnlyMemory<byte> Serialize<TValue>(TValue value)
    {
        if (value is not null && (typeof(TValue).IsInterface || typeof(TValue).IsAbstract))
        {
            return JsonSerializer.SerializeToUtf8Bytes(value, value.GetType(), _options);
        }

        return JsonSerializer.SerializeToUtf8Bytes(value, _options);
    }

    /// <inheritdoc/>
    /// <exception cref="JsonException">
    /// The body is not valid JSON, or does not describe a <typeparamref name="TValue"/>.
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// <typeparamref name="TValue"/> is an interface or an abstract type.
    /// <see cref="System.Text.Json"/> cannot construct an instance of one without a configured
    /// polymorphic contract, which this codec does not set up — deserializing to such a type is a
    /// caller error, not a malformed body, so it is not wrapped as <see cref="JsonException"/>.
    /// </exception>
    public TValue? Deserialize<TValue>(ReadOnlySpan<byte> data)
    {
        return JsonSerializer.Deserialize<TValue>(data, _options);
    }
}
