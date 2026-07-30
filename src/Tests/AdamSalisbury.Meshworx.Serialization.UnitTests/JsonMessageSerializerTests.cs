using System.Text;
using System.Text.Json;

namespace AdamSalisbury.Meshworx.Serialization.UnitTests;

public class JsonMessageSerializerTests
{
    /// <summary>
    /// The acceptance criterion for the codec layer: an ordinary POCO survives a round trip through the
    /// JSON codec unchanged.
    /// </summary>
    [Fact]
    public void SerializeThenDeserialize_Poco_RoundTripsUnchanged()
    {
        var serializer = new JsonMessageSerializer();
        var original = new Order(42, "Widget", 19.99m, [1, 2, 3]);

        ReadOnlyMemory<byte> body = serializer.Serialize(original);
        Order? returned = serializer.Deserialize<Order>(body.Span);

        Assert.Equal(original, returned);
    }

    [Fact]
    public void ContentType_IsApplicationJson()
    {
        Assert.Equal("application/json", new JsonMessageSerializer().ContentType);
    }

    [Fact]
    public void Default_IsShared()
    {
        Assert.Same(JsonMessageSerializer.Default, JsonMessageSerializer.Default);
    }

    /// <summary>
    /// Supplied options are honoured rather than quietly ignored — the difference is observable in the
    /// bytes produced, which is what a consumer swapping in a naming policy is relying on.
    /// </summary>
    [Fact]
    public void Serialize_WithSuppliedOptions_UsesThem()
    {
        var camelCase = new JsonMessageSerializer(
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        ReadOnlyMemory<byte> body = camelCase.Serialize(new Order(1, "A", 1m, []));

        Assert.Contains("\"productName\"", Encoding.UTF8.GetString(body.Span), StringComparison.Ordinal);
    }

    /// <summary>
    /// A malformed body throws rather than returning a partially populated value, so a caller that has
    /// not opted into the Try shape finds out rather than acting on nonsense.
    /// </summary>
    [Fact]
    public void Deserialize_MalformedBody_Throws()
    {
        var serializer = new JsonMessageSerializer();
        byte[] notJson = Encoding.UTF8.GetBytes("{ this is not json");

        Assert.Throws<JsonException>(() => serializer.Deserialize<Order>(notJson));
    }

    /// <summary>
    /// An explicit JSON null deserializes to null rather than throwing — the one case where a null
    /// return is a successful decode rather than a failure.
    /// </summary>
    [Fact]
    public void Deserialize_ExplicitNull_ReturnsNull()
    {
        var serializer = new JsonMessageSerializer();

        Assert.Null(serializer.Deserialize<Order>(Encoding.UTF8.GetBytes("null")));
    }

    /// <summary>
    /// An interface-declared value serializes against its runtime type, so members not on the interface
    /// still make it onto the wire (issue #111) — the static <c>IAnimal</c> contract alone would have
    /// produced only <c>{"Name":"Rex"}</c>, silently dropping <c>Bones</c>.
    /// </summary>
    [Fact]
    public void Serialize_InterfaceDeclaredValue_CarriesTheRuntimeTypesMembers()
    {
        var serializer = new JsonMessageSerializer();
        IAnimal value = new Dog("Rex", 3);

        ReadOnlyMemory<byte> body = serializer.Serialize(value);

        Dog? returned = serializer.Deserialize<Dog>(body.Span);
        Assert.Equal(value, returned);
    }

    /// <summary>
    /// An abstract-declared value serializes against its runtime type for the same reason — the static
    /// <c>Shape</c> contract has no members of its own at all, so it would otherwise produce <c>{}</c>.
    /// </summary>
    [Fact]
    public void Serialize_AbstractDeclaredValue_CarriesTheRuntimeTypesMembers()
    {
        var serializer = new JsonMessageSerializer();
        Shape value = new Circle(2.5);

        ReadOnlyMemory<byte> body = serializer.Serialize(value);

        Circle? returned = serializer.Deserialize<Circle>(body.Span);
        Assert.Equal(value, returned);
    }

    /// <summary>
    /// An <c>object</c>-declared value already resolved correctly before this fix — <see cref="System.Text.Json"/>
    /// falls back to the runtime type for <c>object</c> itself — and must keep doing so; the fix targets
    /// interface and abstract types specifically, not every declared type narrower than the instance.
    /// </summary>
    [Fact]
    public void Serialize_ObjectDeclaredValue_StillCarriesTheRuntimeTypesMembers()
    {
        var serializer = new JsonMessageSerializer();
        object value = new Dog("Rex", 3);

        ReadOnlyMemory<byte> body = serializer.Serialize(value);

        Dog? returned = serializer.Deserialize<Dog>(body.Span);
        Assert.Equal(new Dog("Rex", 3), returned);
    }

    /// <summary>
    /// A <see langword="null"/> interface- or abstract-declared value still serializes to the JSON null
    /// literal rather than throwing on the <c>GetType()</c> call this fix introduces.
    /// </summary>
    [Fact]
    public void Serialize_NullInterfaceDeclaredValue_WritesJsonNull()
    {
        var serializer = new JsonMessageSerializer();
        IAnimal? value = null;

        ReadOnlyMemory<byte> body = serializer.Serialize(value);

        Assert.Equal("null", Encoding.UTF8.GetString(body.Span));
    }

    /// <summary>
    /// Deserializing into an interface type throws — reconstructing a concrete instance from bytes alone
    /// needs a type discriminator this codec does not provide — and, per the widened documented contract
    /// (issue #111), that throw is <see cref="NotSupportedException"/> rather than the previously
    /// documented <see cref="JsonException"/>.
    /// </summary>
    [Fact]
    public void Deserialize_InterfaceDeclaredType_ThrowsNotSupportedException()
    {
        var serializer = new JsonMessageSerializer();
        byte[] body = serializer.Serialize<IAnimal>(new Dog("Rex", 3)).ToArray();

        Assert.Throws<NotSupportedException>(() => serializer.Deserialize<IAnimal>(body));
    }

    private interface IAnimal
    {
        string Name { get; }
    }

    private sealed record Dog(string Name, int Bones) : IAnimal;

    private abstract record Shape;

    private sealed record Circle(double Radius) : Shape;

    private sealed record Order(int Id, string ProductName, decimal Total, int[] LineIds)
    {
        public bool Equals(Order? other)
        {
            return other is not null
                && Id == other.Id
                && string.Equals(ProductName, other.ProductName, StringComparison.Ordinal)
                && Total == other.Total
                && LineIds.AsSpan().SequenceEqual(other.LineIds);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Id, ProductName, Total, LineIds.Length);
        }
    }
}
