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
