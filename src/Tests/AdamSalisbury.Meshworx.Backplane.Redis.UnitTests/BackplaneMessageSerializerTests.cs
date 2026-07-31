namespace AdamSalisbury.Meshworx.Backplane.Redis.UnitTests;

public sealed class BackplaneMessageSerializerTests
{
    [Fact]
    public void RoundTrips_DirectMessage()
    {
        var message = new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Direct,
            RecipientId = Guid.NewGuid(),
            SenderId = Guid.NewGuid(),
            Body = new byte[] { 1, 2, 3, 4, 5 },
        };

        byte[] serialized = BackplaneMessageSerializer.Serialize(message);
        BackplaneMessage roundTripped = BackplaneMessageSerializer.Deserialize(serialized);

        Assert.Equal(message.OriginInstanceId, roundTripped.OriginInstanceId);
        Assert.Equal(message.Kind, roundTripped.Kind);
        Assert.Equal(message.RecipientId, roundTripped.RecipientId);
        Assert.Equal(message.SenderId, roundTripped.SenderId);
        Assert.Null(roundTripped.GroupName);
        Assert.Null(roundTripped.Topic);
        Assert.Equal(message.Body.ToArray(), roundTripped.Body.ToArray());
    }

    [Fact]
    public void RoundTrips_GroupMessage()
    {
        var message = new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Group,
            GroupName = "team-eu",
            SenderId = Guid.NewGuid(),
            Body = new byte[] { 9, 8, 7 },
        };

        BackplaneMessage roundTripped = BackplaneMessageSerializer.Deserialize(
            BackplaneMessageSerializer.Serialize(message));

        Assert.Equal("team-eu", roundTripped.GroupName);
        Assert.Null(roundTripped.Topic);
        Assert.Equal(message.Body.ToArray(), roundTripped.Body.ToArray());
    }

    [Fact]
    public void RoundTrips_TopicMessage()
    {
        var message = new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Topic,
            Topic = "orders.eu.created",
            SenderId = Guid.NewGuid(),
            Body = ReadOnlyMemory<byte>.Empty,
        };

        BackplaneMessage roundTripped = BackplaneMessageSerializer.Deserialize(
            BackplaneMessageSerializer.Serialize(message));

        Assert.Equal("orders.eu.created", roundTripped.Topic);
        Assert.Null(roundTripped.GroupName);
        Assert.Empty(roundTripped.Body.ToArray());
    }

    [Fact]
    public void Deserialize_TooShortForFixedHeader_ThrowsFormatException()
    {
        Assert.Throws<FormatException>(() => BackplaneMessageSerializer.Deserialize(new byte[10]));
    }

    [Fact]
    public void Deserialize_GroupNameLengthRunsPastPayload_ThrowsFormatException()
    {
        var message = new BackplaneMessage
        {
            OriginInstanceId = Guid.NewGuid(),
            Kind = BackplaneMessageKind.Group,
            GroupName = "team",
            SenderId = Guid.NewGuid(),
            Body = ReadOnlyMemory<byte>.Empty,
        };

        byte[] serialized = BackplaneMessageSerializer.Serialize(message);
        byte[] truncated = serialized[..(serialized.Length - message.GroupName.Length - 4)];

        Assert.Throws<FormatException>(() => BackplaneMessageSerializer.Deserialize(truncated));
    }
}
