using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Covers <see cref="MeshClient.UpdateAttributesAsync"/> and <see cref="MeshClient.FindClientsAsync"/>'s
/// client-side bound checks and protocol version gate.
/// </summary>
public sealed class ClientAttributesClientTests
{
    [Fact]
    public async Task UpdateAttributesAsync_TooManyAttributes_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var attributes = new Dictionary<string, string>();
        for (int i = 0; i < 33; i++)
        {
            attributes[$"key{i}"] = "value";
        }

        await Assert.ThrowsAsync<ArgumentException>(() => fixture.Client.UpdateAttributesAsync(attributes));
    }

    [Fact]
    public async Task UpdateAttributesAsync_KeyTooLong_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var attributes = new Dictionary<string, string> { [new string('k', 129)] = "value" };

        await Assert.ThrowsAsync<ArgumentException>(() => fixture.Client.UpdateAttributesAsync(attributes));
    }

    [Fact]
    public async Task UpdateAttributesAsync_ValueTooLong_ThrowsArgumentException()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var attributes = new Dictionary<string, string> { ["role"] = new string('v', 513) };

        await Assert.ThrowsAsync<ArgumentException>(() => fixture.Client.UpdateAttributesAsync(attributes));
    }

    [Fact]
    public async Task UpdateAttributesAsync_AtTheBounds_Succeeds()
    {
        var fixture = new MeshClientFixture();
        await fixture.ConnectAsync();

        var attributes = new Dictionary<string, string>();
        for (int i = 0; i < 32; i++)
        {
            attributes[$"key{i}"] = "value";
        }

        await fixture.Client.UpdateAttributesAsync(attributes);
    }

    [Fact]
    public async Task UpdateAttributesAsync_OnPreClientAttributesPeer_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion((byte)(Protocol.ClientAttributesMinVersion - 1));
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<NotSupportedException>(
            () => fixture.Client.UpdateAttributesAsync(new Dictionary<string, string> { ["role"] = "worker" }));
    }

    [Fact]
    public async Task FindClientsAsync_OnPreClientAttributesPeer_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion((byte)(Protocol.ClientAttributesMinVersion - 1));
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<NotSupportedException>(
            () => fixture.Client.FindClientsAsync(new AttributeQuery([new("role", "worker")])));
    }
}
