using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;

namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// Covers <see cref="MeshClient.SubscribePresenceAsync"/>/<see cref="MeshClient.UnsubscribePresenceAsync"/>'s
/// protocol version gate.
/// </summary>
public sealed class PresenceClientTests
{
    [Fact]
    public async Task SubscribePresenceAsync_OnPrePresencePeer_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion((byte)(Protocol.PresenceMinVersion - 1));
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<NotSupportedException>(() => fixture.Client.SubscribePresenceAsync());
    }

    [Fact]
    public async Task UnsubscribePresenceAsync_OnPrePresencePeer_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion((byte)(Protocol.PresenceMinVersion - 1));
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await Assert.ThrowsAsync<NotSupportedException>(() => fixture.Client.UnsubscribePresenceAsync());
    }

    [Fact]
    public async Task SubscribePresenceAsync_AtPresenceMinVersion_Succeeds()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion(Protocol.PresenceMinVersion);
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        await fixture.Client.SubscribePresenceAsync();
    }
}
