using Moq;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests;

public sealed class MeshHubHostedServiceTests
{
    [Fact(Timeout = 1000)]
    public async Task StartAsync_DelegatesToTheHubsStartAsync()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        var sut = new MeshHubHostedService(hub.Object);

        await sut.StartAsync(CancellationToken.None);

        hub.Verify(h => h.StartAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 1000)]
    public async Task StopAsync_DelegatesToTheHubsStopAsync()
    {
        var hub = new Mock<IMeshHub>();
        hub.Setup(h => h.StopAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        var sut = new MeshHubHostedService(hub.Object);

        await sut.StopAsync(CancellationToken.None);

        hub.Verify(h => h.StopAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}
