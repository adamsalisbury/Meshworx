using System.Net;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.UnitTests;

public sealed class TcpTransportListenerTests
{
    /// <summary>
    /// When StartAsync is called on a listener that is already running, an InvalidOperationException is thrown.
    /// </summary>
    [Fact]
    public async Task StartAsync_AlreadyRunning_ThrowsInvalidOperationException()
    {
        await using var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
    }

    /// <summary>
    /// When AcceptAsync is called before the listener has been started, an InvalidOperationException is thrown.
    /// </summary>
    [Fact]
    public async Task AcceptAsync_NotStarted_ThrowsInvalidOperationException()
    {
        await using var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// When DisposeAsync is called on a started listener, the listener is stopped and subsequent calls to AcceptAsync throw InvalidOperationException.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_AfterStart_AcceptAsyncThrowsInvalidOperationException()
    {
        var listener = new TcpTransportListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync();

        await listener.DisposeAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.AcceptAsync());
    }
}
