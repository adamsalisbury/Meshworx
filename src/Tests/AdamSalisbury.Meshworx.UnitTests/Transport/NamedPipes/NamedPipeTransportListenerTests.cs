using AdamSalisbury.Meshworx.Transport.NamedPipes;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.NamedPipes;

/// <summary>
/// Named pipes are a Windows-only mechanism. On every other platform, the only behaviour to verify is
/// that starting the listener fails clearly with <see cref="PlatformNotSupportedException"/> — the
/// happy path (accept, round-trip, dispose semantics) can only be exercised on Windows.
/// </summary>
public sealed class NamedPipeTransportListenerTests
{
    /// <summary>
    /// When the pipe name is null or empty, an ArgumentException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_EmptyPipeName_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new NamedPipeTransportListener(string.Empty));
    }

    /// <summary>
    /// When the maximum server instance count is not positive, an ArgumentOutOfRangeException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_NonPositiveMaxServerInstances_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new NamedPipeTransportListener("meshworx-test-pipe", maxServerInstances: 0));
    }

    /// <summary>
    /// On a non-Windows platform, StartAsync throws PlatformNotSupportedException rather than
    /// attempting to listen on a mechanism that does not exist there.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_NonWindowsPlatform_ThrowsPlatformNotSupportedException()
    {
        var listener = new NamedPipeTransportListener("meshworx-test-pipe");

        if (OperatingSystem.IsWindows())
        {
            // Nothing to assert here: on Windows this call genuinely starts listening. Dispose to avoid
            // leaking the pipe if this ever does run on a Windows agent.
            await listener.StartAsync().ConfigureAwait(false);
            await listener.DisposeAsync().ConfigureAwait(false);
            return;
        }

        await Assert.ThrowsAsync<PlatformNotSupportedException>(() => listener.StartAsync());

        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// DisposeAsync is safe to call even when the listener was never successfully started (because the
    /// platform check in StartAsync failed) — a listener must not require a successful start before it
    /// can be disposed safely.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task DisposeAsync_NeverStarted_DoesNotThrow()
    {
        var listener = new NamedPipeTransportListener("meshworx-test-pipe");

        await listener.DisposeAsync().ConfigureAwait(false);
        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// A disposed listener rejects a subsequent start.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task StartAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var listener = new NamedPipeTransportListener("meshworx-test-pipe");
        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.StartAsync());
    }

    /// <summary>
    /// AcceptAsync before StartAsync — or on a platform where StartAsync never actually started
    /// anything — throws InvalidOperationException rather than hanging.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AcceptAsync_NotStarted_ThrowsInvalidOperationException()
    {
        var listener = new NamedPipeTransportListener("meshworx-test-pipe");

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.AcceptAsync());

        await listener.DisposeAsync().ConfigureAwait(false);
    }
}
