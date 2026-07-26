using AdamSalisbury.Meshworx.Transport.NamedPipes;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.NamedPipes;

/// <summary>
/// Named pipes are a Windows-only mechanism. On every other platform, the only behaviour to verify is
/// that connecting fails clearly with <see cref="PlatformNotSupportedException"/> rather than hanging or
/// failing with a confusing lower-level error — the happy path itself can only be exercised on Windows.
/// </summary>
public sealed class NamedPipeTransportTests
{
    /// <summary>
    /// When the pipe name is null or empty, an ArgumentException is thrown — checked before the
    /// platform guard, so it applies uniformly regardless of operating system.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_EmptyPipeName_ThrowsArgumentException()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => NamedPipeTransport.ConnectAsync(string.Empty));
    }

    /// <summary>
    /// On a non-Windows platform, ConnectAsync throws PlatformNotSupportedException rather than
    /// attempting a connection that could never succeed.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task ConnectAsync_NonWindowsPlatform_ThrowsPlatformNotSupportedException()
    {
        if (OperatingSystem.IsWindows())
        {
            // Nothing to assert here: on Windows this call is a genuine connection attempt, covered by
            // the listener/loopback tests instead.
            return;
        }

        await Assert.ThrowsAsync<PlatformNotSupportedException>(
            () => NamedPipeTransport.ConnectAsync("meshworx-test-pipe"));
    }
}
