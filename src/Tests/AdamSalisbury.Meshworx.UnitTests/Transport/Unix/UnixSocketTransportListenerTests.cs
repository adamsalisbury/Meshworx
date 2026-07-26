using AdamSalisbury.Meshworx.Transport.Unix;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Unix;

public sealed class UnixSocketTransportListenerTests
{
    /// <summary>
    /// When the path is null or empty, an ArgumentException is thrown.
    /// </summary>
    [Fact]
    public void Constructor_EmptyPath_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new UnixSocketTransportListener(string.Empty));
    }

    /// <summary>
    /// When StartAsync is called on a listener that is already running, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_AlreadyRunning_ThrowsInvalidOperationException()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When AcceptAsync is called before the listener has been started, an InvalidOperationException is thrown.
    /// </summary>
    [Fact(Timeout = 1000)]
    public async Task AcceptAsync_NotStarted_ThrowsInvalidOperationException()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);

        await Assert.ThrowsAsync<InvalidOperationException>(() => listener.AcceptAsync());

        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// When DisposeAsync is called on a started listener, a subsequent accept reports the disposal
    /// rather than hanging or reporting the listener as never started.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_AfterStart_AcceptAsyncThrowsObjectDisposedException()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// A disposed listener stays disposed rather than being restartable.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        await listener.DisposeAsync().ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.StartAsync());
    }

    /// <summary>
    /// DisposeAsync is safe to call more than once.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        await listener.DisposeAsync().ConfigureAwait(false);
        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Disposing the same listener from several threads at once does not throw: no call trips over state
    /// another has already cleared, and the listener is disposed once they have all returned.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_CalledConcurrently_DoesNotThrowAndLeavesListenerDisposed()
    {
        const int disposers = 8;

        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        using var start = new SemaphoreSlim(0, disposers);
        var disposals = new Task[disposers];

        for (int i = 0; i < disposers; i++)
        {
            disposals[i] = Task.Run(async () =>
            {
                await start.WaitAsync().ConfigureAwait(false);
                await listener.DisposeAsync().ConfigureAwait(false);
            });
        }

        start.Release(disposers);

        await Task.WhenAll(disposals).ConfigureAwait(false);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => listener.AcceptAsync());
    }

    /// <summary>
    /// A pending accept, raced against dispose, only ever ends in the disposal being reported.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task AcceptAsync_RacedAgainstDispose_OnlyEverReportsDisposal()
    {
        const int attempts = 25;

        for (int attempt = 0; attempt < attempts; attempt++)
        {
            string path = TempSocketPath.Create();
            var listener = new UnixSocketTransportListener(path);
            await listener.StartAsync().ConfigureAwait(false);

            using var released = new SemaphoreSlim(0, 2);

            Task<Exception?> acceptTask = Task.Run<Exception?>(async () =>
            {
                await released.WaitAsync().ConfigureAwait(false);
                return await Record.ExceptionAsync(() => listener.AcceptAsync()).ConfigureAwait(false);
            });

            Task disposeTask = Task.Run(async () =>
            {
                await released.WaitAsync().ConfigureAwait(false);
                await listener.DisposeAsync().ConfigureAwait(false);
            });

            released.Release(2);

            await disposeTask.ConfigureAwait(false);
            Exception? caught = await acceptTask.ConfigureAwait(false);

            Assert.IsType<ObjectDisposedException>(caught);
        }
    }

    /// <summary>
    /// A stale socket file left behind by a previous instance is deleted before binding, rather than
    /// making the bind fail with "address already in use" even though nothing is listening.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_StaleSocketFileExists_DeletesItAndBindsSuccessfully()
    {
        string path = TempSocketPath.Create();
        await File.WriteAllTextAsync(path, "stale").ConfigureAwait(false);

        var listener = new UnixSocketTransportListener(path);

        await listener.StartAsync().ConfigureAwait(false);

        await listener.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Disposing the listener deletes the socket file it created, leaving no artefact behind after a
    /// clean shutdown.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_DeletesTheSocketFile()
    {
        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);
        await listener.StartAsync().ConfigureAwait(false);

        Assert.True(File.Exists(path));

        await listener.DisposeAsync().ConfigureAwait(false);

        Assert.False(File.Exists(path));
    }

    /// <summary>
    /// The socket file is hardened to owner-only read/write by default, rather than being left at
    /// whatever the ambient umask happens to produce — the entire access-control model for this
    /// transport rests on the socket file's permissions, so a permissive default here would silently
    /// defeat it.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_Default_HardensSocketFileToOwnerOnly()
    {
        if (OperatingSystem.IsWindows())
        {
            // POSIX mode bits do not apply on Windows' AF_UNIX support, which uses NTFS ACLs instead.
            return;
        }

        string path = TempSocketPath.Create();
        var listener = new UnixSocketTransportListener(path);

        try
        {
            await listener.StartAsync().ConfigureAwait(false);

            UnixFileMode mode = File.GetUnixFileMode(path);
            Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, mode);
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A caller that explicitly needs broader access — a group-shared sidecar layout, say — can widen
    /// the socket file's mode via the constructor rather than being stuck with the owner-only default.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task StartAsync_CustomSocketFileMode_AppliesIt()
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        string path = TempSocketPath.Create();
        const UnixFileMode customMode = UnixFileMode.UserRead | UnixFileMode.UserWrite
            | UnixFileMode.GroupRead | UnixFileMode.GroupWrite;
        var listener = new UnixSocketTransportListener(path, socketFileMode: customMode);

        try
        {
            await listener.StartAsync().ConfigureAwait(false);

            Assert.Equal(customMode, File.GetUnixFileMode(path));
        }
        finally
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }
    }
}
