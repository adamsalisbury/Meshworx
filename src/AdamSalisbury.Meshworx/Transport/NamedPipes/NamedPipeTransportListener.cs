using System.IO.Pipes;

namespace AdamSalisbury.Meshworx.Transport.NamedPipes;

/// <summary>
/// An <see cref="ITransportListener"/> implementation that accepts incoming connections over a Windows
/// named pipe.
/// </summary>
/// <remarks>
/// Named pipes are a Windows-only mechanism. <see cref="StartAsync"/> throws
/// <see cref="PlatformNotSupportedException"/> on every other operating system — use
/// <see cref="Unix.UnixSocketTransportListener"/> for the equivalent local inter-process transport on
/// Linux and macOS.
/// </remarks>
public sealed class NamedPipeTransportListener : ITransportListener
{
    private readonly string _pipeName;
    private readonly int _maxServerInstances;

    // Guards every mutable field below, following the same discipline as TcpTransportListener: each
    // caller takes the state it needs under the lock and then works from locals, and nothing that
    // blocks or awaits runs while holding it.
    private readonly Lock _stateLock = new();

    private CancellationTokenSource? _acceptCts;
    private volatile bool _disposed;
    private Task? _disposeTask;

    /// <summary>
    /// Creates a listener for the given pipe name.
    /// </summary>
    /// <param name="pipeName">The name to listen on.</param>
    /// <param name="maxServerInstances">
    /// The maximum number of simultaneous instances of this pipe the operating system will allow.
    /// Defaults to <see cref="NamedPipeServerStream.MaxAllowedServerInstances"/>.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="pipeName"/> is null or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxServerInstances"/> is not positive.</exception>
    public NamedPipeTransportListener(string pipeName, int? maxServerInstances = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipeName);

        if (maxServerInstances is { } max && max <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxServerInstances), "The maximum server instance count must be positive.");
        }

        _pipeName = pipeName;
        _maxServerInstances = maxServerInstances ?? NamedPipeServerStream.MaxAllowedServerInstances;
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">The listener has been disposed.</exception>
    /// <exception cref="PlatformNotSupportedException">The current operating system is not Windows.</exception>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_acceptCts is not null)
            {
                throw new InvalidOperationException("The listener is already running.");
            }

            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException(
                    "Named pipes are only supported on Windows. Use UnixSocketTransportListener for "
                        + "local inter-process communication on Linux and macOS.");
            }

            _acceptCts = new CancellationTokenSource();
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Each call creates its own <see cref="NamedPipeServerStream"/> instance and waits for a client to
    /// connect to it — the named-pipe API models "one waiting connection slot" as one server-stream
    /// instance, so a fresh instance is created for every accept rather than one instance being reused.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">
    /// The listener has been disposed, or was disposed while this accept was pending.
    /// </exception>
    public async Task<ITransport> AcceptAsync(CancellationToken cancellationToken = default)
    {
        CancellationTokenSource acceptCts;

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            acceptCts = _acceptCts ?? throw new InvalidOperationException("The listener has not been started.");
        }

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, acceptCts.Token);

        var serverStream = new NamedPipeServerStream(
            _pipeName,
            PipeDirection.InOut,
            _maxServerInstances,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous);
        try
        {
            await serverStream.WaitForConnectionAsync(linkedCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_disposed)
        {
            // Disposal cancelled the shared token underneath this accept. Report the disposal itself,
            // as TcpTransportListener's accept path does — the hub's accept loop stops on
            // ObjectDisposedException, whereas a plain OperationCanceledException from the caller's own
            // token would be treated differently.
            await serverStream.DisposeAsync().ConfigureAwait(false);
            throw new ObjectDisposedException(
                $"The {nameof(NamedPipeTransportListener)} is no longer accepting connections.");
        }
        catch
        {
            await serverStream.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return new NamedPipeTransport(serverStream);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Safe to call more than once, and from more than one thread at a time. Only the first call tears
    /// the listener down; every call — first or not — returns only once that teardown is complete, which
    /// includes cancelling any pending accept.
    /// </remarks>
    public ValueTask DisposeAsync()
    {
        Task disposal;

        lock (_stateLock)
        {
            if (_disposeTask is null)
            {
                _disposed = true;

                CancellationTokenSource? acceptCts = _acceptCts;
                _acceptCts = null;

                _disposeTask = DisposeCoreAsync(acceptCts);
            }

            disposal = _disposeTask;
        }

        return new ValueTask(disposal);
    }

    private static async Task DisposeCoreAsync(CancellationTokenSource? acceptCts)
    {
        if (acceptCts is not null)
        {
            await acceptCts.CancelAsync().ConfigureAwait(false);
            acceptCts.Dispose();
        }
    }
}
