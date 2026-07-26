using System.IO.Pipes;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;

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
    private readonly PipeSecurity? _pipeSecurity;

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
    /// <param name="pipeSecurity">
    /// The security descriptor to apply to the pipe, restricting which local principals may open it.
    /// Left unset, the pipe defaults to permitting only the current user — Windows' own platform
    /// default for an unspecified <see cref="PipeSecurity"/> is considerably broader (it grants read
    /// access to the Everyone group and the anonymous account alongside full control to LocalSystem,
    /// administrators and the creator owner), which would silently defeat this transport's entire
    /// pipe-name access-control model. Supply your own only for a deployment that genuinely needs a
    /// different or wider set of principals — for example, several distinct service accounts on the
    /// same host that all need to reach this pipe.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="pipeName"/> is null or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxServerInstances"/> is not positive.</exception>
    public NamedPipeTransportListener(
        string pipeName, int? maxServerInstances = null, PipeSecurity? pipeSecurity = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipeName);

        if (maxServerInstances is { } max && max <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxServerInstances), "The maximum server instance count must be positive.");
        }

        _pipeName = pipeName;
        _maxServerInstances = maxServerInstances ?? NamedPipeServerStream.MaxAllowedServerInstances;
        _pipeSecurity = pipeSecurity;
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

        // StartAsync already refused to run at all on a non-Windows platform, so by the time any accept
        // reaches here the process is guaranteed to be on Windows — the only platform PipeSecurity,
        // PipeAccessRule and WindowsIdentity are meaningful on. NamedPipeServerStreamAcl.Create is the
        // ACL-aware factory the System.IO.Pipes.AccessControl package provides in place of the
        // PipeSecurity-accepting constructor .NET Framework used to have.
#pragma warning disable CA1416 // Windows-only API: guarded at run time by the platform check in StartAsync.
        NamedPipeServerStream serverStream = NamedPipeServerStreamAcl.Create(
            _pipeName,
            PipeDirection.InOut,
            _maxServerInstances,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous,
            inBufferSize: 0,
            outBufferSize: 0,
            _pipeSecurity ?? CreateCurrentUserOnlyPipeSecurity());
#pragma warning restore CA1416
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

    /// <summary>
    /// Builds a <see cref="PipeSecurity"/> granting full control to the current user only.
    /// </summary>
    /// <remarks>
    /// This is the default used when no <see cref="PipeSecurity"/> is supplied to the constructor.
    /// Windows' own default for a <see cref="NamedPipeServerStream"/> constructed without one is
    /// considerably broader — it also grants read access to the Everyone group and the anonymous
    /// account — which would silently defeat the pipe-name access-control model this transport's
    /// security relies on. Only reachable on Windows: every call site checks
    /// <see cref="OperatingSystem.IsWindows"/> first.
    /// </remarks>
#pragma warning disable CA1416 // Windows-only API: every caller is already gated on OperatingSystem.IsWindows() in StartAsync.
    [SupportedOSPlatform("windows")]
    private static PipeSecurity CreateCurrentUserOnlyPipeSecurity()
    {
        SecurityIdentifier currentUser = WindowsIdentity.GetCurrent().User
            ?? throw new InvalidOperationException("Unable to determine the current user's security identifier.");

        var security = new PipeSecurity();
        security.AddAccessRule(
            new PipeAccessRule(currentUser, PipeAccessRights.FullControl, AccessControlType.Allow));
        return security;
    }
#pragma warning restore CA1416
}
