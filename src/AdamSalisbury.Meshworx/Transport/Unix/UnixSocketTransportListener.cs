using System.Net.Sockets;

namespace AdamSalisbury.Meshworx.Transport.Unix;

/// <summary>
/// An <see cref="ITransportListener"/> implementation that accepts incoming connections over a Unix
/// domain socket bound at a filesystem path.
/// </summary>
/// <remarks>
/// Intended for a hub and its clients sharing one host — a sidecar or multi-process desktop/daemon
/// layout — where a Unix domain socket avoids the network stack overhead and open port a loopback TCP
/// listener would otherwise cost. Access is controlled by filesystem permissions on the bound path
/// rather than by anything Meshworx enforces itself; set the path's permissions accordingly.
/// </remarks>
public sealed class UnixSocketTransportListener : ITransportListener
{
    // Owner read/write only. Connecting to a Unix domain socket requires write permission on the
    // socket file (unix(7)), so this is the tightest mode that still lets the listener's own process
    // (and, on some platforms, only that process) use the socket at all. Left unset, the file's mode
    // would instead be whatever the hosting process's ambient umask happens to produce — commonly
    // world- or group-writable — silently defeating the filesystem-permission access control this
    // transport's whole security model rests on.
    private static readonly UnixFileMode DefaultSocketFileMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;

    private readonly string _path;
    private readonly bool _deleteExistingSocketFile;
    private readonly UnixFileMode _socketFileMode;

    // Guards every mutable field below, following the same discipline as TcpTransportListener: each
    // caller takes the state it needs under the lock and then works from locals, and nothing that
    // blocks or awaits runs while holding it.
    private readonly Lock _stateLock = new();

    private Socket? _listenSocket;
    private volatile bool _disposed;

    /// <summary>
    /// Creates a listener bound to the given filesystem path.
    /// </summary>
    /// <param name="path">
    /// The filesystem path to bind the Unix domain socket to. The containing directory must already
    /// exist; the socket file itself is created by <see cref="StartAsync"/>.
    /// </param>
    /// <param name="deleteExistingSocketFile">
    /// Whether to delete a pre-existing file at <paramref name="path"/> before binding — the usual cause
    /// is a previous instance that exited without cleaning up its socket file, which would otherwise make
    /// the bind fail with "address already in use" even though nothing is actually listening. Defaults to
    /// <see langword="true"/>. The same file is also deleted on <see cref="DisposeAsync"/>, so a clean
    /// shutdown leaves no artefact behind.
    /// </param>
    /// <param name="socketFileMode">
    /// The POSIX file mode to apply to the socket file once bound, on platforms that support setting
    /// one (see <see cref="OperatingSystem.IsWindows"/> — ignored there, since Windows' AF_UNIX support
    /// uses NTFS ACLs rather than POSIX mode bits). Defaults to owner read/write only, so no other local
    /// account can connect unless you explicitly widen it here. Only relax this if every other account
    /// that could reach the path is one you already trust with full access to the mesh.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="path"/> is null or empty.</exception>
    public UnixSocketTransportListener(
        string path, bool deleteExistingSocketFile = true, UnixFileMode? socketFileMode = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        _path = path;
        _deleteExistingSocketFile = deleteExistingSocketFile;
        _socketFileMode = socketFileMode ?? DefaultSocketFileMode;
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">The listener has been disposed.</exception>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_listenSocket is not null)
            {
                throw new InvalidOperationException("The listener is already running.");
            }

            if (_deleteExistingSocketFile && File.Exists(_path))
            {
                File.Delete(_path);
            }

            var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            try
            {
                socket.Bind(new UnixDomainSocketEndPoint(_path));

                // Harden the socket file's permissions immediately after bind and before Listen, so
                // there is no window in which the file exists with only the ambient umask's (commonly
                // far looser) permissions applied. Windows' AF_UNIX support uses NTFS ACLs rather than
                // POSIX mode bits, so File.SetUnixFileMode is neither meaningful nor supported there.
                if (!OperatingSystem.IsWindows())
                {
                    File.SetUnixFileMode(_path, _socketFileMode);
                }

                socket.Listen();
            }
            catch
            {
                socket.Dispose();
                throw;
            }

            _listenSocket = socket;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    /// <exception cref="ObjectDisposedException">
    /// The listener has been disposed, or was disposed while this accept was pending.
    /// </exception>
    public async Task<ITransport> AcceptAsync(CancellationToken cancellationToken = default)
    {
        Socket listenSocket;

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            listenSocket = _listenSocket ?? throw new InvalidOperationException("The listener has not been started.");
        }

        Socket accepted;
        try
        {
            accepted = await listenSocket.AcceptAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (_disposed)
        {
            // Disposal closed the listening socket underneath this accept. A closed Socket surfaces this
            // as ObjectDisposedException already on most platforms, but translate whatever it actually
            // threw so the hub's accept loop reliably stops rather than logging and retrying against a
            // listener that is never coming back — the same distinction TcpTransportListener draws.
            throw new ObjectDisposedException(
                $"The {nameof(UnixSocketTransportListener)} is no longer accepting connections.", ex);
        }

        return new UnixSocketTransport(accepted);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Safe to call more than once, and from more than one thread at a time. Only the first call tears
    /// the listener down; every call returns once that teardown — including deleting the socket file, if
    /// configured to — is complete.
    /// </remarks>
    public ValueTask DisposeAsync()
    {
        lock (_stateLock)
        {
            if (!_disposed)
            {
                _disposed = true;

                _listenSocket?.Dispose();
                _listenSocket = null;

                if (_deleteExistingSocketFile)
                {
                    TryDeleteSocketFile();
                }
            }
        }

        return ValueTask.CompletedTask;
    }

    private void TryDeleteSocketFile()
    {
        try
        {
            if (File.Exists(_path))
            {
                File.Delete(_path);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup only; a file that cannot be deleted (already gone, permissions,
            // another process holding it open on some platforms) must not fault disposal.
        }
        catch (UnauthorizedAccessException)
        {
            // As above.
        }
    }
}
