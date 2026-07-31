using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AdamSalisbury.Meshworx.Admin;

/// <summary>
/// Exposes an <see cref="IMeshHub"/>'s administrative surface — <see cref="IMeshHub.GetClients"/>,
/// <see cref="IMeshHub.GetGroups"/>, <see cref="IMeshHub.GetTopics"/> and
/// <see cref="IMeshHub.DisconnectClient"/> — over a minimal HTTP/JSON REST API, for ops tooling that
/// would rather speak HTTP than the mesh's own wire protocol.
/// </summary>
/// <remarks>
/// <para>
/// <b>Opt-in and separate from the hub's own port.</b> This server is never started as a side effect of
/// <see cref="IMeshHub.StartAsync"/> — it is an entirely independent component, constructed and started
/// by the integrator against whichever <see cref="Uri"/> prefix they choose, which must never be the
/// mesh's own messaging endpoint: this speaks HTTP, the mesh speaks its own hand-rolled binary protocol
/// over a completely different transport, so the two cannot share a listener even by accident. Binding
/// this to a loopback or otherwise internal-only address, and fronting it with whatever network
/// boundary the deployment already trusts, is strongly recommended — this class enforces no network
/// isolation of its own.
/// </para>
/// <para>
/// <b>Separately secured.</b> The constructor requires an <see cref="AdminRequestAuthenticator"/> — there
/// is no unauthenticated default, unlike an optional <c>ClientAuthenticator</c> on <see cref="MeshHub"/>
/// itself, since an admin surface with no authentication at all would be a materially worse default for
/// a component whose entire purpose is inspecting and disconnecting other people's connections.
/// </para>
/// <para>
/// <b>No effect on the routing hot path.</b> Every request reads the hub's own registries once, on
/// demand, through the same public <see cref="IMeshHub"/> methods any other integrator could call
/// in-process — nothing here runs unless a request actually arrives, and nothing on the messaging path
/// is aware this server exists.
/// </para>
/// <para>
/// Routes: <c>GET /clients</c>, <c>GET /groups</c>, <c>GET /topics</c>, and
/// <c>POST /clients/{id}/disconnect</c> with an optional <c>{"reason": "..."}</c> JSON body. Every
/// response is JSON, including a refusal or an unrecognised route.
/// </para>
/// </remarks>
public sealed class MeshHubAdminServer : IAsyncDisposable
{
    // Bounds ReadDisconnectReasonAsync's body read — see its own remarks for why this exists
    // independently of the server's own shutdown token.
    private static readonly TimeSpan BodyReadTimeout = TimeSpan.FromSeconds(5);

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly IMeshHub _hub;
    private readonly HttpListener _listener;
    private readonly AdminRequestAuthenticator _authenticator;
    private readonly ILogger<MeshHubAdminServer> _logger;
    private readonly ConcurrentDictionary<Task, byte> _requestTasks = new();

    private Task? _acceptLoopTask;
    private CancellationTokenSource? _stoppingCts;
    private int _disposed;

    /// <summary>
    /// Initialises a new instance of <see cref="MeshHubAdminServer"/>, ready to <see cref="StartAsync"/>.
    /// </summary>
    /// <param name="hub">The hub whose administrative surface this exposes over HTTP.</param>
    /// <param name="listenPrefix">
    /// The URI prefix to listen on, in the shape <see cref="HttpListener"/> expects — for example
    /// <c>http://127.0.0.1:9200/</c>. Must not be the mesh's own messaging endpoint.
    /// </param>
    /// <param name="authenticator">Decides whether an incoming request may proceed. Required.</param>
    /// <param name="logger">
    /// A logger for this server's own diagnostics. Defaults to <see cref="NullLogger{T}.Instance"/> when
    /// not supplied.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="hub"/>, <paramref name="listenPrefix"/> or <paramref name="authenticator"/> is
    /// <see langword="null"/>.
    /// </exception>
    public MeshHubAdminServer(
        IMeshHub hub,
        Uri listenPrefix,
        AdminRequestAuthenticator authenticator,
        ILogger<MeshHubAdminServer>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(hub);
        ArgumentNullException.ThrowIfNull(listenPrefix);
        ArgumentNullException.ThrowIfNull(authenticator);

        _hub = hub;
        _authenticator = authenticator;
        _logger = logger ?? NullLogger<MeshHubAdminServer>.Instance;

        string prefix = listenPrefix.AbsoluteUri;
        if (!prefix.EndsWith('/'))
        {
            prefix += "/";
        }

        _listener = new HttpListener();
        _listener.Prefixes.Add(prefix);
    }

    /// <summary>
    /// Gets whether this server is currently listening.
    /// </summary>
    public bool IsRunning => _acceptLoopTask is not null;

    /// <summary>
    /// Starts listening for administrative requests.
    /// </summary>
    /// <exception cref="ObjectDisposedException">This server has been disposed.</exception>
    /// <exception cref="InvalidOperationException">The server is already running.</exception>
    public Task StartAsync()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        if (_acceptLoopTask is not null)
        {
            throw new InvalidOperationException("The admin server is already running.");
        }

        _listener.Start();
        _stoppingCts = new CancellationTokenSource();
        _acceptLoopTask = AcceptLoopAsync(_stoppingCts.Token);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops listening and waits for every in-flight request to finish. Calling this on a server that is
    /// not running does nothing.
    /// </summary>
    public async Task StopAsync()
    {
        if (_acceptLoopTask is not { } acceptLoopTask)
        {
            return;
        }

        _acceptLoopTask = null;

        // HttpListener.GetContextAsync offers no cancellation token of its own; Stop() is the documented
        // way to unblock a pending call to it, which the accept loop below treats as its own shutdown
        // signal via _stoppingCts having already been cancelled first.
        await _stoppingCts!.CancelAsync().ConfigureAwait(false);
        _listener.Stop();

        try
        {
            await acceptLoopTask.ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpListenerException or ObjectDisposedException)
        {
            // Expected: Stop() faults whichever GetContextAsync call was in flight.
        }

        await Task.WhenAll(_requestTasks.Keys).ConfigureAwait(false);

        _stoppingCts.Dispose();
        _stoppingCts = null;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            await StopAsync().ConfigureAwait(false);
            _listener.Close();
        }
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            HttpListenerContext context;
            try
            {
                context = await _listener.GetContextAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is HttpListenerException or ObjectDisposedException)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    // Stop() was called; this is the expected way an in-flight accept unblocks.
                    return;
                }

                // An unexpected accept failure while still meant to be running — logged and retried
                // rather than silently ending the accept loop, mirroring MeshHub's own transient-accept-
                // failure handling for its client listener.
                _logger.LogWarning(ex, "Admin server accept failed; retrying");
                continue;
            }

            Task requestTask = HandleRequestAsync(context, cancellationToken);
            _requestTasks.TryAdd(requestTask, 0);
            _ = requestTask.ContinueWith(
                t =>
                {
                    _requestTasks.TryRemove(t, out _);
                    if (t.IsFaulted)
                    {
                        _logger.LogError(t.Exception, "Unhandled exception in admin request handler");
                    }
                },
                TaskScheduler.Default);
        }
    }

    private async Task HandleRequestAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
        HttpListenerRequest request = context.Request;
        HttpListenerResponse response = context.Response;

        try
        {
            var authContext = new AdminRequestContext(
                request.HttpMethod, request.Url?.AbsolutePath ?? "/", request.Headers["Authorization"]);

            bool authorised;
            try
            {
                authorised = await _authenticator(authContext, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // A throwing authenticator refuses the request rather than faulting it open — the same
                // fail-closed rule MeshHub's own pluggable authenticators document.
                _logger.LogError(ex, "Admin request authenticator threw; refusing the request");
                authorised = false;
            }

            if (!authorised)
            {
                await WriteJsonAsync(
                    response,
                    HttpStatusCode.Unauthorized,
                    new AdminErrorResponse("Unauthorized"),
                    cancellationToken).ConfigureAwait(false);
                return;
            }

            await RouteAsync(request, response, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled exception handling an admin request");

            try
            {
                response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }
            catch (ObjectDisposedException)
            {
                // The response was already closed by whatever failed above; nothing further to report.
            }
            catch (InvalidOperationException)
            {
                // Headers were already sent for this response; the status code can no longer change.
            }
        }
        finally
        {
            response.Close();
        }
    }

    private async Task RouteAsync(
        HttpListenerRequest request, HttpListenerResponse response, CancellationToken cancellationToken)
    {
        string path = (request.Url?.AbsolutePath ?? "/").TrimEnd('/');
        string[] segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);

        if (request.HttpMethod == "GET" && segments is ["clients"])
        {
            await WriteJsonAsync(response, HttpStatusCode.OK, _hub.GetClients(), cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        if (request.HttpMethod == "GET" && segments is ["groups"])
        {
            await WriteJsonAsync(response, HttpStatusCode.OK, _hub.GetGroups(), cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        if (request.HttpMethod == "GET" && segments is ["topics"])
        {
            await WriteJsonAsync(response, HttpStatusCode.OK, _hub.GetTopics(), cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        if (request.HttpMethod == "POST"
            && segments is ["clients", var idSegment, "disconnect"]
            && Guid.TryParse(idSegment, out Guid clientId))
        {
            string? reason = await ReadDisconnectReasonAsync(request, cancellationToken).ConfigureAwait(false);
            bool disconnected = _hub.DisconnectClient(clientId, reason);
            await WriteJsonAsync(
                response,
                disconnected ? HttpStatusCode.OK : HttpStatusCode.NotFound,
                new DisconnectClientResponse(disconnected),
                cancellationToken).ConfigureAwait(false);
            return;
        }

        await WriteJsonAsync(
            response, HttpStatusCode.NotFound, new AdminErrorResponse("Not found"), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads an optional <c>{"reason": "..."}</c> JSON body, tolerating a missing, empty or malformed one
    /// — a kick is a diagnostic convenience, not something that should fail a caller's request over an
    /// optional field it got wrong.
    /// </summary>
    /// <remarks>
    /// Bounded by <see cref="BodyReadTimeout"/> independently of <paramref name="cancellationToken"/>: a
    /// client that opens the request but then sends its body slowly, or not at all, must not be able to
    /// hold this handler — and the request task tracked alongside every other in-flight one — open
    /// indefinitely. <see cref="HttpListenerRequest.HasEntityBody"/> is checked first purely as a fast
    /// path to skip touching <see cref="HttpListenerRequest.InputStream"/> at all for the common case of
    /// no body; it is not relied upon as the sole guard against a slow or absent body, since it reflects
    /// declared framing (a <c>Content-Length</c> or <c>Transfer-Encoding</c> header), not what the client
    /// actually goes on to send.
    /// </remarks>
    private async Task<string?> ReadDisconnectReasonAsync(
        HttpListenerRequest request, CancellationToken cancellationToken)
    {
        if (!request.HasEntityBody)
        {
            return null;
        }

        using var bodyReadCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bodyReadCts.CancelAfter(BodyReadTimeout);

        try
        {
            DisconnectClientRequest? body = await JsonSerializer.DeserializeAsync<DisconnectClientRequest>(
                request.InputStream, SerializerOptions, bodyReadCts.Token).ConfigureAwait(false);
            return body?.Reason;
        }
        catch (JsonException ex)
        {
            _logger.LogDebug(ex, "Malformed disconnect request body; proceeding with no reason");
            return null;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // BodyReadTimeout tripped rather than the server's own shutdown token — tolerated the same
            // way a malformed body is, rather than propagating and faulting the request.
            _logger.LogDebug("Timed out reading the disconnect request body; proceeding with no reason");
            return null;
        }
    }

    private static async Task WriteJsonAsync<T>(
        HttpListenerResponse response, HttpStatusCode statusCode, T body, CancellationToken cancellationToken)
    {
        response.StatusCode = (int)statusCode;
        response.ContentType = "application/json; charset=utf-8";
        await JsonSerializer.SerializeAsync(response.OutputStream, body, SerializerOptions, cancellationToken)
            .ConfigureAwait(false);
    }
}
