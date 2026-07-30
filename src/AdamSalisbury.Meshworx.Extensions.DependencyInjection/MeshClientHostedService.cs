using AdamSalisbury.Meshworx.Transport;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdamSalisbury.Meshworx.Extensions.DependencyInjection;

/// <summary>
/// Connects the keyed <see cref="IMeshClient"/> registered for <paramref name="clientName"/> when the host
/// starts, and disconnects it when the host begins a graceful shutdown.
/// </summary>
/// <remarks>
/// Registered by <c>AddMeshClient</c>, never constructed directly by an integrator. Whether the connection
/// is a plain <see cref="MeshClient"/> or one managed by a <see cref="MeshClientReconnector"/> is decided
/// from <see cref="MeshClientOptions.UseReconnector"/> at start time, since the option may be bound from
/// configuration and is not known until the options pipeline resolves it.
/// <para>
/// A reconnector-backed client is started via <see cref="MeshClientReconnector.StartAsync"/> so its
/// disconnect monitoring is wired up correctly, and is disposed directly on stop rather than left to the
/// dependency-injection container's own disposal of the reconnector's singleton: <see cref="IHost.StopAsync"/>
/// completing does not itself dispose the root service provider — a caller that stops a host without also
/// disposing it would otherwise leave the reconnector's background loop running and the connection open.
/// <see cref="MeshClientReconnector.DisposeAsync"/> is idempotent, so the later container disposal of the
/// same singleton is a safe no-op once this has already run.
/// </para>
/// <para>
/// The initial connection — on either path — is retried with a back-off delay under the host's own start
/// token, rather than failing <see cref="StartAsync"/> on the first attempt. Two failure modes this
/// closes: a plain connection attempt had no timeout of its own, relying entirely on the host's start
/// token, which by default (<c>HostOptions.StartupTimeout</c>) never fires — so host startup could hang
/// for the underlying transport's own connect timeout (on TCP, the OS default of roughly two minutes), or
/// for ever against a custom <see cref="MeshClientOptions.TransportFactory"/> with none. And registering
/// <c>AddMeshClient</c> before <c>AddMeshHub</c> — a natural reading order for "this app has a client, and
/// also hosts the hub" — otherwise fails host startup outright, since <see cref="IHostedService"/> start
/// order is registration order and the hub's listener is not accepting connections yet. Retrying tolerates
/// both: a hub that has not finished starting, in the same process or another, converges within a few
/// attempts rather than killing the host.
/// </para>
/// </remarks>
internal sealed class MeshClientHostedService(
    string clientName,
    IServiceProvider serviceProvider,
    IOptionsMonitor<MeshClientOptions> optionsMonitor) : IHostedService
{
    private static readonly TimeSpan DefaultConnectTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan DefaultConnectRetryDelay = TimeSpan.FromSeconds(1);

    // Set once, in StartAsync, and read back unchanged in StopAsync — deliberately not re-read from
    // optionsMonitor a second time. IOptionsMonitor.Get returns whatever the options pipeline currently
    // holds, which can differ from what it held at start under any reloading configuration source
    // (appsettings.json with the default reloadOnChange: true, a mounted ConfigMap, Azure App
    // Configuration). StopAsync must tear down what StartAsync actually started — the reconnector if that
    // is what was started, the plain client if that is what was started — never whichever the option
    // currently says, or a reload between start and stop takes shutdown down the wrong branch entirely:
    // disposing a reconnector that was never started leaks the one that was, or worse, first-resolving and
    // disposing a reconnector that was never started at all while the plain client that is actually
    // connected is left open. This mirrors the keyed IMeshClient factory, which effectively snapshots
    // UseReconnector at first resolution in exactly the same way.
    private bool _startedWithReconnector;

    /// <inheritdoc/>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        MeshClientOptions options = optionsMonitor.Get(clientName);
        _startedWithReconnector = options.UseReconnector;

        return ConnectWithRetryAsync(options, cancellationToken);
    }

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        if (_startedWithReconnector)
        {
            MeshClientReconnector reconnector = serviceProvider.GetRequiredKeyedService<MeshClientReconnector>(clientName);
            return reconnector.DisposeAsync().AsTask();
        }

        IMeshClient client = serviceProvider.GetRequiredKeyedService<IMeshClient>(clientName);
        return client.DisconnectAsync(cancellationToken);
    }

    /// <summary>
    /// Attempts the client's initial connection, retrying with a back-off delay under
    /// <paramref name="cancellationToken"/> until it succeeds, that token is cancelled, or the failure is
    /// one retrying can never fix — a configuration error, or a permanent hub refusal — in which case it
    /// propagates immediately instead.
    /// </summary>
    /// <remarks>
    /// On the reconnector path, a failed <see cref="MeshClientReconnector.StartAsync"/> call resets the
    /// reconnector's own started flag specifically so it can be retried — calling it again here is the
    /// intended recovery, not a workaround. On the plain path, each attempt is bounded by
    /// <see cref="MeshClientOptions.ConnectTimeout"/> directly, since <see cref="ConnectAsync"/> otherwise
    /// has no timeout of its own; the reconnector path needs no equivalent wrapping here because
    /// <see cref="MeshClientReconnector.StartAsync"/> already bounds its own first attempt with
    /// <see cref="MeshClientOptions.ReconnectConnectTimeout"/>.
    /// </remarks>
    private async Task ConnectWithRetryAsync(MeshClientOptions options, CancellationToken cancellationToken)
    {
        TimeSpan retryDelay = options.ConnectRetryDelay ?? DefaultConnectRetryDelay;
        ILogger<MeshClientHostedService>? logger = serviceProvider.GetService<ILogger<MeshClientHostedService>>();

        while (true)
        {
            try
            {
                if (options.UseReconnector)
                {
                    await serviceProvider.GetRequiredKeyedService<MeshClientReconnector>(clientName)
                        .StartAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    TimeSpan connectTimeout = options.ConnectTimeout ?? DefaultConnectTimeout;
                    using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    attemptCts.CancelAfter(connectTimeout);
                    await ConnectAsync(options, attemptCts.Token).ConfigureAwait(false);
                }

                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The host itself is shutting down, or has given up on startup entirely
                // (HostOptions.StartupTimeout) — propagate rather than retrying into a host that no
                // longer wants us to.
                throw;
            }
            catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
            {
                // MeshClient.ConnectAsync's own precondition checks — an oversized or empty client name,
                // or an unexpected connection state — throw exactly these two types. Retrying cannot fix
                // a configuration error: calling ConnectAsync again with the same invalid clientName just
                // throws the identical exception for ever, spinning the loop uselessly instead of
                // surfacing a clear, actionable failure. Propagate immediately, the same as the host's own
                // cancellation above.
                throw;
            }
            catch (RegistrationRefusedException ex) when (ex.ErrorCode is
                RegistrationErrorCode.UnsupportedProtocolVersion
                or RegistrationErrorCode.ClientNameTooLong
                or RegistrationErrorCode.AuthenticationFailed)
            {
                // A version mismatch, an over-length name, or a rejected credential are all refusals the
                // hub will repeat for the identical request every time — retrying only means re-presenting
                // the same, possibly sensitive, credential indefinitely at the retry interval rather than
                // surfacing a clear failure. DuplicateClientName and HubAtCapacity are deliberately not
                // listed here: both are genuinely worth retrying, since a departing previous instance of
                // this same client freeing its name, or the hub freeing a client slot, are exactly the
                // kind of transient condition this whole retry loop exists to tolerate.
                throw;
            }
            catch (Exception ex)
            {
                // Either this attempt's own timeout fired, or it failed outright (connection refused, DNS
                // failure, and the like). Either way the hub may simply not be up yet — in a real
                // deployment it is very often a separate process — so retry rather than killing the host.
                // Logged at Warning, matching MeshClientReconnector's own equivalent retry log, so a hub
                // that stays unreachable for a long time is visible rather than silent.
                logger?.LogWarning(
                    ex, "Client {ClientName} failed to connect; retrying in {RetryDelay}", clientName, retryDelay);
                await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task ConnectAsync(MeshClientOptions options, CancellationToken cancellationToken)
    {
        IMeshClient client = serviceProvider.GetRequiredKeyedService<IMeshClient>(clientName);
        Func<CancellationToken, Task<ITransport>> transportFactory =
            options.TransportFactory ?? (ct => ConnectDefaultTransportAsync(options, ct));

        ITransport transport = await transportFactory(cancellationToken).ConfigureAwait(false);
        await client.ConnectAsync(transport, options.ClientName, options.Credential, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<ITransport> ConnectDefaultTransportAsync(
        MeshClientOptions options, CancellationToken cancellationToken)
    {
        return await TcpTransport.ConnectAsync(options.Host, options.Port, cancellationToken).ConfigureAwait(false);
    }
}
