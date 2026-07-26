# Dependency injection & hosting — `AddMeshHub` / `AddMeshClient`

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [testing.md](testing.md) · [known-issues.md](known-issues.md)

A second library, `AdamSalisbury.Meshworx.Extensions.DependencyInjection` (added by PR #70, merged as
`0d0c6ff`), that registers a `MeshHub` or `MeshClient` with `Microsoft.Extensions.DependencyInjection` and
runs it alongside a generic host or ASP.NET Core application — the composition-root equivalent of the
constructor calls documented in [for-clanker.md §2](../for-clanker.md#2-how-it-is-meant-to-be-used). It
is **purely additive**: registering and hosting a hub or client changed nothing in `AdamSalisbury.Meshworx`
itself, and it depends on the core library the same way any consumer would (`ProjectReference` only, no
`InternalsVisibleTo` into the core assembly). PR #71 (issue #23) added a **health-check integration** to
this same package — see [Health checks](#health-checks) below — which did need three new `IMeshHub`
members (`IsRunning`, `MaxClients`, `ClaimedClientSlots`, all implemented on `MeshHub`); those are
documented on [hub.md](hub.md), not here.

- **Extension methods** live in namespace `Microsoft.Extensions.DependencyInjection` (so they appear on
  `IServiceCollection` without an extra `using`), following the convention every `Add*` method in that
  ecosystem uses.
- **Options types and hosted services** live in namespace
  `AdamSalisbury.Meshworx.Extensions.DependencyInjection`.
- All types are `sealed`; the two hosted-service classes are `internal` — you never construct them
  directly, only through `AddMeshHub`/`AddMeshClient`.

---

## Hosting a hub — `AddMeshHub`

- **Type:** `public static class MeshHubServiceCollectionExtensions` —
  `src/AdamSalisbury.Meshworx.Extensions.DependencyInjection/MeshHubServiceCollectionExtensions.cs:15`

| Member | Signature | Source |
|---|---|---|
| `AddMeshHub` | `IServiceCollection AddMeshHub(this IServiceCollection, Action<MeshHubOptions>? configureOptions = null)` | `MeshHubServiceCollectionExtensions.cs:33` |
| `AddMeshHub` (config-bound) | `IServiceCollection AddMeshHub(this IServiceCollection, IConfiguration, Action<MeshHubOptions>? configureOptions = null)` | `MeshHubServiceCollectionExtensions.cs:56` |

```csharp
builder.Services.AddMeshHub(options =>
{
    options.Port = 22001;
    options.MaxClients = 1000;
});

// or, bound from configuration:
builder.Services.AddMeshHub(builder.Configuration.GetSection("MeshHub"));
```

### Using it efficiently

- Registers a **singleton `IMeshHub`** (`CreateHub`, `MeshHubServiceCollectionExtensions.cs:94-112`),
  built from a `TcpTransportListener` on `MeshHubOptions.Port` by default, or from
  `MeshHubOptions.Listener` when one is supplied (`:98`) — set `Listener` to get TLS, a non-loopback bind
  address, or `InMemoryTransportListener` instead. Every other `MeshHubOptions` property maps 1:1 to a
  `MeshHub` constructor parameter and carries the **same default**: an `AddMeshHub` call with no
  configuration at all builds the hub exactly as `new MeshHub(logger, listener)` would, including the
  finite defaults from PR #68 (see [known-issues.md](known-issues.md) KI-29 and
  [for-clanker.md §5](../for-clanker.md#5-configuration--environment)).
- Registers `MeshHubHostedService` (`MeshHubHostedService.cs:14`), whose `StartAsync`/`StopAsync` call
  straight through to `IMeshHub.StartAsync`/`StopAsync` (`:17-26`) — nothing more. The hub's own
  `DisposeAsync` runs when the **root service provider** is disposed, since the hub is a singleton;
  stopping a host without also disposing it therefore stops the hub cleanly but leaves the `IMeshHub`
  object itself un-disposed until the container is.
- `services.TryAddSingleton<IMeshHub>(...)` (`:88`) and `services.AddHostedService<MeshHubHostedService>()`
  (`:89`) are **both deduplicated** — calling `AddMeshHub` more than once on the same collection registers
  the hub and its hosted service exactly once; a later call still layers its `configureOptions` onto the
  same (unnamed) options pipeline via `Configure`/`Bind`. This holds because `AddHostedService<T>()` (the
  type-parameter overload) uses `TryAddEnumerable` under the hood — confirmed by inspection of the
  registered `IHostedService` collection, not merely inferred. **Contrast `AddMeshClient` below, which
  does not have this property.**
- Requires an `ILogger<MeshHub>` to be resolvable — call `AddLogging()` first if the host does not
  already provide one; `CreateHub` resolves it with `GetRequiredService` (`:97`) and throws
  `InvalidOperationException` at hub-creation time (i.e. at first resolution, or at host start if nothing
  resolves `IMeshHub` earlier) if none is registered.

### Contracts and gotchas

- **`Port` is validated, everything else is not — by this package.** `optionsBuilder.Validate(options =>
  options.Port is > 0 and <= 65535, ...)` plus `.ValidateOnStart()` (`:85-86`) means an out-of-range port
  throws `OptionsValidationException` at host start, before any socket is touched. Every other property
  (`MaxClients`, `HeartbeatInterval`, `MaxMissedHeartbeats`, `GroupAuthorisationTimeout`,
  `MaxConnectionsPerRemoteEndpoint`) is passed straight to the `MeshHub` constructor, which does its own
  range validation and throws `ArgumentOutOfRangeException` from inside the `IMeshHub` singleton's
  factory the first time it is resolved — not from `ValidateOnStart`, so a bad value there surfaces later
  and with a different exception type than a bad `Port` does.
- **`MaxClients` / `HeartbeatInterval` / `MaxConnectionsPerRemoteEndpoint` are `int?`/`TimeSpan?` on
  `MeshHubOptions`, matching the constructor exactly** (`MeshHubOptions.cs:46`, `:51`, `:81`) — leaving
  one unset is indistinguishable from never passing the parameter, so PR #68's finite defaults (1000 /
  30 s / 100) apply. There is no DI-specific default layered on top.
- **Only the first `AddMeshHub` call's hub configuration composes as "layered".** Because the options are
  unnamed (`services.AddOptions<MeshHubOptions>()` with no name), a second `AddMeshHub` call's
  `configureOptions`/`configuration` runs against the **same** `MeshHubOptions` instance the first call
  configured — there is no per-call isolation. This is consistent with there being only one hub per
  `IServiceCollection` (`TryAddSingleton`), but means a second call's `Bind`/`Configure` can silently
  override the first's.

---

## Hosting a client — `AddMeshClient`

- **Type:** `public static class MeshClientServiceCollectionExtensions` —
  `src/AdamSalisbury.Meshworx.Extensions.DependencyInjection/MeshClientServiceCollectionExtensions.cs:15`

| Member | Signature | Source |
|---|---|---|
| `AddMeshClient` | `IServiceCollection AddMeshClient(this IServiceCollection, string clientName, Action<MeshClientOptions>? configureOptions = null)` | `MeshClientServiceCollectionExtensions.cs:40` |
| `AddMeshClient` (config-bound) | `IServiceCollection AddMeshClient(this IServiceCollection, string clientName, IConfiguration, Action<MeshClientOptions>? configureOptions = null)` | `MeshClientServiceCollectionExtensions.cs:70` |

```csharp
builder.Services.AddMeshClient("Alice", options =>
{
    options.Host = "localhost";
    options.Port = 22001;
    options.UseReconnector = true;
});

var client = serviceProvider.GetRequiredKeyedService<IMeshClient>("Alice");
```

### The keyed-service model

`clientName` is used twice, for two different purposes, and `AddMeshClientCore` keeps them in step
(`MeshClientServiceCollectionExtensions.cs:83-126`):

1. It is the **DI key** every registration is filed under — `IMeshClient`, `MeshClientReconnector`, and
   the named options (`AddOptions<MeshClientOptions>(clientName)`, `:89`). Multiple clients in one
   process are just multiple `AddMeshClient` calls with different names, each independently resolvable
   with `GetRequiredKeyedService<IMeshClient>(name)`.
2. It is the **name the client registers with the hub under** — `PostConfigure(options =>
   options.ClientName = clientName)` (`:102`) runs *after* binding/configuring, so it always wins: a
   `ClientName` set in configuration or a configure delegate is silently overridden to match the service
   key. Set the client's registered name by choosing the key you call `AddMeshClient` with, not by
   setting `MeshClientOptions.ClientName` directly.

### Plain client vs. reconnector — `MeshClientOptions.UseReconnector`

The keyed `IMeshClient` registration is a factory that branches on the option at **resolution time**
(`:110-118`):

```csharp
services.TryAddKeyedSingleton<IMeshClient>(clientName, (serviceProvider, key) =>
{
    var name = (string)key!;
    MeshClientOptions options = serviceProvider.GetRequiredService<IOptionsMonitor<MeshClientOptions>>().Get(name);

    return options.UseReconnector
        ? serviceProvider.GetRequiredKeyedService<MeshClientReconnector>(name).Client
        : CreateClient(serviceProvider, options);
});
```

- **`UseReconnector = false` (default):** the keyed `IMeshClient` is a plain `MeshClient` built by
  `CreateClient` (`:146-156`), which forwards `IdleTimeout`, `SendTimeout`, `MaxSendAttempts` and
  `SendRetryDelay` to the `MeshClient` constructor unchanged.
- **`UseReconnector = true`:** the keyed `IMeshClient` is **the reconnector's own `Client` property**, not
  a second, independent client — resolving `MeshClientReconnector` first (also keyed by the same name,
  `:107-108`) and taking its managed client means callers see the same `IMeshClient` API surface either
  way, per the README's **Dependency injection and hosting** section. `CreateReconnector`
  (`:158-174`) builds the underlying `MeshClient` with `CreateClient` exactly as the plain path does, then
  wraps it in a `MeshClientReconnector` with `ReconnectRetryDelay`, `ReconnectConnectTimeout`,
  `RestoreGroupMembership` and `Credential` — the reconnector-specific options, ignored when
  `UseReconnector` is `false`.
- Both paths share one `TransportFactory` resolution: `options.TransportFactory ?? (ct =>
  ConnectDefaultTransportAsync(options, ct))` (`:163-164`, mirrored in `MeshClientHostedService.cs`),
  where the default connects over TCP to `Host`/`Port` (`ConnectDefaultTransportAsync`, `:178-182`). Set
  `TransportFactory` for TLS or a non-default transport; `Host`/`Port` are then ignored.

### `MeshClientHostedService` — connect on start, tear down on stop

- **Type:** `internal sealed class MeshClientHostedService` — `MeshClientHostedService.cs:28`.
- **`StartAsync`** (`:34-41`) re-reads `MeshClientOptions` from the `IOptionsMonitor` at start time (not
  capture time), then either starts the reconnector (`MeshClientReconnector.StartAsync`) or connects the
  plain client directly, branching on `UseReconnector` the same way the registration factory does.
- **`StopAsync`** (`:44-56`) does **not** rely on container disposal to release the reconnector. This was
  a deliberate fix during the PR #70 review: `IHost.StopAsync` completing does not itself dispose the
  root service provider, so a caller that stops a host without also disposing it would otherwise leave a
  reconnector's background reconnect loop running and its connection open. `StopAsync` therefore resolves
  the keyed service and tears it down **directly** — `reconnector.DisposeAsync()` for the
  reconnector-backed path, `client.DisconnectAsync(cancellationToken)` for the plain path — rather than
  leaving either to whatever disposes the DI container later. `MeshClientReconnector.DisposeAsync` is
  idempotent, so the container's own disposal of the same singleton after host shutdown is a safe
  no-op (see [client.md](client.md) for the reconnector's disposal contract).

### Contracts and gotchas

- **`AddMeshClient`'s hosted-service registration needs an explicit dedup guard — `AddMeshHub`'s doesn't.**
  The client's own singleton and reconnector registrations use `TryAddKeyedSingleton` (deduplicated by
  the framework), but the hosted service is registered with `services.AddHostedService(serviceProvider =>
  new MeshClientHostedService(...))` — the **factory overload**, which is a plain `AddSingleton` under the
  hood and is **not** deduplicated the way `AddHostedService<T>()` is for `AddMeshHub`. `AddMeshClientCore`
  therefore registers a private `MeshClientHostedServiceRegistrationMarker` as a keyed singleton per
  `clientName` before adding the hosted service, and skips the hosted-service registration on a repeat
  call for a name whose marker is already present — the moral equivalent, per client name, of what
  `TryAddEnumerable` gives `AddMeshHub` for free. This was caught and fixed during PR #70's review; see
  [known-issues.md](known-issues.md) KI-30. Do not remove the guard, and do not register the marker
  without also registering the hosted service (or vice versa) — they must stay in lock-step.
- **`Credential` is `byte[]?`, not `ReadOnlyMemory<byte>`, deliberately** (`MeshClientOptions.cs:60`). The
  configuration binder has no converter for `ReadOnlyMemory<byte>` and would silently leave it empty
  rather than fail, so a credential bound from a base64 configuration value (e.g. a mounted secret) would
  be dropped without error under the "obvious" type. This was caught and fixed during PR #70's review;
  `byte[]` is the type the binder actually supports, and it converts implicitly to
  `ReadOnlyMemory<byte>` at every call site that needs it (`ConnectAsync`, the reconnector constructor).
- **Resolving `IMeshClient` before the host starts gives you an unconnected client.** DI builds the
  `MeshClient`/`MeshClientReconnector` object eagerly on first resolution of the keyed singleton, but
  `ConnectAsync`/`StartAsync` only run from `MeshClientHostedService.StartAsync`. Calling `SendAsync` or
  any other method on an `IMeshClient` injected into something that runs before hosted services start
  throws `InvalidOperationException` ("Not connected to a hub.") — see [client.md](client.md).
- **The keyed `MeshClientReconnector` registration exists regardless of `UseReconnector`.** It is always
  registered (`:107-108`), because the `IMeshClient` factory needs to be able to resolve it when
  `UseReconnector` is `true` without knowing that in advance. Resolving `MeshClientReconnector` by key
  when `UseReconnector` is `false` is possible but **not what `AddMeshClient` wires up**: `CreateReconnector`
  builds its own, independent `MeshClient` via `CreateClient` (`:162`), so the result is a second,
  unconnected client that `MeshClientHostedService` never starts, stops, or otherwise manages. Resolve
  `IMeshClient` by the documented key, not `MeshClientReconnector` directly, unless you intend to drive it
  yourself.
- **Validation mirrors `AddMeshHub`'s asymmetry.** `Port` and `MaxSendAttempts` are checked by
  `.Validate(...).ValidateOnStart()` (`:103-105`, `OptionsValidationException` at host start); every other
  property (`IdleTimeout`, `SendTimeout`, `SendRetryDelay`, the reconnector options) is validated only by
  whichever downstream constructor (`MeshClient` or `MeshClientReconnector`) receives it, the first time
  the keyed singleton is actually resolved.

---

## Health checks

Added by PR #71 (issue #23), in the same package, against
`Microsoft.Extensions.Diagnostics.HealthChecks` (`PackageReference` added to both the package `.csproj`
and `Directory.Packages.props`). Two `IHealthChecksBuilder` extension methods, in the same
`Microsoft.Extensions.DependencyInjection` namespace and following the same "no extra `using`" convention
as `AddMeshHub`/`AddMeshClient` above — this matches the real ecosystem convention, since the framework's
own `AddHealthChecks()` lives there too, not in `Microsoft.Extensions.Diagnostics.HealthChecks`.

| Member | Signature | Source |
|---|---|---|
| `AddMeshHub` | `IHealthChecksBuilder AddMeshHub(this IHealthChecksBuilder, string name = "meshhub", HealthStatus? failureStatus = null, IEnumerable<string>? tags = null)` | `MeshHubHealthCheckBuilderExtensions.cs:29` |
| `AddMeshClient` | `IHealthChecksBuilder AddMeshClient(this IHealthChecksBuilder, string clientName, string? name = null, HealthStatus? failureStatus = null, IEnumerable<string>? tags = null)` | `MeshClientHealthCheckBuilderExtensions.cs:30` |

```csharp
builder.Services.AddHealthChecks()
    .AddMeshHub()
    .AddMeshClient("Alice");
```

Both require the hub or the named client to already be registered — call `AddMeshHub`/`AddMeshClient`
(the `IServiceCollection` extensions documented above) first. `AddMeshClient`'s `name` defaults to
`"meshclient:{clientName}"`; `AddMeshHub`'s to `"meshhub"`. Both back onto `internal sealed` `IHealthCheck`
implementations you never construct directly: `MeshHubHealthCheck` (`MeshHubHealthCheck.cs:21`) and
`MeshClientHealthCheck` (`MeshClientHealthCheck.cs:21`).

### Using it efficiently

- **`MeshHubHealthCheck`** (`CheckHealthAsync`, `MeshHubHealthCheck.cs:24-55`): `Unhealthy` while
  `!hub.IsRunning`; `Degraded` once `hub.ClaimedClientSlots >= hub.MaxClients` — the hub is still serving
  existing clients but refusing new ones; `Healthy` otherwise. **Capacity is judged against
  `ClaimedClientSlots`, not `ConnectedClientCount`** — a slot is claimed as soon as a connection is
  accepted and released only once its handler has fully finished, so `ClaimedClientSlots` is what the
  hub's own admission check enforces `MaxClients` against and it stays ahead of `ConnectedClientCount`
  while a client is mid-handshake or mid-teardown. Comparing `ConnectedClientCount` instead would let this
  check report `Healthy` while the hub was already refusing connections — see [hub.md](hub.md) and
  [known-issues.md](known-issues.md) KI-26. The result's `data` dictionary carries
  `connectedClientCount`, `claimedClientSlots` and `maxClients` — see the exposure caveat below before
  wiring this to an unauthenticated endpoint.
- **`MeshClientHealthCheck`** (`CheckHealthAsync`, `MeshClientHealthCheck.cs:24-32`): `Healthy` while
  `client.IsConnected`, `Unhealthy` otherwise — including while a `MeshClientReconnector` is still
  retrying, since the managed client reports disconnected until it reconnects (see
  [client.md](client.md)).
- **Both resolve their target from an injected `IServiceProvider` inside `CheckHealthAsync`, not as a
  constructor dependency captured by the `HealthCheckRegistration` factory delegate.** This is deliberate:
  `MeshHubHealthCheck` resolves `IMeshHub` with `GetRequiredService` and `MeshClientHealthCheck` resolves
  the keyed `IMeshClient` with `GetRequiredKeyedService` (both inside `CheckHealthAsync`), so a hub or
  named client that was never registered surfaces as a resolution failure **caught by the health check
  service** and mapped to the registration's configured `failureStatus` — not as an unhandled exception
  thrown before that service's own try/catch runs. `AddMeshHub_HubNotRegistered_ReportsUnhealthyRatherThanThrowing`
  and `AddMeshClient_ClientNotRegistered_ReportsUnhealthyRatherThanThrowing` are the regression tests for
  this; keep resolving inside `CheckHealthAsync` if you touch either check.

### Contracts and gotchas

- **Exposing either check over an unauthenticated HTTP endpoint leaks operational detail.** Neither check
  does any endpoint-mapping itself — that is entirely the integrator's call, typically via ASP.NET Core's
  `MapHealthChecks` — but if you do map one without authentication, the response's `data` (connected and
  claimed client counts, `MaxClients`, and the client name embedded in `AddMeshClient`'s default check
  name) is visible to any caller who can reach the endpoint. See [known-issues.md](known-issues.md) KI-31.
- **A health check added without its matching `AddMeshHub`/`AddMeshClient` call reports the registration's
  failure status, not a 500 from an unhandled exception** — see "Using it efficiently" above. This means a
  typo'd `clientName` in `AddMeshClient(...)` (health check) that does not match the one passed to the
  `IServiceCollection` `AddMeshClient(...)` call fails quietly as `Unhealthy` (or whatever `failureStatus`
  you configured) rather than crashing health-check evaluation — check the reported exception in the
  `HealthReport` entry if a check you expect to pass never does.



- Neither extension throws if called with a `null` `IServiceCollection` silently — both guard with
  `ArgumentNullException.ThrowIfNull(services)` at the top of every public overload, consistent with the
  house style ([for-clanker.md §6](../for-clanker.md#6-cross-cutting-conventions-imitate-these)).
- Both rely on `Options.ValidateOnStart()`, which requires the generic host's options-validation feature
  to actually run — that happens automatically for any host built with
  `Host.CreateApplicationBuilder`/`WebApplication.CreateBuilder`; a hand-rolled `ServiceProvider` built
  without going through `IHost` will build the options but never call the validators, so a bad `Port`
  would only surface the first time something resolves `IOptions<MeshHubOptions>`/
  `IOptionsMonitor<MeshClientOptions>`, not proactively at "start". *(Inference from how `ValidateOnStart`
  is documented to hook into `IHostedService`; not separately exercised by this package's own tests.)*
- Neither package references the console test apps (`HubApp`/`ClientApp`), and the test apps do not
  reference this package either — they still construct `MeshHub`/`MeshClient` directly. The two are
  independent, parallel ways of standing up the same types.

## Known issues

See [known-issues.md](known-issues.md) KI-30 for the `AddMeshClient` hosted-service duplication described
above — found and fixed before merge, in the same PR that introduced it — and KI-31 for the
health-check-endpoint-exposure caveat described under [Health checks](#health-checks). Nothing else here
is registered in the known-issues table; the design otherwise matches the core library's documented
defaults exactly.
