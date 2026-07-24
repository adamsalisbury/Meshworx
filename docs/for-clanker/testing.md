# Tests, fixtures, build & CI

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [transport.md](transport.md)

The test suite is the best source of **intended usage** and the required place to prove a change. Stack:
**xUnit** + **Moq**, `net10.0`, in `src/Tests/AdamSalisbury.Meshworx.UnitTests`. `coverlet.collector` is
wired for coverage. `IsPackable=false`; suppresses `CA1707` (underscore test names), `CA2007`, and
`xUnit1030`.

## Layout

| File | Lines | Covers |
|---|---|---|
| `Fixtures/MeshHubFixture.cs` | 173 | Hub test harness (mock listener/transport, register helpers, authenticator pass-through) |
| `Fixtures/MeshClientFixture.cs` | 106 | Client test harness (mock transport, scripted receive) |
| `MeshHubTests.cs` | 1588 | Registration, **authentication**, routing, broadcast, groups, heartbeat, capacity, lifecycle |
| `MeshClientTests.cs` | 1367 | Connect/disconnect, send/broadcast/group, lookup correlation, idle timeout, events |
| `MeshClientReconnectorTests.cs` | 479 | Fail-fast start, reconnect-on-drop, coalescing, `Reconnected`, credential replay |
| `MeshIntegrationTests.cs` | 327 | Hub + real clients over `InMemoryTransport`, end-to-end |
| `Transport/InMemory/InMemoryTransportTests.cs` | 173 | Pair semantics, copy-on-send, close signalling |
| `Transport/Tcp/TcpTransportTests.cs` | 342 | Framing, oversize rejection, invalid length, batch send |
| `Transport/Tcp/TcpTransportLoopbackTests.cs` | 70 | Round-trip over a real stream |
| `Transport/Tcp/TcpTransportListenerTests.cs` | 47 | Start/accept/dispose |

## Testing conventions (follow these)

- **Unit tests mock `ITransport` / `ITransportListener` with Moq — no real sockets.** The transport
  contract is the seam. Integration tests use `InMemoryTransport` for a real end-to-end path without
  ports.
- **Drive the receive loop with a `Channel`, not `SetupSequence` returning `null`.** A completed/`null`
  receive is now interpreted as a lost connection and triggers teardown. `MeshClientFixture`
  (`Fixtures/MeshClientFixture.cs:44-63`) writes the registration response and scripted frames into an
  **unbounded channel left uncompleted**, so the loop stays alive awaiting more — exactly like a live
  transport. The blocking read honours the cancellation token, so `DisconnectAsync` cancels cleanly.
- **Synchronise on observable state, not sleeps.** `MeshHubFixture.RegisterClientAsync`
  (`Fixtures/MeshHubFixture.cs`) captures the `RegistrationComplete` frame via a `SendAsync` callback,
  extracts the id, then spins on `IsClientRegistered(id)` with `Task.Yield()` until the hub has recorded
  the client. Copy this rather than `Task.Delay`.
- **Fixture helpers build wire frames by hand** (`CreateRegistrationRequest`, `CreateDeliverMessagePayload`,
  `CreateLookupFound/NotFoundResponse`) with the raw opcodes — a useful cross-check of
  [protocol.md](protocol.md). If you change a frame layout, these helpers must change too.
  `CreateRegistrationRequest(name, credential)` builds a **version 3** frame
  (`[0x04][0x03][nameLen u16 BE][name][credential]`) and hard-codes the version byte, so a
  `Protocol.Version` bump requires editing it (`Fixtures/MeshHubFixture.cs:60-72`).
- **Test the authenticator through the hub, not in isolation.** `MeshHubFixture` takes `authenticator`
  and `maxConcurrentAuthentications` pass-throughs, so the seam is exercised over the real registration
  path. The `HandleClient_Authenticator*` tests in `MeshHubTests.cs` cover the outcomes worth copying:
  rejection, throw, `OperationCanceledException` from inside the callback, a hanging callback refused at
  `registrationTimeout`, concurrency bounded by the semaphore, and a successful admit carrying name plus
  credential. `HandleClient_EmptyClientName_DropsConnectionWithoutRegistering` covers the malformed-frame
  path.
- Test names use `Method_State_ExpectedBehaviour` with underscores (hence `CA1707` suppressed).

### Minimal end-to-end pattern (integration style)

```csharp
var listener = new InMemoryTransportListener();
await using var hub = new MeshHub(NullLogger<MeshHub>.Instance, listener);
await hub.StartAsync();

await using var alice = new MeshClient(NullLogger<MeshClient>.Instance);
await alice.ConnectAsync(listener.Connect(), "Alice");
```

## Build & CI

- **Local:** `dotnet build Meshworx.slnx -c Release` then `dotnet test Meshworx.slnx -c Release`.
- **CI** (`.github/workflows/ci.yml`) runs on push/PR to `main`: checkout → setup .NET `10.0.x` →
  `restore` → `build -c Release --no-restore` → `test -c Release --no-build`, all against the **root**
  `Meshworx.slnx`. There is no publish/pack step.
- **Warnings are errors** (`Directory.Build.props` `TreatWarningsAsErrors=true`, `AnalysisLevel
  latest-recommended`, `EnforceCodeStyleInBuild`). A style/analyser violation fails the build. Notable
  local overrides: `CA2007` (ConfigureAwait) is a **warning = error** in the library but suppressed in
  the test and HubApp projects; the library also `NoWarn`s `CA1873` and `CS1591`.
- "Done" = clean Release build **and** green tests. Match the existing test density: every new opcode /
  branch / edge case has a focused test.
