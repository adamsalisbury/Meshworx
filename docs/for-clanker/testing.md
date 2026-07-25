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
| `MeshHubTests.cs` | 1951 | Registration, **authentication**, routing, broadcast, groups, **heartbeat schedule (eviction interval, N=1 no-probe boundary, no false eviction)**, capacity, lifecycle, **concurrent stop/dispose and start-vs-stop races** |
| `MeshClientTests.cs` | 1499 | Connect/disconnect, send/broadcast/group, lookup correlation, idle timeout, events, **local-disconnect vs. receive-loop teardown race** |
| `MeshClientReconnectorTests.cs` | 785 | Fail-fast start, reconnect-on-drop, coalescing, `Reconnected`, credential replay, **TLS transport factory**, **drop-before-subscription race, duplicate-signal settling, rejected-attempt transport disposal** |
| `MeshIntegrationTests.cs` | 385 | Hub + real clients over `InMemoryTransport`, end-to-end, plus **one mutual-TLS run over real sockets** |
| `Transport/InMemory/InMemoryTransportTests.cs` | 173 | Pair semantics, copy-on-send, close signalling |
| `Transport/InMemory/InMemoryTransportListenerTests.cs` | 90 | **Listener disposal contract in memory (4 tests):** accept after dispose, dispose-without-ever-starting, a queued connection closed rather than served, repeated/concurrent dispose |
| `Transport/Tcp/TcpTransportTests.cs` | 342 | Framing, oversize rejection, invalid length, batch send |
| `Transport/Tcp/TcpTransportLoopbackTests.cs` | 70 | Round-trip over a real stream |
| `Transport/Tcp/TcpTransportListenerTests.cs` | 282 | Start/accept/dispose (cleartext), **plus the disposal contract (11 tests): start-after-dispose, pending accept ended by dispose, accept raced against dispose, concurrent dispose (cleartext and TLS), `IsStoppedListenerFailure` vs the framework** |
| `Transport/Tcp/TcpTransportTlsTests.cs` | 494 | Server TLS, mutual TLS, rejection paths, handshake timeout, silent-peer flood, constructor guards, `TargetHost` defaulting, `IsEncrypted` |
| `Transport/Tcp/TlsOptionsCloneTests.cs` | 242 | Reflection-driven proof that both TLS option clones copy **every** settable property |
| `Transport/Tcp/TestCertificates.cs` | 67 | Helper: self-signed certificate generation and a pinning validation callback |

## Testing conventions (follow these)

- **Hub and client tests mock `ITransport` / `ITransportListener` with Moq — no real sockets.** The
  transport contract is the seam. Integration tests use `InMemoryTransport` for a real end-to-end path
  without ports. **The exception is the TCP transport's own tests**, which must exercise real sockets;
  see the TLS bullet below.
- **TLS tests bind loopback on port 0 and read the port back.** `new TcpTransportListener(new
  IPEndPoint(IPAddress.Loopback, 0), tlsOptions)` then `((IPEndPoint)listener.LocalEndPoint!).Port` —
  that `internal` property exists for exactly this. Certificates come from
  `TestCertificates.CreateSelfSigned(subjectName)` (`Transport/Tcp/TestCertificates.cs:17`) and trust is
  established with `TestCertificates.PinnedTo(cert)` (`:59`) on both ends, never by returning `true`
  from the validation callback. Every TLS test carries an explicit `[Fact(Timeout = …)]` (10–30 s) and
  disposes the listener in a `finally`, because these tests can genuinely hang rather than fail.
  Shrink the bounds under test — `tlsHandshakeTimeout: TimeSpan.FromMilliseconds(300)`,
  `maxConcurrentTlsHandshakes: 2` — rather than waiting out the production defaults.
- **Assert the negative directly when testing a denial-of-service property.** The silent-peer tests
  (`TcpTransportTlsTests.cs:207`, `:263`) make the point that a surviving-client assertion alone is not
  proof: one test asserts the abandoned peer's socket actually reaches end of stream (a zero-byte read),
  the other floods with more silent peers than there are handshake slots. Copy that pairing if you touch
  the pump — a test that only checks "a good client still got through" passes even with the protection
  removed.
- **To race two operations, dispatch both and release them together — never call them in sequence.**
  `AcceptAsync_RacedAgainstDispose_OnlyEverReportsDisposal` (`TcpTransportListenerTests.cs:136`) parks an
  accept and a dispose on separate `Task.Run`s, each awaiting a shared `SemaphoreSlim` that is then
  released twice, and repeats the whole thing 50 times. Sequencing them on one thread would not race at
  all: an accept issued first has always registered itself before it yields, so it would only ever
  exercise the pending-accept path. Assert the **one** acceptable outcome
  (`Assert.IsType<ObjectDisposedException>`), not "did not throw" — a `NullReferenceException`, a
  "never started" claim and a raw socket error are each a distinct symptom the test exists to exclude.
  The same release-together shape drives the concurrent-dispose tests (`:181` cleartext, `:215` TLS,
  eight disposers each), which lock in a guarantee rather than reproduce a failure: the window they close
  was a few instructions wide and never reliably reproducible, so they assert the property the elected
  single teardown makes structural.
- **Where the behaviour you depend on is the platform's, assert against the platform, not through your
  wrapper.** `IsStoppedListenerFailure_WhatAStoppedTcpListenerActuallyThrows_IsRecognised` (`:99`) stops
  a real `System.Net.Sockets.TcpListener` twice — once under a pending accept, once before a fresh accept
  — and asserts `TcpTransportListener.IsStoppedListenerFailure` recognises whatever came back, naming the
  actual type in the failure message. That is the entire reason the predicate is `internal` rather than
  `private`: one of the three cases it covers is not reliably reachable through the listener, so a
  framework change would otherwise surface not as a red test but as a hot accept loop in production
  ([known-issues.md](known-issues.md) KI-22). Copy this shape whenever a guard is written against
  undocumented platform behaviour.
- **When the bug is "one interval late", assert a count, not an outcome.** The heartbeat tests
  (`MeshHubTests.cs:1695`, `:1743`) do not merely assert that a silent client was evicted — that passes
  whether eviction fires on the Nth or the (N+1)th interval, which was exactly the KI-11 defect. They
  count `Ping` frames in the mock's `SendAsync` callback and **snapshot the count inside the
  `DisposeAsync` setup**, so the teardown itself latches the value and no later write can inflate it,
  then assert it equals `maxMissedHeartbeats - 1`. Use a `TaskCompletionSource` completed from that same
  `DisposeAsync` callback to wait for eviction rather than sleeping. Copy this shape for any timing
  contract where the wrong answer is still a *plausible* answer. The complementary direction — a client
  that keeps sending is never evicted — is `HandleClient_ClientSendingFramesEveryInterval_IsNotEvicted`
  (`:1787`); it deliberately runs at `maxMissedHeartbeats: 3` with a send cadence well inside the
  interval so a scheduling stall on a loaded runner cannot masquerade as a genuine eviction.
- **Drive the receive loop with a `Channel`, not `SetupSequence` returning `null`.** A completed/`null`
  receive is now interpreted as a lost connection and triggers teardown. `MeshClientFixture`
  (`Fixtures/MeshClientFixture.cs:44-63`) writes the registration response and scripted frames into an
  **unbounded channel left uncompleted**, so the loop stays alive awaiting more — exactly like a live
  transport. The blocking read honours the cancellation token, so `DisconnectAsync` cancels cleanly.
- **Pin a client-teardown race deterministically by parking the mocked `DisposeAsync`.** This is the
  reusable seam for anything that has to interleave with `HandleReceiveLoopTerminationAsync`, and it is
  worth knowing about before you invent something flakier. The teardown calls `CleanUpAsync`, which
  awaits `transport.DisposeAsync()` (`MeshClient.cs:848`, disposal at `:605`) — and that await sits
  **after** the loop has claimed the connection (`Connected` → `Disconnecting`, `:827-838`) but
  **before** it decides whether to raise `Disconnected` (`:850-869`). Returning an incomplete
  `ValueTask` from the `DisposeAsync` setup therefore parks the receive loop at precisely that point,
  for as long as the test needs:

  ```csharp
  var teardownClaimed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
  var releaseTeardown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
  fixture.Transport.Setup(t => t.DisposeAsync())
      .Returns(() =>
      {
          teardownClaimed.TrySetResult();
          return new ValueTask(releaseTeardown.Task);
      });
  ```

  `DisconnectAsync_RacesReceiveLoopTeardown_DoesNotRaiseDisconnected` (`MeshClientTests.cs:1267`) uses
  it to reproduce the KI-21/issue-#10 race exactly rather than hoping for it: write `null` to the
  receive channel to lose the connection remotely, `await teardownClaimed.Task`, call `DisconnectAsync`
  while the loop is pinned, then release. The mirror-image seam is **holding the outgoing `Disconnect`
  frame open in the `SendAsync` setup**, which parks `DisconnectAsync` itself in the `Disconnecting`
  state; `Disconnected_AfterAConcurrentDisconnectClaimedATeardown_StillRaisedOnTheNextDrop`
  (`:1334`) uses that to land a second, redundant `DisconnectAsync` on an in-flight first one, then
  reconnects over a second transport and asserts a genuine drop **still** raises
  `DisconnectReason.ConnectionLost` — the regression guard on the claim flag leaking across connections.
  Both carry `[Fact(Timeout = 5000)]`, because the failure mode is a hang.
- **When you park a race, prove the code reached the decision point — do not just settle.** The race
  test above asserts a negative ("no event"), which a test that merely stalls short of the decision
  would also satisfy, passing for entirely the wrong reason. It closes that hole by waiting on
  observable state that the teardown mutates *in the same locked block* in which it decides whether to
  raise: `Name` is cleared at `MeshClient.cs:855`, `raiseDisconnected` is read at `:861`. Only once
  `Client.Name` is empty is a short 250 ms settle meaningful, because by then only the few instructions
  between that lock release and the delegate invocation remain (`MeshClientTests.cs:1307-1321`). Copy
  this two-step — *wait for a marker past the decision, then settle briefly* — for any "did not happen"
  assertion where a stalled system-under-test would look identical to a correct one.

<a id="parking-a-caller-mid-lifecycle"></a>

- **Park a caller mid-*hub*-lifecycle with one of two seams.** The client-side seams above have hub-side
  equivalents, added with the lifecycle work in PR #64 (issue #12). Both are deterministic and, unlike
  the client seams, **neither needs a settle delay at all** — `MeshHub.StopAsync` is not `async`, so it
  takes its decision synchronously under the lock and has provably decided by the time it hands a task
  back. Prefer these over anything timing-based.

  1. **To park a caller *inside* a shutdown, hold the mocked `ITransport.SendAsync` open on the
     `Disconnect` frame.** The shutdown's notification loop (`MeshHub.cs:299-309`) awaits each client's
     `SendAsync` in turn, and that await sits **after** the caller has claimed the hub's state but
     **before** the shutdown has finished — so returning an incomplete task there pins the hub
     mid-shutdown for as long as the test needs. The helper is already written:

     ```csharp
     // MeshHubTests.cs:337 — fires onFrame, signals, then blocks until released
     ParkOnDisconnectFrame(client.Transport, notificationReached, releaseNotification,
         () => Interlocked.Increment(ref disconnectFrames));

     // Before the fix the first stop blocked inside the notification instead of
     // returning a task, so it has to run off-thread or the test parks itself.
     Task firstStop = Task.Run(() => fixture.Hub.StopAsync());
     await notificationReached.Task.WaitAsync(WaitTimeout);

     Task secondStop = fixture.Hub.StopAsync();
     Assert.Equal(1, Volatile.Read(ref disconnectFrames));   // no settle needed
     ```

     `StopAsync_CalledWhileAShutdownIsInFlight_NotifiesEachClientOnce` (`MeshHubTests.cs:131`) counts
     `Disconnect` frames to distinguish "joined the existing shutdown" from "started a second one";
     `..._ReturnsOnlyOnceTheShutdownCompletes` (`:163`) asserts on the joining caller's task instead.
     Note the first uses `RegisterMultiMessageClientAsync`, since the parked transport must survive more
     than one scripted frame.

  2. **To park a *start*, gate the mocked `ITransportListener.StartAsync`.** Returning an incomplete task
     there stops the hub at the exact point where it has claimed the running slot (`_starting = true`)
     but has not yet published `_cts` and `_acceptLoopTask` — the window that KI-23's fifth defect lived
     in:

     ```csharp
     fixture.Listener.Setup(l => l.StartAsync(It.IsAny<CancellationToken>()))
         .Returns(async () =>
         {
             listenerStarting.TrySetResult();
             await releaseListenerStart.Task.ConfigureAwait(false);
         });
     ```

     `StopAsync_WhileAStartIsInProgress_LeavesTheStartedHubIntact` (`MeshHubTests.cs:304`) lands a full
     `StopAsync` in that window, releases the start, and then proves the hub is *genuinely* running by
     registering a client over it — not merely that no exception was thrown. Copy that "prove it still
     works" ending; asserting the absence of a throw would pass against a hub that had been left
     inert.

  Both carry `[Fact(Timeout = 10000)]`, because the failure mode is a hang rather than an assertion
  failure. See [hub.md](hub.md#lifecycle) for the lifecycle contract these pin, and
  [known-issues.md](known-issues.md) KI-23.
- **Synchronise on observable state, not sleeps.** `MeshHubFixture.RegisterClientAsync`
  (`Fixtures/MeshHubFixture.cs`) captures the `RegistrationComplete` frame via a `SendAsync` callback,
  extracts the id, then spins on `IsClientRegistered(id)` with `Task.Yield()` until the hub has recorded
  the client. Copy this rather than `Task.Delay`.
- **The one sanctioned exception to that: proving something *did not* happen.** There is no observable
  state to wait on for "no spurious reconnect was queued" or "the loop is not stuck retrying", so the
  reconnector's negative tests shrink `retryDelay` to 10 ms, then settle for 400 ms and assert an
  attempt *count* — a stuck loop shows up as a count well past the expected one
  (`MeshClientReconnectorTests.cs:305-308`, `:385-388`). Pair the delay with a count assertion, never a
  bare "still connected" check, which would pass with the guard removed. Every such test carries an
  explicit `[Fact(Timeout = …)]`, because the failure mode under test is a hang.
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
- **`TlsOptionsCloneTests` is an intentional tripwire, not a normal test.** It reflects over every
  public settable property of `SslClientAuthenticationOptions` / `SslServerAuthenticationOptions`, sets
  each to a non-default value, clones, and asserts the value survived. **An unrecognised property type
  fails the test outright** — that is the design: a property added by a future .NET release must force
  someone to decide how it is carried, rather than being silently dropped from a security setting.
  `TargetHost` is excluded from the mechanical sweep because it is deliberately defaulted, and is
  asserted separately (`TlsOptionsCloneTests.cs:35-37`, `:55`). If this test fails after an SDK bump,
  fix the clone in `TcpTransport.CloneClientOptions` / `TcpTransportListener.CloneServerOptions` — do
  not add an exclusion.
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
