# Tests, fixtures, build & CI

[← back to index](../for-clanker.md) · related: [hub.md](hub.md) · [client.md](client.md) · [transport.md](transport.md)

The test suite is the best source of **intended usage** and the required place to prove a change. Stack:
**xUnit** + **Moq**, `net10.0`, in `src/Tests/AdamSalisbury.Meshworx.UnitTests`. `coverlet.collector` is
wired for coverage. `IsPackable=false`; suppresses `CA1707` (underscore test names), `CA2007`, and
`xUnit1030`.

**A second, separate suite** covers the DI/hosting package (PR #70, plus health checks added by PR #71):
`src/Tests/AdamSalisbury.Meshworx.Extensions.DependencyInjection.UnitTests`, same stack and
`NoWarn` set, seven files —
`MeshHubServiceCollectionExtensionsTests.cs`, `MeshClientServiceCollectionExtensionsTests.cs`,
`MeshHubHostedServiceTests.cs`, `MeshHubHealthCheckTests.cs`, `MeshClientHealthCheckTests.cs`,
`MeshHubHealthCheckBuilderExtensionsTests.cs` and `MeshClientHealthCheckBuilderExtensionsTests.cs`. Most
tests build a `ServiceCollection`/`ServiceProvider` or a real `HostBuilder`/`IHost` directly, mocking only
`ITransportListener`/`IMeshHub`/`ILogger`; the hosted-service **lifecycle** tests (host start/stop
connecting and disconnecting a client) run against a real `MeshHub` over `InMemoryTransport` rather than
a mock, because `ConnectAsync` performs a genuine registration handshake a bare transport mock cannot
answer (`MeshClientServiceCollectionExtensionsTests.cs:214-274`) — no real sockets are used anywhere in
this suite. `MeshClientServiceCollectionExtensionsTests.AddMeshClient_CalledTwiceForTheSameName_RegistersOnlyOneHostedService`
covers the double-registration guard described in [known-issues.md](known-issues.md) KI-30; an
`AddMeshHub`-called-twice scenario is not separately covered, since that path is deduplicated by the
framework itself (`AddHostedService<T>()`) rather than by anything this repo wrote. See
[dependency-injection.md](dependency-injection.md) for what the package does.

**The health-check test files follow the same "mock for unit behaviour, real object for the end-to-end
flip" split as the rest of the suite.** `MeshHubHealthCheckTests.cs`/`MeshClientHealthCheckTests.cs` test
`MeshHubHealthCheck`/`MeshClientHealthCheck` directly against a mocked `IMeshHub`/`IMeshClient` resolved
from a small `ServiceProvider`, including a not-registered case that asserts
`CheckHealthAsync` throws `InvalidOperationException` (proving the health check service, not the check
itself, is what turns that into a status — see [dependency-injection.md](dependency-injection.md#health-checks)).
`MeshHubHealthCheckBuilderExtensionsTests.cs`/`MeshClientHealthCheckBuilderExtensionsTests.cs` test
`AddMeshHub`/`AddMeshClient` through a real `HealthCheckService`, including one test each that drives a
**real** `MeshHub`/`MeshClient` through its actual start/stop or connect/disconnect lifecycle and asserts
the reported `HealthStatus` flips `Unhealthy` → `Healthy` → `Unhealthy` — the acceptance criteria from
issue #23 — rather than trusting a mocked `IsRunning`/`IsConnected` to stand in for the real thing.

## Layout

| File | Lines | Covers |
|---|---|---|
| `Fixtures/MeshHubFixture.cs` | 321 | Hub test harness (mock listener/transport, register helpers, **authenticator and group-authoriser pass-throughs**, group/lookup/direct frame builders, `FrameRecorder`, **`CreateRegistrationRequest`'s `versionMin`/`versionMax` parameters, PR #73**) |
| `Fixtures/MeshClientFixture.cs` | 130 | Client test harness (mock transport, scripted receive, **`CreateGroupJoinRefusal`**, **an 18-byte `RegistrationComplete` builder taking a negotiated-version byte, PR #73**) |
| `Fixtures/MetricsCapture.cs` | 88 | **Metrics test harness (PR #72):** a `MeterListener` filtered to one `Meter` reference plus one instrument name, capturing every measurement and its tags in recording order |
| `MeshHubTests.cs` | 2849 | Registration, **authentication**, routing, broadcast, groups, **heartbeat schedule (eviction interval, N=1 no-probe boundary, no false eviction)**, **capacity claim/release under a concurrent registration**, **groups as an authorisation boundary**, lifecycle, **concurrent stop/dispose and start-vs-stop races**, **`IsRunning`/`MaxClients` accessors (PR #71)**, **inverted/non-overlapping version-range refusal and highest-shared-version negotiation (PR #73)** |
| `MeshHubMetricsTests.cs` | 422 | **All five `MeshHub` instruments (PR #72):** connected-clients up/down counter, routed/dropped counters per direction/reason, the zero-recipient exclusion for broadcast, the outbound-queue-depth gauge |
| `MeshClientTests.cs` | 1636 | Connect/disconnect, send/broadcast/group, lookup correlation, idle timeout, events, **local-disconnect vs. receive-loop teardown race**, **group-join refusal handling**, **`NegotiatedProtocolVersion` recorded on connect and reset on disconnect (PR #73)** |
| `MeshClientReconnectorTests.cs` | 944 | Fail-fast start, reconnect-on-drop, coalescing, `Reconnected`, credential replay, **TLS transport factory**, **drop-before-subscription race, duplicate-signal settling, rejected-attempt transport disposal**, **restored membership re-authorised by the hub**, **the documented `Reconnected` handler idiom containing a post-suspension failure** |
| `MeshClientReconnectorMetricsTests.cs` | 73 | **The `meshworx.client.reconnects` counter (PR #72):** excluded on the initial `StartAsync` connect, incremented exactly once on a genuine reconnect |
| `MeshIntegrationTests.cs` | 482 | Hub + real clients over `InMemoryTransport`, end-to-end, plus **one mutual-TLS run over real sockets** and **non-member/unauthorised group paths** |
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
  (`MeshHubTests.cs:2103`, `:2151`) do not merely assert that a silent client was evicted — that passes
  whether eviction fires on the Nth or the (N+1)th interval, which was exactly the KI-11 defect. They
  count `Ping` frames in the mock's `SendAsync` callback and **snapshot the count inside the
  `DisposeAsync` setup**, so the teardown itself latches the value and no later write can inflate it,
  then assert it equals `maxMissedHeartbeats - 1`. Use a `TaskCompletionSource` completed from that same
  `DisposeAsync` callback to wait for eviction rather than sleeping. Copy this shape for any timing
  contract where the wrong answer is still a *plausible* answer. The complementary direction — a client
  that keeps sending is never evicted — is `HandleClient_ClientSendingFramesEveryInterval_IsNotEvicted`
  (`:2195`); it deliberately runs at `maxMissedHeartbeats: 3` with a send cadence well inside the
  interval so a scheduling stall on a loaded runner cannot masquerade as a genuine eviction.
- **Drive the receive loop with a `Channel`, not `SetupSequence` returning `null`.** A completed/`null`
  receive is now interpreted as a lost connection and triggers teardown. `MeshClientFixture`
  (`Fixtures/MeshClientFixture.cs:44-75`) writes the registration response and scripted frames into an
  **unbounded channel left uncompleted**, so the loop stays alive awaiting more — exactly like a live
  transport. The blocking read honours the cancellation token, so `DisconnectAsync` cancels cleanly.
- **Pin a client-teardown race deterministically by parking the mocked `DisposeAsync`.** This is the
  reusable seam for anything that has to interleave with `HandleReceiveLoopTerminationAsync`, and it is
  worth knowing about before you invent something flakier. The teardown calls `CleanUpAsync`, which
  awaits `transport.DisposeAsync()` (`MeshClient.cs:909`, disposal at `:640`) — and that await sits
  **after** the loop has claimed the connection (`Connected` → `Disconnecting`, `:888-899`) but
  **before** it decides whether to raise `Disconnected` (`:911-930`). Returning an incomplete
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

  `DisconnectAsync_RacesReceiveLoopTeardown_DoesNotRaiseDisconnected` (`MeshClientTests.cs:1370`) uses
  it to reproduce the KI-21/issue-#10 race exactly rather than hoping for it: write `null` to the
  receive channel to lose the connection remotely, `await teardownClaimed.Task`, call `DisconnectAsync`
  while the loop is pinned, then release. The mirror-image seam is **holding the outgoing `Disconnect`
  frame open in the `SendAsync` setup**, which parks `DisconnectAsync` itself in the `Disconnecting`
  state; `Disconnected_AfterAConcurrentDisconnectClaimedATeardown_StillRaisedOnTheNextDrop`
  (`:1437`) uses that to land a second, redundant `DisconnectAsync` on an in-flight first one, then
  reconnects over a second transport and asserts a genuine drop **still** raises
  `DisconnectReason.ConnectionLost` — the regression guard on the claim flag leaking across connections.
  Both carry `[Fact(Timeout = 5000)]`, because the failure mode is a hang.
- **When you park a race, prove the code reached the decision point — do not just settle.** The race
  test above asserts a negative ("no event"), which a test that merely stalls short of the decision
  would also satisfy, passing for entirely the wrong reason. It closes that hole by waiting on
  observable state that the teardown mutates *in the same locked block* in which it decides whether to
  raise: `Name` is cleared at `MeshClient.cs:916`, `raiseDisconnected` is read at `:922`. Only once
  `Client.Name` is empty is a short 250 ms settle meaningful, because by then only the few instructions
  between that lock release and the delegate invocation remain (`MeshClientTests.cs:1410-1424`). Copy
  this two-step — *wait for a marker past the decision, then settle briefly* — for any "did not happen"
  assertion where a stalled system-under-test would look identical to a correct one.

<a id="parking-a-caller-mid-lifecycle"></a>

- **Park a caller mid-*hub*-lifecycle with one of two seams.** The client-side seams above have hub-side
  equivalents, added with the lifecycle work in PR #64 (issue #12). Both are deterministic and, unlike
  the client seams, **neither needs a settle delay at all** — `MeshHub.StopAsync` is not `async`, so it
  takes its decision synchronously under the lock and has provably decided by the time it hands a task
  back. Prefer these over anything timing-based.

  1. **To park a caller *inside* a shutdown, hold the mocked `ITransport.SendAsync` open on the
     `Disconnect` frame.** The shutdown's notification loop (`MeshHub.cs:499-509`) awaits each client's
     `SendAsync` in turn, and that await sits **after** the caller has claimed the hub's state but
     **before** the shutdown has finished — so returning an incomplete task there pins the hub
     mid-shutdown for as long as the test needs. The helper is already written:

     ```csharp
     // MeshHubTests.cs:339 — fires onFrame, signals, then blocks until released
     ParkOnDisconnectFrame(client.Transport, notificationReached, releaseNotification,
         () => Interlocked.Increment(ref disconnectFrames));

     // Before the fix the first stop blocked inside the notification instead of
     // returning a task, so it has to run off-thread or the test parks itself.
     Task firstStop = Task.Run(() => fixture.Hub.StopAsync());
     await notificationReached.Task.WaitAsync(WaitTimeout);

     Task secondStop = fixture.Hub.StopAsync();
     Assert.Equal(1, Volatile.Read(ref disconnectFrames));   // no settle needed
     ```

     `StopAsync_CalledWhileAShutdownIsInFlight_NotifiesEachClientOnce` (`MeshHubTests.cs:133`) counts
     `Disconnect` frames to distinguish "joined the existing shutdown" from "started a second one";
     `..._ReturnsOnlyOnceTheShutdownCompletes` (`:165`) asserts on the joining caller's task instead.
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

     `StopAsync_WhileAStartIsInProgress_LeavesTheStartedHubIntact` (`MeshHubTests.cs:306`) lands a full
     `StopAsync` in that window, releases the start, and then proves the hub is *genuinely* running by
     registering a client over it — not merely that no exception was thrown. Copy that "prove it still
     works" ending; asserting the absence of a throw would pass against a hub that had been left
     inert.

  Both carry `[Fact(Timeout = 10000)]`, because the failure mode is a hang rather than an assertion
  failure. See [hub.md](hub.md#lifecycle) for the lifecycle contract these pin, and
  [known-issues.md](known-issues.md) KI-23.
- **Assert a resolved default through an `internal` accessor rather than waiting out a real interval.**
  Added with the finite-defaults work in PR #68 (issue #16), and following the same shape as
  `TryReserveClientSlot`/`ReleaseClientSlot` below: `GetHeartbeatIntervalForTesting()`
  (`MeshHub.cs:619-622`) is `internal` rather than `private` purely so a test can assert what
  `heartbeatInterval` resolved to — including the 30 s default and the
  `Timeout.InfiniteTimeSpan` opt-out — without a real `PeriodicTimer` interval ever having to elapse.
  `Constructor_HeartbeatIntervalNotSpecified_DefaultsToThirtySeconds` (`MeshHubTests.cs:1035`) and
  `Constructor_HeartbeatIntervalSetToInfinite_DisablesIdleEviction` (`:1048`) are the
  reference pair. Copy this shape — an `internal` read-only accessor plus `InternalsVisibleTo` — for any
  future constructor-resolved default that would otherwise need a real timer/interval to observe.
  `Constructor_MaxClientsNotSpecified_DefaultsToOneThousand` (`:1016`) pins the `maxClients` default the
  same way, but through `TryReserveClientSlot` directly (claiming it 1000 times) rather than a dedicated
  accessor, since `maxClients` has no equivalent private field worth exposing.
<a id="per-remote-endpoint-connection-cap"></a>
- **Give a mock transport a remote address to test the per-remote-endpoint cap.** Also added by PR #68.
  `MeshHubFixture.CreateMockTransport(IPEndPoint? remoteEndPoint = null)` and `RegisterClientAsync(string
  name, IPEndPoint? remoteEndPoint = null)` (`Fixtures/MeshHubFixture.cs:137`, `:157-158`) both grew an
  optional `remoteEndPoint` parameter — when given, the mock also implements
  `IRemoteEndPointTransport` and reports it, so it participates in the cap exactly as `TcpTransport`
  does. The four tests under the `// AcceptLoop — per-remote-endpoint connection cap` banner
  (`MeshHubTests.cs:1086`) are the reference set:
  - `AcceptLoop_TooManyConnectionsFromSameAddress_RefusesFurtherConnectionWithoutHandshake` proves the
    refusal happens **before any handshake** — it asserts `ReceiveAsync` was `Times.Never` called on the
    refused mock, not merely that registration failed.
  - `AcceptLoop_ConnectionFromCappedAddressDisconnects_FreesSlotForAnotherFromSameAddress` waits on
    `ClientDisconnected`, not a sleep, before attempting the replacement — the event only fires after
    both the client slot and the endpoint slot have been released, so waiting on it pins the replacement
    to a point where the address is provably free again.
  - `AcceptLoop_TransportWithoutRemoteEndPoint_IsNeverCappedByAddress` uses the fixture's default
    `RegisterClientAsync` (no `remoteEndPoint`), proving a transport that never reports an address is
    never subject to the cap at all.
  - `AcceptLoop_TwoIPv6AddressesInSamePrefix_ShareTheConnectionCap` and
    `AcceptLoop_TwoIPv6AddressesInDifferentPrefixes_HaveIndependentConnectionCaps` pin the `/64`
    normalisation both ways — same prefix shares the cap, different prefixes don't — using addresses
    (`2001:db8:1:1::1` / `::2` vs `2001:db8:2:2::1`) chosen so the difference falls inside vs outside the
    masked range. Copy this pairing if you extend `NormaliseForEndpointCap`: a single "it capped
    something" test cannot tell a correct `/64` mask from one that is off by a bit.
  See [hub.md](hub.md#per-remote-endpoint-connection-cap) and [known-issues.md](known-issues.md) KI-29.
- **Park a *registration* mid-admission by holding the authenticator open.** Added with the capacity
  claim in PR #65 (issue #13). A `ClientAuthenticator` that signals a TCS and then awaits a second one
  parks the registration at exactly the point past the pre-authentication early-out and immediately
  before the hub takes its capacity decision — the only window in which the racing state can be staged:

  ```csharp
  // MeshHubTests.cs:870
  var fixture = new MeshHubFixture(
      maxClients: 1,
      authenticator: async (_, ct) =>
      {
          authenticatorReached.TrySetResult();
          await releaseAuthenticator.Task.WaitAsync(ct);
          return true;
      });
  ...
  await authenticatorReached.Task.WaitAsync(WaitTimeout);

  // Stand in for a registration that holds the only slot but is not yet in _clients.
  Assert.True(fixture.Hub.TryReserveClientSlot());
  Assert.Equal(0, fixture.Hub.ConnectedClientCount);   // a count-based check would admit here
  ```

  The `Assert.Equal(0, ConnectedClientCount)` is the point of the test, not incidental: it demonstrates
  that the observable count *cannot* see the claim, which is why the old check-then-act was unsound.
  Staging the second registration directly through `TryReserveClientSlot` rather than racing two real
  registrations is what makes it deterministic — do not rewrite it as a thread race. See
  [known-issues.md](known-issues.md) KI-26.
- **Synchronise on observable state, not sleeps.** `MeshHubFixture.RegisterClientAsync`
  (`Fixtures/MeshHubFixture.cs`) captures the `RegistrationComplete` frame via a `SendAsync` callback,
  extracts the id, then spins on `IsClientRegistered(id)` with `Task.Yield()` until the hub has recorded
  the client. Copy this rather than `Task.Delay`.
- **The one sanctioned exception to that: proving something *did not* happen.** There is no observable
  state to wait on for "no spurious reconnect was queued" or "the loop is not stuck retrying", so the
  reconnector's negative tests shrink `retryDelay` to 10 ms, then settle for 400 ms and assert an
  attempt *count* — a stuck loop shows up as a count well past the expected one
  (`MeshClientReconnectorTests.cs:396-399`, `:476-479`). Pair the delay with a count assertion, never a
  bare "still connected" check, which would pass with the guard removed. Every such test carries an
  explicit `[Fact(Timeout = …)]`, because the failure mode under test is a hang.
- **The better way to prove "the hub did not send X": order, not time.** PR #66 added `FrameRecorder`
  (`Fixtures/MeshHubFixture.cs:222`), which records every frame written to one client's transport and
  lets a test await the first frame matching a predicate. The group-authorisation tests use it to assert
  an *absence* deterministically: after the frame that must not appear, they have the hub send one that
  certainly will — a direct message to the same client — and because a client's outbound queue is drained
  **in order**, the later frame's arrival proves the earlier one was never queued. Prefer this to a
  settle-and-count wherever the thing you are excluding shares a queue with something you can trigger.
  See `SendToGroup_SenderIsNotAMember_MessageIsNotDelivered` (`MeshHubTests.cs:2348`).
- **Fixture helpers build wire frames by hand** (`CreateRegistrationRequest`, `CreateDeliverMessagePayload`,
  `CreateLookupFound/NotFoundResponse`) with the raw opcodes — a useful cross-check of
  [protocol.md](protocol.md). If you change a frame layout, these helpers must change too.
  `CreateRegistrationRequest(name, credential, versionMin=0x04, versionMax=0x04)` builds a
  `[0x04][versionMin][versionMax][nameLen u16 BE][name][credential]` frame, defaulting both version
  bytes to the current `Protocol.MinSupportedVersion`/`MaxSupportedVersion` (`Fixtures/MeshHubFixture.cs:118-131`,
  updated for the min/max negotiation in PR #73). Most call sites take the defaults; pass explicit
  `versionMin`/`versionMax` to exercise `TryNegotiateProtocolVersion`'s refusal paths (inverted or
  non-overlapping range) without touching `Protocol.cs` itself.
- **Test the authenticator through the hub, not in isolation.** `MeshHubFixture` takes `authenticator`
  and `maxConcurrentAuthentications` pass-throughs, so the seam is exercised over the real registration
  path. The `HandleClient_Authenticator*` tests in `MeshHubTests.cs` cover the outcomes worth copying:
  rejection, throw, `OperationCanceledException` from inside the callback, a hanging callback refused at
  `registrationTimeout`, concurrency bounded by the semaphore, and a successful admit carrying name plus
  credential. `HandleClient_EmptyClientName_DropsConnectionWithoutRegistering` covers the malformed-frame
  path.
- **Test the group authoriser the same way, through the hub.** `MeshHubFixture` takes `groupAuthoriser`
  and `groupAuthorisationTimeout` (`Fixtures/MeshHubFixture.cs:27-28`). The twelve
  `SendToGroup_*` / `JoinGroup_*` tests under the `// Groups as an authorisation boundary` banner
  (`MeshHubTests.cs:2328`) are the reference set — they are enumerated in
  [hub.md](hub.md#idiomatic-usage-from-tests). Note the hanging-authoriser test releases its callback at
  the end (`MeshHubTests.cs:2548`) so an abandoned task does not outlive the test; do the same, because
  the hub's timeout does not stop the callback (KI-28 in [known-issues.md](known-issues.md)).
- **End-to-end coverage lives in `MeshIntegrationTests.cs`**: `EndToEnd_NonMemberCannotSendToGroup`
  (`:152`) and `EndToEnd_UnauthorisedClientIsRefusedGroupMembership` (`:191`) run the rules over real
  `MeshClient`s and `InMemoryTransport`, and `RestoredGroupMembership_IsAuthorisedAgainByTheHub`
  (`MeshClientReconnectorTests.cs:722`) pins that a reconnect's restore goes through authorisation.
- **Code the README hands out carries a compiled guard.** Three tests in
  `MeshClientReconnectorTests.cs` exist only to keep a documented snippet honest, and are named
  `*FromDocumentation*` so they are findable: `Constructor_AcceptsTcpTransportFactoryFromDocumentation`
  (`:62`), `StartAsync_TlsTransportFactoryFromDocumentation_ConnectsOverEncryptedTransport` (`:79`) and
  `Reconnected_HandlerIdiomFromDocumentation_ContainsPostSuspensionFailure` (`:124`, added with the
  README's **Event handlers** subsection in PR #67, issue #15). The last transcribes the documented
  handler idiom verbatim but for a catch block that records what it caught, then makes the mocked
  `SendAsync` throw **after a `Task.Yield()`** — past the point at which an `async void` handler would
  have returned to the reconnect loop — and asserts the handler's own `catch` observed the `IOException`;
  a second drop afterwards proves the loop was unharmed. Copy that shape when you change a README
  snippet: fail *after* the suspension, because a failure before it is contained by the loop's own
  callback boundary and would pass against the very idiom the test exists to exclude. See the callback
  boundary in
  [for-clanker.md](../for-clanker.md#4-threading--async-model-read-before-changing-any-loop).
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

<a id="metrics-tests"></a>
- **Filter a `MeterListener` by `Meter` reference, not by instrument name alone, when testing metrics.**
  Added with the instrumentation in PR #72 (issue #24). `MetricsCapture<T>`
  (`Fixtures/MetricsCapture.cs`) wraps a `MeterListener` whose `InstrumentPublished` callback enables
  measurement events only when `ReferenceEquals(instrument.Meter, meter)` **and** the name matches
  (`:27`); every value and its tags are appended in recording order (`Values`, `Tags`). Construct one
  from `fixture.Hub.GetMeterForTesting()` or `reconnector.GetMeterForTesting()` — both `internal`
  accessors exist for exactly this — so a test is immune to another `MeshHub`/`MeshClientReconnector`
  publishing to the same meter name concurrently, in the same test class or a parallel one. Call
  `RecordObservableInstruments()` to force an `ObservableGauge` to report immediately rather than waiting
  out its own collection cycle — `OutboundQueueDepth_MessagesQueued_ReportsPositiveAggregateDepth`
  (`MeshHubMetricsTests.cs:375`) does this to assert `meshworx.hub.outbound_queue.depth` without a real
  collector attached.
  - **Proving a broadcast/group send recorded `messages.routed` exactly once, not once per recipient,**
    needs a genuine multi-recipient send: `BroadcastMessage_MultipleRecipients_IncrementsRoutedCounterOnceTaggedBroadcast`
    (`:238`) registers three clients, asserts `routedCapture.Values` filtered to `direction=broadcast`
    contains a **single** `1L`, not a count matching the recipient total.
  - **Proving the zero-recipient case records nothing** is the harder direction, and both fan-out kinds
    have a dedicated test: `BroadcastMessage_SenderIsOnlyClient_DoesNotIncrementRoutedCounter` (`:282`)
    registers only the sender, sends a broadcast, then a **lookup on the same connection** as a barrier —
    since the hub processes one client's frames in order, the lookup's response proves the broadcast was
    already handled, so asserting `routedCapture.Tags` contains no `direction=broadcast` entry at that
    point is deterministic rather than a timing guess. Copy this barrier-then-assert-absence shape (the
    same one `FrameRecorder` uses elsewhere in this suite) for any "this must not have recorded" metrics
    assertion — a bare settle-and-check would pass just as easily against a regression that recorded the
    zero-recipient case anyway.
  - **`MeshClientReconnectorMetricsTests.cs`'s one test**,
    `Reconnects_AfterConnectionLost_IncrementsReconnectsCounter` (`:28`), asserts **both** halves of the
    exclusion in one run: `capture.Values` is empty immediately after `StartAsync`'s initial connect, then
    reads exactly `[1L]` after a real hub-initiated drop and reconnect to a second, freshly stood-up hub.
    Asserting only the second half would miss a regression that counted the initial connect too.

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
