<!-- for-clanker:freshness
repo: Meshworx (github.com/adamsalisbury/Meshworx)
scope: full
reconciled-to-commit: 12b2785 (branch feature/rpc-request-response, PR #83, open, not yet merged to main) — two commits on top of main at 4b11234, clean working tree
reconciled-to-date: 2026-07-27
mode: update
-->

# Meshworx — coding agent field guide

This is the entry point. Read it in full before touching the code, then jump to the area file for
whatever you are changing. Every claim here is grounded in the source; where something is inferred
rather than read directly, it says so.

> **Documented tree, this pass:** branch `feature/rpc-request-response` (open **PR #83**, not tied to a
> numbered issue in this handover), two commits (`71ad727`, `12b2785`) on top of `main` at `4b11234`
> (`git merge-base main feature/rpc-request-response` confirmed equal to `main`'s own tip — `main` has
> advanced to `4b11234`, "feat: QUIC transport with multiplexed streams (System.Net.Quic)", which **is**
> the previous pass's PR #82 merged), clean working tree throughout (`git status --porcelain` empty at
> both the start and end of this pass). Diffed with `git diff main...feature/rpc-request-response --stat`:
> 6 files, 781 insertions/12 deletions — `IMeshClient.cs` (+53), `MeshClient.cs` (+249/−12), a new
> `Messages/RequestReplyHeaderKeys.cs` (26 lines), a 12-line addition to
> `Messages/MessageReceivedEventArgs.cs`, and growth in `MeshClientTests.cs` (+415) and
> `MeshIntegrationTests.cs` (+38). **No `MeshHub.cs`, `IMeshHub.cs`, transport, or DI-package file is
> touched at all** — confirmed directly from the diff stat — so nothing in [hub.md](for-clanker/hub.md),
> [transport.md](for-clanker/transport.md) or [dependency-injection.md](for-clanker/dependency-injection.md)
> needed correcting; this branch is entirely a `MeshClient`-side addition.
>
> **This branch adds a correlated request/response helper — `IMeshClient.RequestAsync`/`ReplyAsync`.**
> `RequestAsync(Guid recipientId, ReadOnlyMemory<byte> message, TimeSpan timeout, CancellationToken)`
> sends a direct message and awaits a correlated reply, or fails with `TimeoutException`;
> `ReplyAsync(MessageReceivedEventArgs request, ReadOnlyMemory<byte> message, CancellationToken)` answers
> one. **Deliberately built on the existing header envelope (PR #74) rather than as new wire protocol**:
> a request/reply pair are ordinary `SendMessageWithHeaders`/`DeliverMessageWithHeaders` (`0x11`/`0x12`)
> frames carrying two new well-known keys (`Messages/RequestReplyHeaderKeys.cs`, `internal`) —
> `"mesh.request-id"` (both frames) and `"mesh.reply"` (the reply only). **No new opcode, no protocol
> version bump** — `Protocol.MaxSupportedVersion` stays `5`. `MessageReceivedEventArgs` gains
> `long? CorrelationId` (`null` for an ordinary message, set for an incoming request), and the receive
> loop's `DeliverMessageWithHeaders` branch now checks `TryCompletePendingRequest` **before** raising
> `MessageReceived` at all, so a reply frame never surfaces through the event — this makes KI-9's
> "dispatch ladder gains a branch per opcode" pattern not quite universal any more: this is a *nested*
> check inside an existing branch, not a new one. A reply is accepted **only from the client the request
> was actually addressed to** (`PendingRequest.ExpectedResponderId` checked against the frame's real
> sender), so a third client on the same hub cannot forge a reply to someone else's request. Concurrent
> `RequestAsync` calls on the same client are fully independent (`ConcurrentDictionary`-backed
> `_pendingRequests`, no shared serialisation the way `GetClientIdByNameAsync`'s single-slot lookup has).
> Full write-up in [client.md](for-clanker/client.md#request-response) and
> [protocol.md](for-clanker/protocol.md#request-response-headers).
>
> **Two new known issues, both low severity, both by design — recorded as KI-42 and KI-43.** `SendAsync`'s
> pre-existing headers overload now throws `ArgumentException` if a caller's own `MessageHeaders` contains
> either reserved key (`ThrowIfReservedHeaderKeyPresent`, new) — a narrow but real breaking change for any
> application that already used one of those two exact strings as its own header key (KI-42). And because
> the hub never decodes header *content* (only header-block *length*, unchanged since PR #74), request/
> response correlation is enforced entirely by the two `MeshClient` instances involved — any inbound frame
> carrying `mesh.reply=1`, from **any** sender, real `RequestAsync` caller or not, is intercepted and
> dropped before `MessageReceived`, which only matters for a non-`MeshClient` peer or hand-built frame
> (KI-43). Neither is a defect in the shipped feature; both are documented so a future change does not
> "fix" either in a way that breaks the design. This section's own capability table (§2) and pitfalls
> (§8) below reflect the new capability; [known-issues.md](for-clanker/known-issues.md) carries the full
> entries.
>
> **Coordinate shift, this pass: `MeshClient.cs` +225 net lines (1138 → 1363) across nine separate
> insertion points, `IMeshClient.cs` +53 in one place (after `GetClientIdByNameAsync`, so nothing above it
> moved).** Derived from `git diff main...feature/rpc-request-response -- '*.cs' | grep '^@@'` and verified
> by comparing cited-line content before and after re-pointing, the same technique validated on every
> prior pass back to #64. Every `MeshClient.cs`/`IMeshClient.cs` coordinate in
> [client.md](for-clanker/client.md), [protocol.md](for-clanker/protocol.md),
> [known-issues.md](for-clanker/known-issues.md) and this file was re-pointed this pass. **Two pre-existing,
> off-by-a-few-lines citations were found and corrected while re-pointing** (not caused by this branch, but
> free to fix while the file was open anyway, per the standing "touching the file makes fixing it free"
> rule from earlier passes): [client.md](for-clanker/client.md)'s class-declaration citation
> (`MeshClient.cs:9` → true `:12`) and [protocol.md](for-clanker/protocol.md)'s `ClientLookupResponse`/
> `Ping`-reply citations (`:717-724`/`:726-737`, which had pointed at unrelated cleanup code for at least
> one prior pass → true `:1096-1103`/`:1105-1118`), both flagged inline where corrected.
> [testing.md](for-clanker/testing.md) was **not** re-pointed — its `MeshClient.cs:909`/`:640`/`:888-899`/
> `:911-930`/`:916`/`:922` citations (in the client-teardown-race parking section) are now stale by this
> branch's shift and are flagged here rather than silently left wrong: per the user's own standing
> instruction not to fully re-derive individual-test citations, and because this branch's own diff does
> not touch `MeshClientTests.cs`'s pre-existing content (only appends new tests after it), reconciling
> those six citations is deferred to a pass that opens `testing.md` for its own reasons.
>
> **Documented tree (prior pass):** branch `feat/issue-21-quic-transport` (open **PR #82**, closing
> issue #21, **not yet merged to `main`**), two commits on top of `main` at `84fd51f`
> (`git merge-base main feat/issue-21-quic-transport` confirmed equal to `main`'s own tip), clean
> working tree. Diffed with `git diff main...feat/issue-21-quic-transport --stat`: 9 files, 2016
> insertions — two new source files (`Transport/Quic/QuicTransport.cs`,
> `Transport/Quic/QuicTransportListener.cs`), five new test files (all under
> `src/Tests/AdamSalisbury.Meshworx.UnitTests/Transport/Quic/`), a `README.md` update, and a
> `.github/workflows/ci.yml` step installing `libmsquic` before build/test. **`MeshHub.cs`,
> `MeshClient.cs` and `IMeshClient.cs` are all untouched** — confirmed directly from the diff stat, not
> assumed from the branch's own description — so nothing in [hub.md](for-clanker/hub.md) or
> [client.md](for-clanker/client.md) needed correcting; this branch is transport-only, exactly as issue
> #21's own design scoped it.
>
> **This branch adds a seventh transport** (counting `InMemoryTransport`), `QuicTransport`/
> `QuicTransportListener` (namespace `AdamSalisbury.Meshworx.Transport.Quic`), backed by
> `System.Net.Quic` — one bidirectional `QuicStream` per connection, framed via the same shared
> `StreamFramer` helper `TcpTransport`/`UnixSocketTransport`/`NamedPipeTransport` already use (now four
> transports on one framing implementation, not three). **Two things distinguish it from every prior
> transport added by these last three passes:** TLS is **mandatory**, not optional — QUIC requires it at
> the protocol level, so there is no cleartext overload at all, unlike TCP/WebSocket's opt-in TLS and
> unlike Unix-socket/named-pipe's total absence of a TLS concept — and it **does** implement the public
> `IRemoteEndPointTransport`, reporting the connection's genuine remote address on both sides, so unlike
> the two local-IPC transports from PR #81 it **is** subject to `MeshHub`'s per-remote-endpoint
> connection cap. Both points were confirmed against the actual source, not assumed from the PR's own
> framing, and the latter is corrected explicitly in [known-issues.md](for-clanker/known-issues.md) KI-38
> so that entry does not read as though it now covers three transports instead of two. Full write-up in
> [transport.md](for-clanker/transport.md#quictransport--transportquicquictransportcs33).
>
> **The listener's negotiation pump is the one genuinely novel design problem this branch solves**, and
> is worth understanding before touching either it or a future transport with the same shape. QUIC's
> `AcceptConnectionAsync` completes the *entire* TLS 1.3 handshake internally before ever handing back a
> connection — unlike TCP/WebSocket, there is no cheap "has this peer sent anything yet" pre-check left
> to gate a negotiation slot on, so a connection that will never open its first stream is
> indistinguishable, cheaply, from one that eventually will. The **shipped** design (verified against the
> final commit, not an intermediate one) is two caps layered in front of each other: a global semaphore
> (`maxConcurrentNegotiations`, default 64) bounding how many connections may wait for their first stream
> at once, and — checked first, ahead of it — a per-source cap
> (`maxConcurrentNegotiationsPerSource`, default one eighth of the global figure) keyed on source address
> with the identical IPv6 `/64` masking `MeshHub`'s own per-remote-endpoint cap uses, duplicated locally
> rather than shared across the transport/hub assembly boundary. A connection failing either check is
> shed **off the accept loop**, not awaited inline, because disposing an established QUIC connection is
> measurably expensive. This is the *end state* of a two-stage hardening history (an earlier
> single-global-semaphore design, then a two-intermediate-semaphore design mirroring TCP's pump that a
> loopback test caught head-of-line-queueing on, before landing on the per-source-cap shape above) — the
> intermediate designs are gone from the shipped code and are not documented as current anywhere; only
> the final combination is. The per-source cap **mitigates, not eliminates**, a flood spread across many
> distinct sources — recorded as [known-issues.md](for-clanker/known-issues.md) **KI-40** (new, open, by
> design). Full write-up in
> [transport.md](for-clanker/transport.md#the-negotiation-pump--two-tier-admission-read-this-before-touching-it).
>
> **A genuine, QUIC-specific behavioural quirk affects anyone driving this transport directly, though
> never real Meshworx usage:** a QUIC stream is invisible to the receiving end until data actually
> arrives on it — opening one is a purely local operation — so `QuicTransportListener.AcceptAsync` cannot
> complete until the connecting client has called `SendAsync` at least once. Verified against
> `MeshClient.cs` directly that this is a non-issue for the real flow, since `MeshClient.ConnectAsync`
> sends the registration frame immediately once handed a transport. Every test in
> `Transport/Quic/QuicTransportLoopbackTests.cs`/`QuicMeshIntegrationTests.cs` sends before accepting —
> the reverse of every other transport's test shape — documented in
> [testing.md](for-clanker/testing.md).
>
> **A genuine bug was found and is *not* fixed by this pass (docs-only): `QuicTransportListener.StartAsync`
> is not safe under concurrent invocation**, unlike every other listener in this codebase. Because
> `QuicListener.ListenAsync` is itself the asynchronous bind step — there is no synchronous constructor
> phase to serialise under the lock the way every socket-backed listener's bind can be — the type
> correctly guards a `StartAsync`-vs-`DisposeAsync` race (tested, `QuicTransportListenerTests.cs:286`)
> and originally did **not** guard a `StartAsync`-vs-`StartAsync` race: two overlapping calls could both
> pass the "already running" check, both genuinely bind a `QuicListener`, and the second to publish its
> state would silently overwrite the first's, leaking a bound listener and an orphaned background task
> rather than throwing `InvalidOperationException` the way a properly serialised second call does. **Fixed
> in the same PR, immediately after this pass found it** (commit `d4de3b3`): a `_starting` flag now
> serialises concurrent `StartAsync` calls, mirroring `MeshHub.StartAsync`'s identical pattern, and
> `StartAsync_CalledConcurrently_OnlyOneSucceeds` covers the race directly. Recorded as
> **[known-issues.md](for-clanker/known-issues.md) KI-41 (fixed)**.
>
> **A separate, out-of-scope finding, noted here rather than acted on:** `main`'s tip has advanced since
> the previous documentation pass. That pass reconciled against an *open* PR #81 (Unix-socket/named-pipe
> transport) at `0ea35a8`; `main` now sits at `84fd51f`, which **is** PR #81 merged (confirmed: `git log
> --oneline main -3` shows `84fd51f feat: Unix domain socket / named-pipe transport for local IPC` as
> `main`'s own tip, and it is the exact commit this QUIC branch is built on). **Every "PR #81, open, not
> yet merged to `main`" callout elsewhere in this documentation set — in
> [transport.md](for-clanker/transport.md), [known-issues.md](for-clanker/known-issues.md) (KI-38, KI-39)
> and [testing.md](for-clanker/testing.md) — is therefore now stale**, and none of them were corrected in
> this pass: the task was explicitly scoped to reconciling the QUIC branch's own diff, and a full
> "PR #81 has merged" sweep would mean re-verifying every coordinate that pass wrote against `main`
> directly, which is a materially larger job than this one. **A future pass should do exactly that sweep**
> before trusting this document's PR #81 framing at face value; until then, treat every "not yet merged"
> claim about the Unix-socket/named-pipe transport as describing history rather than the present, while
> its actual technical content (the coordinates, the behaviour) is not known to be wrong — only its
> merge-status framing is.
>
> ---
>
> **Documented tree (prior pass):** branch `feat/issue-20-unix-socket-named-pipe-transport` (open **PR
> #81**, closing issue #20, **not yet merged to `main`, at the time of that pass**), two commits on top of `main` at `dbb6709`
> (merge-base confirmed equal to `main`'s own tip), clean working tree. Diffed with
> `git diff main...feat/issue-20-unix-socket-named-pipe-transport --stat`: 14 files — five new source
> files (`Transport/Framing/StreamFramer.cs`, `Transport/Unix/UnixSocketTransport.cs`,
> `Transport/Unix/UnixSocketTransportListener.cs`, `Transport/NamedPipes/NamedPipeTransport.cs`,
> `Transport/NamedPipes/NamedPipeTransportListener.cs`), one existing source file refactored
> (`Transport/Tcp/TcpTransport.cs` — its length-prefixed framing extracted into the new `StreamFramer`,
> confirmed a **pure, behaviour-preserving refactor**: all 48 pre-existing `Transport/Tcp/*Tests.cs` tests
> pass unmodified against the delegating implementation, verified by building and running them against
> this branch during this pass), seven new test files, and a README addition. **`MeshHub.cs`,
> `MeshClient.cs` and `IMeshClient.cs` are all untouched** — confirmed from the diff stat directly, not
> assumed from the PR's own description — so nothing in [hub.md](for-clanker/hub.md) or
> [client.md](for-clanker/client.md) needed correcting. This is **exactly why** the gap noted below
> (KI-38) is recorded as a known issue rather than fixed: closing it properly requires changing
> `MeshHub.cs`, which issue #20's own accepted design explicitly scoped out ("new transport only;
> hub/client untouched").
>
> This branch adds a **fifth and sixth transport**, `UnixSocketTransport`/`UnixSocketTransportListener`
> (namespace `AdamSalisbury.Meshworx.Transport.Unix`, Linux/macOS) and
> `NamedPipeTransport`/`NamedPipeTransportListener`
> (namespace `AdamSalisbury.Meshworx.Transport.NamedPipes`, Windows-only — throws
> `PlatformNotSupportedException` elsewhere) — both for fast, same-host inter-process communication with
> no open network port, and both framed identically to `TcpTransport` via the new shared
> `Transport.Framing.StreamFramer` helper (extracted from what was previously `TcpTransport`'s own
> private framing code). Neither implements `IBatchSendTransport`'s WebSocket-style narrower win — both
> get `TcpTransport`'s full single-rented-buffer coalescing, since both share its exact framing code.
> **Neither has a TLS option, and neither implements the public `IRemoteEndPointTransport`** — the first
> is deliberate (traffic never leaves the host), the second is the source of KI-38 below. Security
> hardening was added to both listeners during a review pass on this PR, not present in the PR's first
> commit: `UnixSocketTransportListener` now calls `File.SetUnixFileMode` immediately after `Bind` and
> before `Listen` to restrict the socket file to owner read/write only by default (an optional
> `socketFileMode` constructor parameter widens it), and `NamedPipeTransportListener` now creates its
> server stream via `NamedPipeServerStreamAcl.Create` with an explicit `PipeSecurity` restricted to the
> current user by default (an optional `pipeSecurity` constructor parameter overrides it) rather than
> Windows' own broader platform default, which additionally grants read access to the `Everyone` group
> and the anonymous account. Both suppressions of `CA1416` (the Windows-only-API analyser rule) around
> the named-pipe ACL calls were checked against the actual runtime guard during this pass — `StartAsync`
> really does check `OperatingSystem.IsWindows()` before `_acceptCts` is ever assigned, so the
> suppressions' stated justification holds. Full write-up in
> [transport.md](for-clanker/transport.md#unixsockettransport--unixsockettransportlistener--transportunix)
> and
> [transport.md](for-clanker/transport.md#namedpipetransport--namedpipetransportlistener--transportnamedpipes).
> Two new known issues are recorded: **KI-38** (high severity, open, deliberately deferred) — neither new
> transport implements `IRemoteEndPointTransport`, so `maxConnectionsPerRemoteEndpoint` cannot see
> connections over either one, and a single local peer can claim up to the hub's full `MaxClients` budget
> instead of the intended per-source ceiling — and **KI-39** (low severity, open) — the
> `deleteExistingSocketFile` constructor parameter on `UnixSocketTransportListener` doubles up as the
> switch for delete-on-dispose too, correctly documented but untested for the `false` case. Both are in
> [known-issues.md](for-clanker/known-issues.md). [testing.md](for-clanker/testing.md) gained seven new
> rows for the new test files and two new conventions paragraphs — one on the Unix socket transport's
> `MemoryStream`-driven framing tests (mirroring `TcpTransportTests.cs`'s malformed-frame coverage, since
> both transports now share the framing code being tested), one on the named-pipe tests being
> platform-guard-only on this repo's `ubuntu-latest`-only CI.
>
> **An unrelated, pre-existing documentation inconsistency was noticed but not corrected in this pass**
> (out of scope — it predates this branch and this branch does not touch the affected file): the summary
> table's KI-37 row in [known-issues.md](for-clanker/known-issues.md) states the pipelined-first-WebSocket-
> frame path now **has** a dedicated regression test and is **fixed**, while that entry's own body text
> still says the opposite (**no** dedicated test, gap still open). The test the table row names
> (`WebSocketTransportLoopbackTests.ConnectAndAccept_ClientPipelinesFirstFrameAheadOfUpgradeResponse_LeftoverBytesAreNotLost`)
> does genuinely exist on `main` — confirmed by this pass — so the table row is the accurate half and the
> body text is the stale one. A future pass touching `known-issues.md` for its own reasons should
> reconcile KI-37's body to match its table row.
>
> **A note on the PR #74 narrative immediately below: it has since merged to `main`.** `main`'s tip at the
> time this pass began (and the commit this branch is built on) is `dbb6709`, "feat: structured
> message-header envelope while keeping the body opaque" — a single squashed commit carrying the work the
> paragraph below still describes as living on an open branch at `535432a`. That framing is now stale, but
> re-deriving it was out of scope for this pass, which was scoped specifically to the WebSocket-transport
> diff above; the `MeshHub.cs`/`MeshClient.cs`/`IMeshClient.cs` coordinates and the rest of the paragraph
> below were **not** re-verified here and should be treated as carried forward unchanged from the prior
> pass, not re-confirmed against `main`. A future pass reconciling the library surface more generally
> should correct this framing while it re-verifies those coordinates.
>
> **Documented tree (prior pass):** branch `feat/issue-32-header-envelope` (then-open **PR #74**, closing issue #32),
> currently checked out on top of `main` at `3aed070` (clean working tree, two commits ahead: `62de94d`,
> `535432a`). **This branch adds a structured message-header envelope alongside the opaque message body.**
> `Messages/Protocol.cs` raises `MaxSupportedVersion` from `4` to `5` and adds
> `HeaderEnvelopeMinVersion = 5` (`Messages/Protocol.cs:21`); `Messages/MessageType.cs` appends four
> opcodes — `SendMessageWithHeaders` (`0x11`), `DeliverMessageWithHeaders` (`0x12`),
> `GroupMessageWithHeaders` (`0x13`), `DeliverGroupMessageWithHeaders` (`0x14`) — each the existing frame
> with one extra `[headerBlockLength(2, BE)][headerBlock]` pair spliced in before the body. A new public
> `MessageHeaders` (`Messages/MessageHeaders.cs`) is a small, immutable, string-keyed
> `IReadOnlyDictionary<string, string>`; a new internal `HeaderEnvelope` (`Messages/HeaderEnvelope.cs`)
> encodes/decodes the header-block wire format, bounds-checking every internal length and throwing
> `FormatException` on a malformed block rather than letting a span-slice exception escape.
> `IMeshClient`/`MeshClient` gain `SendAsync`/`SendToGroupAsync` overloads taking a `MessageHeaders`; an
> empty one produces a byte-identical frame to the existing overload (no wire overhead), while a
> non-empty one on a connection negotiated below version `5` throws `NotSupportedException`. **This is
> also the first thing to actually branch on `NegotiatedProtocolVersion`** — `MeshHub.ClientConnection`
> now records its own negotiated version at registration, and `RouteMessageWithHeaders`/
> `SendToGroupWithHeaders` (new hub-side methods) read it **per recipient** to forward a header-bearing
> frame unchanged or strip it to the plain equivalent, so a group with mixed negotiated versions gets
> mixed frame shapes for the same send. This resolves [known-issues.md](for-clanker/known-issues.md)
> KI-14 for the header envelope specifically (see its history there). A second commit on this branch
> (`535432a`) then bounded the wire format's own length (`GetEncodedLength` rejects an aggregate encoding
> over 65 535 bytes) and fixed a real, independent bug the header work exposed: an oversized *outbound*
> delivery frame (recipient/group name + header block + body exceeding a transport's cap) threw
> `ArgumentException` inside `SendLoopAsync`, uncaught, which faulted the task `HandleClientAsync`'s
> `finally` awaits and leaked that client's registration slot permanently — fixed by widening the catch
> to also treat `ArgumentException` as a transport fault. See
> [known-issues.md](for-clanker/known-issues.md) KI-33 (new, fixed) and KI-34 (new, open — a related,
> unrelated-to-the-fix foot-gun found while documenting this: `MessageHeaders`'s constructor throws on a
> duplicate key rather than last-wins). Full write-up in
> [protocol.md](for-clanker/protocol.md#message-headers), [hub.md](for-clanker/hub.md#routing-helpers) and
> [client.md](for-clanker/client.md#sending-headers).
>
> **Coordinate shift, this pass.** `MeshHub.cs` grew by 250 net lines (1981 → 2231) across eight separate
> insertion points climbing in discrete steps (+0 below old line 1019, +14, +41, +50, +120, +242, +243,
> +250 by old line 1950), each verified by comparing cited-line content before and after re-pointing, not
> computed from a single blanket offset. `MeshClient.cs` grew by 176 net lines (962 → 1138) across seven
> insertion points (+0, +2, +14, +32, +43, +78, +151, +176), and `IMeshClient.cs` by 51 (+0, +26, +51).
> Every `MeshHub.cs`/`MeshClient.cs`/`IMeshClient.cs` coordinate in [hub.md](for-clanker/hub.md),
> [client.md](for-clanker/client.md), [protocol.md](for-clanker/protocol.md),
> [types.md](for-clanker/types.md), [known-issues.md](for-clanker/known-issues.md) and one in
> [transport.md](for-clanker/transport.md) was re-pointed this pass. `testing.md` was **not** re-pointed —
> this branch added substantially to the test suite (two new files, `Messages/MessageHeadersTests.cs` and
> `Messages/HeaderEnvelopeTests.cs`, plus growth in `MeshClientTests.cs`, `MeshHubTests.cs`,
> `MeshIntegrationTests.cs` and both fixtures) but the doc pass was scoped to the library surface the PR
> actually changes; treat `testing.md`'s line counts and any coordinate inside the four files above as
> unconfirmed against this branch until a future pass opens that file for its own reasons. `README.md` was
> rewritten by this branch itself (wire-protocol section) and needed no doc-side correction beyond what is
> already reflected above.
>
> The protocol-version-negotiation work (PR #73, closing issue #47) has since merged as `3aed070`, which
> is the `main` commit this branch is built on. In summary: `Messages/Protocol.cs` replaced the single
> `Version = 3` equality check with `MinSupportedVersion`/`MaxSupportedVersion` range negotiation (both `4`
> at the time); `RegistrationRequest` gained a second version byte; `MeshHub.TryNegotiateProtocolVersion`
> picks the highest version common to both ranges; `RegistrationComplete` grew to 18 bytes to carry it;
> `MeshClient`/`IMeshClient` gained `NegotiatedProtocolVersion`. At the time **nothing downstream read
> it** — this pass's own PR #74 shift (above) is what first put that property to use. Every `MeshHub.cs`/
> `MeshClient.cs`/`IMeshClient.cs` coordinate PR #73 touched has since been re-pointed again by this pass's
> own shift, above, where affected.
>
> The metrics-instrumentation work (PR #72, closing issue #24) merged as `f6d7fd1`, which PR #73 was built
> on. In summary: `MeshHub` and `MeshClientReconnector` each own a `System.Diagnostics.Metrics.Meter`
> (name `"AdamSalisbury.Meshworx"`, `Diagnostics/MeshworxMeterName.cs`), publishing
> `meshworx.hub.clients.connected`, `meshworx.hub.messages.routed`/`bytes.routed` (tagged `direction`),
> `meshworx.hub.messages.dropped` (tagged `reason`), `meshworx.hub.outbound_queue.depth` (an aggregate
> gauge) and `meshworx.client.reconnects`. No protocol, payload, public constructor or DI-package change
> accompanied it. Full write-up in [hub.md](for-clanker/hub.md#metrics) and
> [client.md](for-clanker/client.md#metrics); KI-32 records the routed/dropped-counter nuance for
> `broadcast`/`group` sends, now extended by this pass to cover the header-bearing routing methods PR #74
> added. Every `MeshHub.cs` and `MeshClientReconnector.cs` coordinate it touched has since been re-pointed
> again by this pass's own shift, above, where affected.
>
> The `IsRunning`/`MaxClients`/`ClaimedClientSlots` and health-check work (PR #71, closing issue #23) — open
> at the time of the previous pass — **has since merged as `e2892c1`**, which is the `main` commit this
> branch is built on. `IMeshHub` gained `IsRunning` (`bool`), `MaxClients` (`int` — the constructor
> parameter was already stored in a private field; it is now also a public getter) and `ClaimedClientSlots`
> (`int` — the pre-existing `_reservedClientSlots` counter, now exposed: it is what admission is actually
> enforced against, not `ConnectedClientCount`), all implemented on `MeshHub`; the existing
> `AdamSalisbury.Meshworx.Extensions.DependencyInjection` package gained a health-check integration against
> `Microsoft.Extensions.Diagnostics.HealthChecks` (`AddMeshHub`/`MeshHubHealthCheck` and
> `AddMeshClient`/`MeshClientHealthCheck` extension methods on `IHealthChecksBuilder`). Full write-up in
> [dependency-injection.md](for-clanker/dependency-injection.md#health-checks); an
> HTTP-endpoint-exposure caveat for consumers who map these checks to a health endpoint is recorded as
> [known-issues.md](for-clanker/known-issues.md) KI-31. **At the time, `MeshHub.cs` shifted** because
> `private readonly int _maxClients` (removed) became the `MaxClients` auto-property (added, with
> `IsRunning` and `ClaimedClientSlots`, 18 net new lines) — a shift since superseded by PR #72's own,
> larger one described above; both are folded into the coordinates now in the document.
>
> The dependency-injection and generic-host integration work (PR #70, closing issue #22) — open at the
> time of the previous pass — **has since merged as `0d0c6ff`**, which is the `main` commit this branch is
> built on. It was purely additive as described below, and confirmed unchanged against the merged commit,
> so nothing in [dependency-injection.md](for-clanker/dependency-injection.md) needed correcting beyond
> updating its own "currently open" references to describe `main` directly.
>
> The unbounded-resource-consumption-defaults fix (PR #68, closing issue #16) merged as `76f9c89`, which is
> the `main` commit PR #70 was built on. In summary: `maxClients` defaults to **1000** (was
> `int.MaxValue`/unlimited); `heartbeatInterval` defaults to **30 s** (was `null`/disabled), with
> `Timeout.InfiniteTimeSpan` as the explicit opt-out sentinel; a new `maxConnectionsPerRemoteEndpoint`
> constructor parameter (default **100**) caps connections from a single remote address, backed by a new
> public `IRemoteEndPointTransport` capability that `TcpTransport` implements. See
> [§5](#5-configuration--environment), [hub.md](for-clanker/hub.md#per-remote-endpoint-connection-cap) and
> [known-issues.md](for-clanker/known-issues.md) KI-29 for the full write-up, including why this was a
> breaking behavioural change for any existing hub that relied on the old defaults.
>
> **Documented tree (PR #67):** branch `docs/reconnected-handler-async-void` (PR #67, closing issue #15),
> which is `main` plus a README **Event handlers** subsection and one guard test in
> `MeshClientReconnectorTests.cs`. **No library code changed on that branch** — no behaviour, no
> signature and no configuration differs from `main` there, so every contract documented below (outside
> the `MeshHub.cs` coordinates re-pointed above) describes `main` exactly as it describes PR #67.
>
> The group-authorisation work (PR #66, closing issue #14) has since merged as `975c10e`, so the
> [Group authorisation](for-clanker/hub.md#group-authorisation) and
> [Routing helpers](for-clanker/hub.md#routing-helpers) sections of [hub.md](for-clanker/hub.md) plus its
> constructor rows, the `0x10 GroupJoinRefused` opcode and the
> [Additive opcodes](for-clanker/protocol.md#additive-opcodes-within-a-version) section of
> [protocol.md](for-clanker/protocol.md), the
> [Authorisation types](for-clanker/types.md#authorisation-types) section of
> [types.md](for-clanker/types.md), the
> [Group membership](for-clanker/client.md#group-membership) section of
> [client.md](for-clanker/client.md), KI-2/KI-4/KI-8/KI-9/KI-10 and KI-27/KI-28 in
> [known-issues.md](for-clanker/known-issues.md), the group-authorisation test rows in
> [testing.md](for-clanker/testing.md), and the group bullets in §4 and §8 below now describe `main`
> directly.
>
> The atomic capacity-admission fix (PR #65, closing issue #13) has since merged as `40e7731`, so the
> [Registration handshake](for-clanker/hub.md#registration-handshake-hub-side) section of
> [hub.md](for-clanker/hub.md) and its `ConnectedClientCount` row, the hub-side validation order in
> [protocol.md](for-clanker/protocol.md#registration-handshake), the `ClientAuthenticator` contract in
> [types.md](for-clanker/types.md#authentication-types), KI-26, the authenticator parking seam in
> [testing.md](for-clanker/testing.md#parking-a-caller-mid-lifecycle) and the admission bullet in §4
> below now describe `main` directly.
>
> The hub lifecycle-concurrency fix (PR #64, closing issue #12) merged as `c90d515`, so the
> [Lifecycle & concurrency](for-clanker/hub.md#lifecycle) section of [hub.md](for-clanker/hub.md), its
> `StartAsync` / `StopAsync` / `DisposeAsync` rows, KI-23/KI-24/KI-25, the hub-lifecycle parking seams in
> [testing.md](for-clanker/testing.md#parking-a-caller-mid-lifecycle) and the lifecycle bullet in §4
> describe `main` too. The listener disposal-contract fix (PR #63, closing issue #11) merged as
> `a8b05d2`, so the `ITransportListener` disposal contract and the `TcpTransportListener` /
> `InMemoryTransportListener` sections of [transport.md](for-clanker/transport.md), the accept-loop note
> in [hub.md](for-clanker/hub.md#the-accept-loop), KI-22 and the two listener test rows in
> [testing.md](for-clanker/testing.md) describe `main` too.
>
> Everything else documented here is on `main`. The disconnect-race fix (PR #62, closing issue #10) has
> since merged as `28672a0`, so the claim protocol in
> [client.md](for-clanker/client.md#the-claim-protocol-load-bearing), the `DisconnectAsync` row in §2
> below, KI-21 and the `MeshClientTests.cs` row in [testing.md](for-clanker/testing.md) now describe
> `main` directly. The heartbeat eviction fix (PR #61, closing issue #9) merged as `a005af2`, so the
> heartbeat schedule in [hub.md](for-clanker/hub.md#heartbeat-schedule), the `maxMissedHeartbeats` row in
> §5 below, KI-11 and every `MeshHub.cs` coordinate describe `main` too. The reconnector race fix
> (PR #60, closing issue #8) is documented in the `MeshClientReconnector` sections of
> [client.md](for-clanker/client.md) and KI-19/KI-20. The TLS transport work (PR #59, closing issue #7)
> is documented in [transport.md](for-clanker/transport.md); the registration-authentication work of
> PR #56 and protocol version 3 are on `main`.
>
> **Known documentation gap — closed for `client.md` and `known-issues.md` by the PR #73 pass, and
> re-verified/re-pointed again by this (PR #74) pass; the rest of the tree remains unverified beyond what
> this pass's own edits touched.** For several passes, `MeshClient.cs` coordinates outside the
> registration and group-membership paths were written against an older tree and had drifted — PR #55
> (client send timeout and retry) landed on `main` after this documentation set was first written and
> went unreconciled through PRs #59–#72, each of which was scoped to its own branch and so only corrected
> the coordinates its own diff moved. **PR #73 forced `client.md` open for its own registration-path
> shift, and rather than reconcile only that narrow slice, that pass re-derived every
> `MeshClient.cs`/`IMeshClient.cs` citation in `client.md` and `known-issues.md` from the current source**
> — the Surface Table in full, the claim protocol, the receive-loop dispatch ladder and termination
> gates, the correlated lookup, and the group-membership section — rather than trust old numbers or
> apply a blanket arithmetic shift. That pass also corrected several citations that were wrong *before*
> PR #73 too (not just shifted): KI-3's clientName-length check, KI-8's group-name-length check, KI-12's
> `_pendingLookup`/`GetClientIdByNameAsync`, and KI-13's `Data`-view sites all cited unrelated lines for
> several passes and now cite the actual checks. The one citation still resolving to the wrong line by
> design is [types.md](for-clanker/types.md)'s `AdamSalisbury.Meshworx.csproj:726` (see the note further
> down) — untouched because `.csproj` files were out of scope for that pass's re-derivation. **This
> pass (PR #74) re-verified and re-pointed every one of those same `MeshClient.cs`/`IMeshClient.cs`
> citations again** for its own coordinate shift (see the top of this document) rather than trusting the
> PR #73 pass's numbers to still be correct after re-deriving the offset — a `MeshClient.cs` mention in
> [protocol.md](for-clanker/protocol.md), [hub.md](for-clanker/hub.md) or [types.md](for-clanker/types.md)
> was touched wherever this pass's own edits required it; `testing.md` was not, and remains unconfirmed
> against this branch's shift.
>
> **The matching gap in `MeshClientReconnector.cs` — open since PR #52 and reported by every pass from
> PR #63 through PR #72 — was fixed by the PR #73 pass and is untouched by this one**, because this
> branch does not modify `MeshClientReconnector.cs` at all. The **Surface** table's `Client`, `StartAsync`,
> `Reconnected` and `DisposeAsync` rows and the **How it works** bullets for `OnDisconnected` and
> `ReconnectLoopAsync` cite the current source directly. See
> [client.md](for-clanker/client.md#meshclientreconnector) for the values.
>
> `MeshClientReconnector.cs`, `MeshHubTests.cs`'s coordinates outside this pass's own edits,
> `TcpTransport.cs`, `ITransportListener.cs`, `TcpTransportListener.cs` and `InMemoryTransportListener.cs`
> are untouched by PR #74 and remain exactly as the PR #73 pass below left them (PR #73, in turn, touched
> only `MeshHub.cs`, `MeshClient.cs` and `IMeshClient.cs` among this set). `MeshHub.cs`/`MeshClient.cs`/
> `IMeshClient.cs` were most recently re-pointed in full for **this pass, PR #74** (issue #32 — see the
> top of this document for the shift map). Before that, they were re-pointed for **PR #73** (issue #47):
> 38 net lines gained in one place (`TryNegotiateProtocolVersion`) plus one more at the
> `RegistrationComplete` payload in `MeshHub.cs`, and `MeshClient.cs`/`IMeshClient.cs` shifted for the
> new version-range fields — both since superseded by this pass's own, larger shift.
> Before PR #73, `MeshHub.cs` and `MeshClientReconnector.cs` were most recently re-pointed in full for
> **PR #72** (issue #24): `MeshHub.cs` gained 152 net lines
> across many separate insertion points and `MeshClientReconnector.cs` gained 31, so every coordinate in
> either file had moved by an amount that climbs in discrete steps depending on position — derived
> from the diff's hunk boundaries and verified by comparing cited-line content before and after, not
> computed from a single offset. `MeshHubTests.cs` is **untouched by PR #72** (no test file besides two new
> ones — `MeshHubMetricsTests.cs`, `MeshClientReconnectorMetricsTests.cs` — and a new `MetricsCapture.cs`
> fixture were added by that branch; see [testing.md](for-clanker/testing.md)), so its coordinates remain
> exactly as the PR #71 pass below left them. Before PR #72, the `MeshHub.cs` and `MeshHubTests.cs` sets
> were most recently re-pointed in full for **PR #71** (issue #23): `MeshHub.cs` lost one field
> (`_maxClients`, folded into the new `MaxClients` auto-property) and gained 18 net new lines (`IsRunning`,
> `MaxClients`, `ClaimedClientSlots`) right after `ConnectedClientCount`, which shifted every coordinate
> between the removed field and that insertion point by **−1** and every coordinate from the insertion
> point onward by **+17** — both since superseded by PR #72's own shift, folded into the coordinates now in
> the document. `MeshHubTests.cs` gained 46 lines of new tests (`IsRunning`/`MaxClients` coverage) inserted
> after its existing `ConnectedClientCount` tests, shifting every coordinate below that point by the same
> amount; coordinates above it (mostly the lifecycle-concurrency tests near the top of the file) are
> unchanged. Both were re-pointed line by line against the source, not by a single computed offset, exactly
> as the PR #68 pass below describes doing for its own, differently-shaped change. Before PR #71, the
> `MeshHub.cs` and `MeshHubTests.cs` sets were most recently re-pointed in full for **PR #68** (issue #16,
> merged as
> `76f9c89`): `MeshHub.cs` gained ~270 lines across new constructor
> defaults, validation, an `internal` testing accessor and the whole per-remote-endpoint cap (new
> constants, a new field, `AcceptLoopAsync`'s cap check, and five new private helpers inserted before
> `HandleClientAsync`), which shifted every coordinate below the new `DefaultMaxClients` constant by
> different amounts depending on position (from +2 near the top of the file to +236 from
> `TryReserveClientSlot` onward) — each was individually verified against the source rather than
> computed from a single offset. `MeshHubTests.cs` gained ~230 lines of new tests inserted mid-file
> (before the "unsupported protocol version" region), shifting everything after that insertion point by
> the same amount. `TcpTransport.cs` was re-pointed for the same PR, which added the
> `IRemoteEndPointTransport` implementation (+1 line from a new `using`, +9 more from the `RemoteEndPoint`
> property and its XML doc, from `IsEncrypted` onward) and **remains untouched by PR #71**. Before PR #68,
> the `MeshHub.cs` set was re-pointed in full for PR #66, which moved everything below its new
> `_groupAuthoriser` field and rewrote the group helpers wholesale, the `MeshHubTests.cs` set likewise
> (PR #66 appended its tests but also added a `using`, shifting every pre-existing coordinate by one), and
> the listener sets were re-pointed in full for PR #63 and remain untouched by PR #68, PR #71 or PR #72.

---

## 1. What Meshworx is

Meshworx is a **.NET class library** (`AdamSalisbury.Meshworx`, target `net10.0`, package version
`0.1.0`) that provides **named message routing through a central hub**. It is not an application or a
service — it is a library you embed. Two test/console apps ship alongside it purely to exercise it.

A second, optional library — `AdamSalisbury.Meshworx.Extensions.DependencyInjection` (PR #70) — wraps the
core types for `Microsoft.Extensions.DependencyInjection` and the generic host: `AddMeshHub` and
`AddMeshClient` register a hub or client and run its start/stop alongside the host's own lifecycle, in
place of the constructor-and-`StartAsync` calls shown directly below. It is a thin composition layer —
nothing in the core library changed to support it. See [dependency-injection.md](for-clanker/dependency-injection.md).

The model in one paragraph: a **hub** (`MeshHub`) listens on a pluggable transport and accepts
**clients** (`MeshClient`). Each client registers under a **unique name** and is assigned a `Guid` id.
Clients then exchange **opaque byte payloads** — addressed directly by recipient id, broadcast to
everyone, or sent to a named **group**. The hub never interprets payloads; it reads a one-byte routing
opcode and forwards the body. Delivery is **best-effort, fire-and-forget** — there are no acks, no
retries, no ordering guarantees beyond a single connection's stream, and no persistence.

Since protocol version 3 the hub has an **authentication seam**: an optional `ClientAuthenticator`
callback decides whether a registering client may join, given its name and an opaque credential it
supplied. **It is opt-in — a hub constructed without one admits any peer that can reach the listener**,
under any unused name.

There is also an **authorisation seam, but it covers groups only**. An optional `GroupAuthoriser`
callback gates every group join, and — with or without it — **sending to a group requires membership of
that group**. Nothing else is gated: an admitted client can still resolve names, broadcast, and
direct-send to any id it holds. Treat the transport boundary as the trust boundary, and see
[known-issues.md](for-clanker/known-issues.md) KI-2.

### Headline facts

| Fact | Value | Source |
|---|---|---|
| Kind | Library, + an optional DI/hosting library, + two console test apps | `src/AdamSalisbury.Meshworx/AdamSalisbury.Meshworx.csproj`, `src/AdamSalisbury.Meshworx.Extensions.DependencyInjection/…csproj` |
| Target framework | `net10.0` | `AdamSalisbury.Meshworx.csproj:4` |
| Language level | C# with `ImplicitUsings` + `Nullable` enabled | `AdamSalisbury.Meshworx.csproj:5-6` |
| Only runtime dependency | `Microsoft.Extensions.Logging` | `AdamSalisbury.Meshworx.csproj:734` |
| Wire protocol version | Negotiated range, currently `4`–`5` (was a fixed `3`, then a `4`–`4` range from PR #73; widened to `5` by PR #74 for the header envelope) | `Messages/Protocol.cs:8`, `:14`, `:21` |
| Max frame payload | 1 MiB (`1024*1024`). `TcpTransport`/`UnixSocketTransport`/`NamedPipeTransport`/`QuicTransport` share **one** constant (`StreamFramer.MaxPayloadSize`, PR #81, extended to `QuicTransport` by PR #82); `WebSocketTransport` keeps its own independent constant of the same value | `Transport/Framing/StreamFramer.cs:28`, `Transport/WebSocket/WebSocketTransport.cs:25` |
| Transport encryption | TCP/WebSocket: optional TLS, **off by default**. `UnixSocketTransport`/`NamedPipeTransport` (PR #81): **no TLS option at all** — local-only, access controlled by filesystem/ACL permissions instead. `QuicTransport` (PR #82): TLS **mandatory** — QUIC requires it at the protocol level, so there is no cleartext mode | `Transport/Tcp/TcpTransport.cs:142`, `TcpTransportListener.cs:110`, `Transport/WebSocket/WebSocketTransportListener.cs:112`, `Transport/Quic/QuicTransport.cs:126-141` |
| Max client-name length | 256 (chars, see gotcha) | `Messages/Protocol.cs:23` |
| Warnings as errors | Yes (`Directory.Build.props`) | `src/Directory.Build.props:3` |

---

## 2. How it is meant to be used

The public surface is small and deliberate. Everything an application touches lives in namespace
`AdamSalisbury.Meshworx` (plus `.Messages` for event args/enums and `.Transport[.Tcp|.InMemory]` for
transports). `MessageType` and `Protocol` are **`internal`** — you cannot see the opcodes from outside
the assembly, and you should not need to.

**Host a hub:**

```csharp
using var loggerFactory = LoggerFactory.Create(b => b.AddConsole());
var listener = new TcpTransportListener(port: 22001);   // binds LOOPBACK only — see transport.md
await using var hub = new MeshHub(loggerFactory.CreateLogger<MeshHub>(), listener);
await hub.StartAsync();
// ... hub now accepts clients; StopAsync() / DisposeAsync() to shut down ...
```

`StopAsync` and `DisposeAsync` are safe to call concurrently and are idempotent — overlapping callers
share one shutdown and each returns only once the hub has actually stopped. Note that `await using`
passes no cancellation token, and the shutdown's client notification has no send timeout (KI-24); a
stopped hub is not restartable (KI-25).

Add authentication by passing a callback — without one the hub admits anyone who can reach it:

```csharp
ClientAuthenticator authenticator = (context, _) =>
    ValueTask.FromResult(CredentialStore.IsValid(context.ClientName, context.Credential.Span));

await using var hub = new MeshHub(logger, listener, authenticator: authenticator);
```

Add **group authorisation** by passing a second callback. This is the other half of the security seam:
the authenticator decides *who a peer is*, the authoriser decides *what it may do*. Without one, any
admitted client may join any group:

```csharp
GroupAuthoriser groupAuthoriser = (context, _) =>
    ValueTask.FromResult(TenantDirectory.MayJoin(context.ClientName, context.GroupName));

await using var hub = new MeshHub(
    logger, listener, authenticator: authenticator, groupAuthoriser: groupAuthoriser);
```

**Sending to a group requires membership of it whether or not you configure an authoriser** — the hub
drops a group message from a client that has not joined. There is no send-only capability.

Add transport encryption by giving the listener TLS options — separate from, and composable with, the
authenticator above. Framing is unchanged, so nothing else in the stack cares:

```csharp
var listener = new TcpTransportListener(
    new IPEndPoint(IPAddress.Any, 22001),
    new SslServerAuthenticationOptions { ServerCertificate = hubCertificate });
```

**Connect a client:**

```csharp
await using var client = new MeshClient(loggerFactory.CreateLogger<MeshClient>());
var transport = await TcpTransport.ConnectAsync("localhost", 22001);   // CLEARTEXT — see transport.md
// ... or, against a TLS listener:
// var transport = await TcpTransport.ConnectAsync("hub.example.com", 22001, new SslClientAuthenticationOptions());
await client.ConnectAsync(transport, clientName: "Alice");   // client TAKES OWNERSHIP of transport
// ... or, against a hub with an authenticator:
// await client.ConnectAsync(transport, "Alice", credential: Encoding.UTF8.GetBytes(apiKey));

client.MessageReceived += (_, e) => Handle(e.SenderId, e.Data.Span);

Guid? bobId = await client.GetClientIdByNameAsync("Bob");
if (bobId is not null)
{
    await client.SendAsync(bobId.Value, Encoding.UTF8.GetBytes("hello"));
}
```

The canonical call sequence is always: **construct → `ConnectAsync` → (send / lookup / group ops +
handle events) → `DisconnectAsync` / dispose**. Runnable examples live in
`src/AdamSalisbury.Meshworx.TestApps/{HubApp,ClientApp}/Program.cs` — read `ClientApp/Program.cs` for a
complete, idiomatic client that uses every capability.

**In a generic host or ASP.NET Core app, `AddMeshHub`/`AddMeshClient` replace this construct-and-start
sequence** with `IServiceCollection` registration plus a hosted service that calls
`StartAsync`/`ConnectAsync` and `StopAsync`/`DisconnectAsync` for you — see
[dependency-injection.md](for-clanker/dependency-injection.md). The two console test apps above still
use the manual sequence directly; they do not go through the DI package.

**Capabilities at a glance** (all on `IMeshClient`, `src/AdamSalisbury.Meshworx/IMeshClient.cs`):

| Task | Call | Notes |
|---|---|---|
| Direct message | `SendAsync(recipientId, payload)` | Dropped silently if recipient unknown |
| Direct message with headers | `SendAsync(recipientId, payload, headers)` | PR #74; `headers` is a `MessageHeaders` — throws `NotSupportedException` unless negotiated at protocol version 5+; throws `ArgumentException` if `headers` contains a reserved request/reply key (PR #83) |
| Request and await a reply | `RequestAsync(recipientId, payload, timeout)` | PR #83; correlated request/response over a direct message, same version requirement as headers above — throws `TimeoutException` on no reply, `InvalidOperationException` if the connection drops first; see [client.md](for-clanker/client.md#request-response) |
| Answer a request | `ReplyAsync(request, payload)` | PR #83; `request` is the `MessageReceivedEventArgs` a `CorrelationId is not null` message arrived on |
| Broadcast | `BroadcastAsync(payload)` | Every other client; never echoed to sender |
| Resolve name → id | `GetClientIdByNameAsync(name)` | `null` if not found; serialised, one in flight |
| Join / leave group | `JoinGroupAsync(name)` / `LeaveGroupAsync(name)` | Groups created on first join, removed when empty. The join is **optimistic** — a hub with a `GroupAuthoriser` may refuse it |
| Group message | `SendToGroupAsync(name, payload)` | Every other member. **The sender must be a member** — the hub silently drops a group message from a non-member |
| Group message with headers | `SendToGroupAsync(name, payload, headers)` | PR #74; same version requirement as the direct overload — see [client.md](for-clanker/client.md#sending-headers). **Cannot** be a request/reply — `RequestAsync` only ever addresses a single `recipientId` |
| Graceful disconnect | `DisconnectAsync()` | Does **not** raise `Disconnected`, even when it races a remote drop — see KI-21 for the one residual window |
| Auto-reconnect | wrap in `MeshClientReconnector` | Re-establishes on drop; you restore app state |
| Present a credential | `ConnectAsync(transport, name, credential)` | Opaque bytes; only meaningful if the hub has an `authenticator` |
| Learn a join was refused | `GroupJoinRefused` event | Group already removed from `JoinedGroups` when it fires; not retried |

---

## 3. Architecture

```mermaid
flowchart LR
    subgraph App["Application code"]
        C1["MeshClient (Alice)"]
        C2["MeshClient (Bob)"]
        R["MeshClientReconnector<br/>(optional wrapper)"]
    end
    subgraph Hub["MeshHub process"]
        L["ITransportListener"]
        H["MeshHub<br/>routing + groups + heartbeat"]
    end
    R -.wraps.-> C1
    C1 -- ITransport --> H
    C2 -- ITransport --> H
    L -- AcceptAsync --> H
    H -- DeliverMessage / DeliverGroupMessage --> C1
    H -- DeliverMessage / DeliverGroupMessage --> C2
```

**Dependency direction (respect this):** `MeshHub` and `MeshClient` depend on the transport
**abstractions** (`ITransport` / `ITransportListener`), never on `Tcp*`, `WebSocket*` or `InMemory*`
concretes. The
concrete transports depend only on the abstractions. Keep it that way — the whole point of the design is
that the transport is swappable. Message opcodes and framing knowledge live in `MeshHub`/`MeshClient`;
transports are dumb pipes that only know framing, not opcodes.

**Data flow for a direct send:** client `SendAsync` frames `[SendMessage][recipientId(16)][body]` and
writes it to its transport → hub receive loop reads it, looks up the recipient's `ClientConnection`,
builds `[DeliverMessage][senderId(16)][body]`, and `TryWrite`s it to the recipient's **bounded outbound
queue** → the recipient connection's **send loop** drains the queue (coalescing bursts) and writes to
its transport → recipient's client receive loop reads it and raises `MessageReceived`.

The **outbound queue + send loop per connection** is the core of the hub's concurrency design: inbound
receive loops never block on a slow recipient's socket; they just enqueue. See
[hub.md](for-clanker/hub.md) for the full model.

### Component map → where to read

| Area | Types | File |
|---|---|---|
| Hub: routing, groups, heartbeat, lifecycle, metrics | `MeshHub`, `IMeshHub` | [hub.md](for-clanker/hub.md#metrics) |
| Client + reconnection + metrics | `MeshClient`, `IMeshClient`, `MeshClientReconnector` | [client.md](for-clanker/client.md#metrics) |
| Transports (incl. TLS) | `ITransport`, `ITransportListener`, `IBatchSendTransport`, `IRemoteEndPointTransport`, `StreamFramer` (PR #81), `TcpTransport(Listener)`, `WebSocketTransport(Listener)` (PR #78), `UnixSocketTransport(Listener)` (PR #81), `NamedPipeTransport(Listener)` (PR #81), `QuicTransport(Listener)` (PR #82, open), `InMemoryTransport(Listener)` | [transport.md](for-clanker/transport.md) |
| Wire protocol & framing | `MessageType`, `Protocol`, handshake, opcode payloads | [protocol.md](for-clanker/protocol.md) |
| Public value types | event args, `MessageHeaders`, `DisconnectReason`, `RegistrationErrorCode`, `ClientAuthenticator`, `RegistrationContext`, `GroupAuthoriser`, `GroupJoinContext`, `RegistrationRefusedException` | [types.md](for-clanker/types.md) |
| DI & generic-host integration | `AddMeshHub`, `AddMeshClient`, `MeshHubOptions`, `MeshClientOptions` | [dependency-injection.md](for-clanker/dependency-injection.md) |
| Tests, fixtures, build/CI | xUnit + Moq suite, `MeshHubFixture`, `MeshClientFixture`, `MetricsCapture` | [testing.md](for-clanker/testing.md) |
| **Known issues register** | consolidated foot-guns and limitations | [known-issues.md](for-clanker/known-issues.md) |

---

## 4. Threading & async model (read before changing any loop)

This is a concurrency-heavy library. The invariants below are load-bearing; breaking them causes
deadlocks or dropped messages that tests may not catch.

- **Async all the way, `ConfigureAwait(false)` everywhere.** This is library code; `CA2007` is a
  **warning = build error** (`.editorconfig` `dotnet_diagnostic.CA2007.severity = warning`,
  `Directory.Build.props` `TreatWarningsAsErrors=true`). Every `await` in the library uses it. Never
  block on async (`.Result` / `.Wait()`).
- **`ITransport` contract:** `SendAsync` must be safe to call **concurrently**; `ReceiveAsync` is
  **single-reader** (never called concurrently). Both hub and client rely on this. `TcpTransport`
  enforces send-concurrency with an internal `SemaphoreSlim` write lock (`TcpTransport.cs:29`), now via
  the shared `StreamFramer` helper (PR #81) — see [transport.md](for-clanker/transport.md#shared-framing-streamframer-internal--transportframingstreamframercs18).
- **`ITransportListener` contract:** a listener disposed with an accept still pending must end that
  accept with **`ObjectDisposedException`**, must throw the same for every later accept, and its
  `DisposeAsync` must be idempotent, safe to call concurrently, and return only once teardown is
  complete (`Transport/ITransportListener.cs:6-22`). The type matters:
  `MeshHub.AcceptLoopAsync` stops on `ObjectDisposedException` but logs-and-retries **without delay**
  on anything else (`MeshHub.cs:691-699`), so a finished listener that reports itself any other way
  spins the hub hot. See [known-issues.md](for-clanker/known-issues.md) KI-22.
- **Hub lifecycle is serialised behind one lock.** `MeshHub.StartAsync`, `StopAsync` and `DisposeAsync`
  may all be called concurrently. Every lifecycle field (`_cts`, `_acceptLoopTask`, `_stopTask`,
  `_disposeTask`, `_starting`, `_disposed`) is guarded by `Lock _stateLock` (`MeshHub.cs:113`), and each
  entry point **captures what it needs once into locals and never awaits while holding the lock**.
  `StopAsync` is deliberately **not `async`** (`MeshHub.cs:454`): it decides synchronously under the
  lock, so overlapping callers provably share one shutdown — clients are notified once, and every caller
  returns only when the hub has actually stopped. Do not re-read a lifecycle field outside the lock and
  do not make `StopAsync` `async` again. See [hub.md](for-clanker/hub.md#lifecycle) and
  [known-issues.md](for-clanker/known-issues.md) KI-23.
- **Client admission is an atomic claim, not a count check.** `maxClients` is enforced against
  `_reservedClientSlots` (`MeshHub.cs:99`), which a registration takes with a single compare-and-swap
  (`TryReserveClientSlot`, `MeshHub.cs:1229`) and gives back in its handler's `finally`
  (`MeshHub.cs:1191-1194`). The claim sits **after** the authenticator so an unauthenticated peer cannot
  hold capacity, with a cheap at-capacity early-out **before** it so a full hub still never runs the
  callback. Consequence for any code you write here: `ConnectedClientCount` can transiently read *below*
  the number of claimed slots, so never gate admission on it. See
  [hub.md](for-clanker/hub.md#registration-handshake-hub-side) and
  [known-issues.md](for-clanker/known-issues.md) KI-26.
- **A second, independent cap bounds connections per remote address, checked in the accept loop before
  any handler exists.** `maxConnectionsPerRemoteEndpoint` guards the pre-registration window
  `maxClients` cannot see — a connection flood that never completes a handshake — via a CAS claim
  (`TryReserveEndpointSlot`, `MeshHub.cs:809`) against a `ConcurrentDictionary<IPAddress, int>`
  (`MeshHub.cs:57`) keyed on the transport's `IRemoteEndPointTransport.RemoteEndPoint` (only checked when
  the transport reports one). Refused connections are disposed immediately, before any registration
  frame is read (`AcceptLoopAsync`, `MeshHub.cs:706-716`). Added by PR #68 (issue #16). See
  [hub.md](for-clanker/hub.md#per-remote-endpoint-connection-cap) and
  [known-issues.md](for-clanker/known-issues.md) KI-29.
- **Group membership is the hub's only enforceable boundary, and the join gate is an awaited callback.**
  A group send is dropped unless the sender is in the group — tested **inside** the group's lock
  (`MeshHub.cs:1983`) so a sender removed concurrently cannot slip through. A join, when a
  `GroupAuthoriser` is configured, awaits that callback **from the calling client's own receive loop**
  (`MeshHub.cs:1046-1047`), which therefore reads nothing else from that client until it returns. Two
  consequences: a slow authoriser stalls only its own client, and a client parked on a decision looks
  idle to the heartbeat monitor and can be evicted mid-decision. Keep integrator awaits out of
  `Group.Lock`. See [hub.md](for-clanker/hub.md#group-authorisation) and
  [known-issues.md](for-clanker/known-issues.md) KI-28.
- **Hub per-connection tasks:** each accepted connection runs a **receive loop** (`HandleClientAsync`),
  a **send loop** (`SendLoopAsync`, drains the bounded outbound `Channel`), and — only when heartbeats
  are configured — a **heartbeat monitor** (`MonitorHeartbeatAsync`, one `PeriodicTimer`). All share one
  linked `CancellationTokenSource` per client. The monitor **checks the miss counter before it probes**,
  so a silent client is evicted on the `maxMissedHeartbeats`th consecutive silent interval and receives
  one fewer ping than that; see [hub.md](for-clanker/hub.md#heartbeat-schedule).
- **Client single receive loop** (`ReceiveLoopAsync`) plus an optional **idle monitor** on a
  `PeriodicTimer`. The client uses an `AsyncLocal<bool> _inReceiveLoop` flag so that calling
  `DisconnectAsync` **from inside a `MessageReceived`/`Disconnected` handler does not deadlock** by
  awaiting its own loop (`MeshClient.cs:17-20`, `:249`). Preserve this if you refactor disconnect.
- **Liveness is detected by an activity counter, not a per-frame timer.** Both sides bump a
  monotonically increasing counter on every received frame; the monitor compares it between timer ticks.
  This avoids arming a `CancellationTokenSource`/timer per frame. Don't reintroduce per-frame timers.
- **Bounded outbound queue (capacity 1024), `TryWrite` delivery.** If a recipient's queue is full,
  the hub **drops the message and logs a warning** — it never blocks the router. This is intentional
  back-pressure-by-dropping. See [known-issues.md](for-clanker/known-issues.md) KI-1.
- **`SendLoopAsync`'s catch is load-bearing for cleanup ordering, not just logging.** It treats
  `IOException`/`ObjectDisposedException`/`ArgumentException` alike, cancelling the client rather than
  letting any of them propagate — an uncaught exception there would fault the task
  `HandleClientAsync`'s `finally` awaits, aborting that `finally` partway through and permanently leaking
  the client's registration slot (fixed for `ArgumentException` — an oversized outbound frame — by
  PR #74; see [known-issues.md](for-clanker/known-issues.md) KI-33). Do not narrow this `when` clause.
- **Event handlers are invoked on the loop's thread inside `try/catch`.** A throwing subscriber is
  logged and swallowed at every callback boundary so it cannot fault a loop. Handlers must be
  thread-safe (hub events fire concurrently for different clients — `IMeshHub.cs:44-46`).
  **That containment reaches only as far as the handler's first suspension.** Every event is a plain
  `EventHandler`/`EventHandler<T>` with no completion for the raiser to await, so an `async void`
  handler returns to the raiser when it first suspends and everything after runs outside the `try` —
  a later throw is rethrown on the thread pool, where nothing observes it. Keep handlers synchronous, or
  start the work from the handler and catch inside the task you start; the README's **Event handlers**
  section carries the idiom, and
  `Reconnected_HandlerIdiomFromDocumentation_ContainsPostSuspensionFailure`
  (`MeshClientReconnectorTests.cs:124`) is the guard on it.

---

## 5. Configuration & environment

There is **no config file, no environment variables, no external services**. Everything is configured
through constructor parameters. The only ambient dependency is an `ILogger<T>` you supply.

**`MeshHub` options** (`MeshHub.cs:178-273`, all optional):

| Param | Default | Effect |
|---|---|---|
| `registrationTimeout` | 10 s | Drop a connection that accepts but never registers |
| `maxClients` | **1000** (was unlimited before PR #68) | Refuse beyond this with `HubAtCapacity`. A **hard** cap — admission is one atomic claim, so concurrent registrations cannot overshoot it. Pass `int.MaxValue` for the old unlimited behaviour |
| `heartbeatInterval` | **30 s** (was `null`/disabled before PR #68) | Ping idle clients; evict on the `maxMissedHeartbeats`th consecutive silent interval. Idle eviction now runs unless you opt out: pass `Timeout.InfiniteTimeSpan` explicitly to disable it — simply omitting the parameter no longer disables it, it takes the 30 s default |
| `maxMissedHeartbeats` | 2 | **Silent intervals until eviction, counted inclusively:** a client that sends nothing is evicted on the Nth silent interval and probed N − 1 times first. At 1 it is never probed at all and the constructor logs a warning. Schedule table in [hub.md](for-clanker/hub.md#heartbeat-schedule) |
| `authenticator` | `null` (**open admission**) | Decides whether each registering client may join; `false` → `AuthenticationFailed` |
| `maxConcurrentAuthentications` | 64 | Caps concurrent authenticator callbacks; ignored when `authenticator` is `null` |
| `groupAuthoriser` | `null` (**any client may join any group**) | Decides whether each registered client may join a group; `false` → `GroupJoinRefused` to that client. Fails closed on throw, self-cancellation or timeout. Group **sends** require membership with or without this |
| `groupAuthorisationTimeout` | 10 s | How long the hub waits for a decision before refusing. Bounds the **wait**, not the callback — see [known-issues.md](for-clanker/known-issues.md) KI-28. Ignored when `groupAuthoriser` is `null`; keep it below `heartbeatInterval × maxMissedHeartbeats` — now always relevant, since `heartbeatInterval` defaults to a real value |
| `maxConnectionsPerRemoteEndpoint` | **100** (new in PR #68) | Caps connections accepted from one remote address at once, checked in the accept loop before any handshake — covers the pre-registration window `maxClients` does not. Only enforced for a transport reporting `IRemoteEndPointTransport.RemoteEndPoint`; an IPv6 address is masked to its `/64` first. Pass `int.MaxValue` to opt out. See [hub.md](for-clanker/hub.md#per-remote-endpoint-connection-cap) |

> **Every `MeshHub` default is now finite (PR #68, issue #16).** A hub constructed with no arguments at
> all — `new MeshHub(logger, listener)` — used to admit an unlimited number of clients and never evict an
> idle one; it now caps at 1000 clients, 100 concurrent connections per remote address, and evicts idle
> clients after 60 s (30 s interval × 2 missed). This is a **behavioural change for any hub already
> relying on the old unlimited/disabled defaults** — pass `int.MaxValue` / `Timeout.InfiniteTimeSpan`
> explicitly to keep the old behaviour. See [known-issues.md](for-clanker/known-issues.md) KI-29.

**`MeshClient` options** (`MeshClient.cs:77`): `idleTimeout` (default `null`), `sendTimeout`
(default `null`), `maxSendAttempts` (default `1` — the first attempt counts, so `1` disables retrying;
only transient I/O errors are retried) and `sendRetryDelay` (default `100 ms`, linear back-off). Set
`idleTimeout` **above** the hub's `heartbeatInterval` so the hub's pings reset it; a genuinely silent
hub then trips it and raises `Disconnected(ConnectionLost)`.

**`MeshClientReconnector` options** (`MeshClientReconnector.cs:86`): `retryDelay` (1 s), `connectTimeout`
(10 s), `restoreGroupMembership`, optional `ILogger`, and `credential` (empty; replayed on every
reconnect — it cannot be changed afterwards, see [known-issues.md](for-clanker/known-issues.md) KI-16).

**`TcpTransportListener` options** (`TcpTransportListener.cs:110`, all optional):

| Param | Default | Effect |
|---|---|---|
| `tlsOptions` | `null` (**cleartext**) | `SslServerAuthenticationOptions` used to authenticate every accepted connection as the server; set `ClientCertificateRequired` for mutual TLS |
| `tlsHandshakeTimeout` | 10 s | Bounds a single negotiation; ignored without `tlsOptions` |
| `maxConcurrentTlsHandshakes` | 64 | Caps concurrent handshakes (CPU bound, **not** an admission limit); 16× that many may be pending. Ignored without `tlsOptions` |

Client-side TLS is configured per connection, not per client: pass `SslClientAuthenticationOptions` to
`TcpTransport.ConnectAsync`. Both option objects are **copied** on the way in (shallow), so reassigning
a property afterwards does not affect a live listener or connection — but mutating a shared object you
handed over still does. Details in [transport.md](for-clanker/transport.md).

**`WebSocketTransportListener` options** (`Transport/WebSocket/WebSocketTransportListener.cs:109`, all
optional, added by PR #78 / issue #18):

| Param | Default | Effect |
|---|---|---|
| `path` | `"/"` | The HTTP request path a client must upgrade on; anything else is refused (see [known-issues.md](for-clanker/known-issues.md) KI-36 for the exact status code the doc comment gets wrong) |
| `tlsOptions` | `null` (**cleartext**, `ws://`) | Same shape as `TcpTransportListener`'s — `SslServerAuthenticationOptions`, copied via the same `CloneServerOptions`; `wss://` when set |
| `handshakeTimeout` | 10 s | Bounds one connection's whole negotiation — the TLS handshake where configured, **plus** the HTTP upgrade parse |
| `maxConcurrentHandshakes` | 64 | Caps concurrent negotiations; unlike its TCP namesake this also bounds plain HTTP header parsing for a cleartext listener, since the pump always runs here — see [known-issues.md](for-clanker/known-issues.md) KI-35. 16× that many may be pending |

Client-side: `WebSocketTransport.ConnectAsync(uri, configureOptions, ct)` takes a `ws://`/`wss://` `Uri`
plus a callback onto `ClientWebSocketOptions` for certificates/validation. All constructors validate
ranges the same way the TCP pair does, throwing `ArgumentOutOfRangeException` for non-positive
timeouts/counts and `ArgumentException` if `tlsOptions` carries no certificate, certificate context, or
certificate-selection callback.

**`UnixSocketTransportListener` options** (`Transport/Unix/UnixSocketTransportListener.cs:59-67`, all
optional, added by PR #81 / issue #20, **not yet merged to `main`**):

| Param | Default | Effect |
|---|---|---|
| `deleteExistingSocketFile` | `true` | Deletes a stale socket file at `path` before binding (recovers from a crashed previous instance), **and** deletes this instance's own socket file on `DisposeAsync` — one flag controls both; see [known-issues.md](for-clanker/known-issues.md) KI-39 |
| `socketFileMode` | Owner read/write only (`UserRead \| UserWrite`) | POSIX mode applied to the socket file immediately after bind, before `Listen()`; ignored on Windows (NTFS ACLs apply there instead). Widen only for a local account you already trust with full mesh access |

No TLS parameter — this transport never leaves the host, so there is nothing to secure at that layer.
Client-side: `UnixSocketTransport.ConnectAsync(path, ct)`. **Does not implement
`IRemoteEndPointTransport`**, so `maxConnectionsPerRemoteEndpoint` (above) cannot see connections over
this transport — see [known-issues.md](for-clanker/known-issues.md) KI-38.

**`NamedPipeTransportListener` options** (`Transport/NamedPipes/NamedPipeTransportListener.cs:53-67`,
all optional, added by PR #81 / issue #20, **not yet merged to `main`**):

| Param | Default | Effect |
|---|---|---|
| `maxServerInstances` | `NamedPipeServerStream.MaxAllowedServerInstances` | The operating system's own cap on simultaneous instances of this pipe — not a Meshworx admission control, and unrelated to `maxClients`/`maxConnectionsPerRemoteEndpoint` |
| `pipeSecurity` | Current user only, full control (`PipeAccessRights.FullControl`) | Windows' own unset-`PipeSecurity` default is considerably broader — it also grants read access to `Everyone` and the anonymous account — so this listener always builds an explicit, narrower one unless you override it |

No TLS parameter, same reasoning as the Unix socket listener. **Windows-only**: both
`NamedPipeTransport.ConnectAsync` and `NamedPipeTransportListener.StartAsync` throw
`PlatformNotSupportedException` on every other operating system, checked before any platform-specific
API runs. Also does **not** implement `IRemoteEndPointTransport` — see
[known-issues.md](for-clanker/known-issues.md) KI-38.

**`QuicTransportListener` options** (`Transport/Quic/QuicTransportListener.cs:129-134`, all but the
first two optional, added by PR #82 / issue #21, **not yet merged to `main`**):

| Param | Default | Effect |
|---|---|---|
| `tlsOptions` | **required** — no default | `SslServerAuthenticationOptions`; QUIC mandates TLS, so unlike `TcpTransportListener`/`WebSocketTransportListener` there is no cleartext mode to fall back to |
| `streamOpenTimeout` | 10 s | How long a connected peer has to open its first stream before the connection is abandoned; bounds how long a negotiation slot can be held |
| `maxConcurrentNegotiations` | 64 | Connections waiting for their first stream at once; a connection beyond this is refused immediately (shed), not queued — there is no cheap pre-check to gate admission on the way TCP/WebSocket's pumps have, so this genuinely bounds how many connect-and-never-send peers can occupy the pool |
| `maxConcurrentNegotiationsPerSource` | one eighth of `maxConcurrentNegotiations` (min 1) | Caps how much of that pool a single source address may hold at once, checked **before** the global cap; IPv6 masked to `/64` first, identically to `maxConnectionsPerRemoteEndpoint`. Mitigates, does not eliminate, a many-source flood — see [known-issues.md](for-clanker/known-issues.md) KI-40 |

Client-side TLS is required, not optional: `QuicTransport.ConnectAsync(host, port,
SslClientAuthenticationOptions, ct)` has no cleartext overload. Both ends default `ApplicationProtocols`
to a shared internal `"meshworx"` ALPN constant if left unset — set it explicitly on both ends if you
override it, so they still agree. **Reports a real `RemoteEndPoint` on both sides** (unlike
`WebSocketTransport`'s client-side gap), so `QuicTransport` **is** subject to
`maxConnectionsPerRemoteEndpoint` above — unlike `UnixSocketTransport`/`NamedPipeTransport`. Details in
[transport.md](for-clanker/transport.md#quictransport--transportquicquictransportcs33).

> **Concurrent `StartAsync` calls on `QuicTransportListener` are now guarded** — see
> [known-issues.md](for-clanker/known-issues.md) KI-41 (fixed). Every other listener above binds
> synchronously under its lock and so never had this problem; `QuicTransportListener`'s bind
> (`QuicListener.ListenAsync`) is itself asynchronous, so it additionally claims a `_starting` flag
> before that await and re-checks it, alongside disposal, once the await completes.

---

## 6. Cross-cutting conventions (imitate these)

Derived from the code, `.editorconfig`, and `Directory.Build.props`. See the root `CLAUDE.md` for the
full house style; the points below are the ones the code actually enforces and demonstrates.

- **Sealed by default.** Every concrete public class is `sealed`. Seal anything new unless it is
  explicitly an extension point.
- **File-scoped namespaces, Allman braces, four-space indent, always-braced blocks** — enforced as
  build warnings (`IDE0011`, `IDE0161`, `IDE0055`).
- **Interfaces carry the XML docs; implementations use `<inheritdoc/>`.** `GenerateDocumentationFile`
  is on and `CS1591` is suppressed, so public members without an obvious inherited doc are fine but the
  contract lives on the interface. Follow this: document new behaviour on the interface.
- **Guard clauses first:** `ArgumentNullException.ThrowIfNull`, `ArgumentException.ThrowIfNullOrEmpty`,
  explicit range checks. Every public entry point does this.
- **Catch specific exceptions.** Broad `catch (Exception)` appears **only** at loop/callback boundaries
  and is always logged with a comment explaining why it is intentional (e.g. `MeshHub.cs:691-699`,
  `:1433`, and the three catches in `AuthenticateAsync` `:1378-1402`). Even a narrowed multi-type `when`
  clause gets the same treatment — `SendLoopAsync`'s `catch (Exception ex) when (ex is IOException or
  ObjectDisposedException or ArgumentException)` (`MeshHub.cs:1491`) has a comment explaining exactly why
  `ArgumentException` belongs there (PR #74, issue #32; see [known-issues.md](for-clanker/known-issues.md)
  KI-33). `CA1031` is a suggestion, not an error, but the convention is strict — match it.
- **No blocking, no `.Result`.** `CA2007` (ConfigureAwait) is a build error in the library.
- **Binary wire work uses `System.Buffers.Binary.BinaryPrimitives`** (big-endian) and
  `Guid.TryWriteBytes` / `new Guid(span)` for the 16-byte ids. Frame buffers on hot paths are rented
  from `ArrayPool<byte>.Shared` in `TcpTransport`; delivery frames are built once and shared read-only
  across recipients in the hub.

**Adding a new message type / capability** (the shape to follow):
1. Add the opcode to `internal enum MessageType` (`Messages/MessageType.cs`) — pick the next free byte.
2. Raise `Protocol.MaxSupportedVersion` (`Messages/Protocol.cs`) if the change is not backward-compatible
   — the hub negotiates the highest version common to its range and the connecting client's, and refuses
   with `UnsupportedProtocolVersion` only if the ranges don't overlap. **A version bump alone does not
   make a change safe**: add a `Protocol.XyzMinVersion` constant marking where your capability becomes
   available, and have both hub and client explicitly check `NegotiatedProtocolVersion` before using it —
   PR #74 (issue #32) is the worked example (`Protocol.HeaderEnvelopeMinVersion`,
   `MeshClient.RequireHeaderEnvelopeSupport`, `MeshHub.ClientConnection.NegotiatedProtocolVersion`); see
   [known-issues.md](for-clanker/known-issues.md) KI-14. A **hub → client** opcode that an older client
   can safely ignore does **not** need a bump — that is the `GroupJoinRefused` precedent, and the exact
   conditions are in [protocol.md](for-clanker/protocol.md#additive-opcodes-within-a-version). A
   client → hub opcode, or any change to an existing frame's layout, always does — this is why PR #74's
   `SendMessageWithHeaders`/`GroupMessageWithHeaders` (client → hub) needed the version bump even though
   `GroupJoinRefused` did not.
3. Client: add the framing/send method to `MeshClient` and the interface method + XML doc to
   `IMeshClient`; add the inbound branch to `ReceiveLoopAsync`.
4. Hub: add the inbound branch to `HandleClientAsync`'s dispatch chain and any routing helper.
5. Update the protocol table in [protocol.md](for-clanker/protocol.md) and the README.
6. Add tests mirroring the existing per-opcode tests; use the fixtures. See [testing.md](for-clanker/testing.md).

**A fourth route needs none of the above: a capability built entirely inside the existing header
envelope.** PR #83's `RequestAsync`/`ReplyAsync` is the worked example — no new opcode, no version bump,
no hub-side change at all. It reuses the existing header-bearing opcodes (`0x11`/`0x12`) and adds two new
well-known `MessageHeaders` keys (`Messages/RequestReplyHeaderKeys.cs`) that only the two `MeshClient`
instances involved interpret; the hub still never decodes header content. This route is only available
when the capability is client-to-client and already fits inside "a direct message with metadata" — it
does not extend to anything the hub itself needs to act on (routing, admission, groups), which still
needs the numbered-route treatment above. See [client.md](for-clanker/client.md#request-response) and
[protocol.md](for-clanker/protocol.md#request-response-headers). If you add reserved header keys of your
own this way, guard them the same way `SendAsync` guards these two
(`ThrowIfReservedHeaderKeyPresent`) — see [known-issues.md](for-clanker/known-issues.md) KI-42/KI-43 for
what happens if you don't.

**"Done" means:** `dotnet build Meshworx.slnx -c Release` clean (warnings are errors) **and**
`dotnet test Meshworx.slnx` green. CI runs exactly this on push/PR to `main`
(`.github/workflows/ci.yml`).

---

## 7. Build, test, run

```sh
# from repo root
dotnet restore Meshworx.slnx
dotnet build   Meshworx.slnx -c Release --no-restore
dotnet test    Meshworx.slnx -c Release --no-build

# run the demo hub, then a client (separate terminals)
dotnet run --project src/AdamSalisbury.Meshworx.TestApps/HubApp
dotnet run --project src/AdamSalisbury.Meshworx.TestApps/ClientApp
```

> **Two solution files exist:** root `Meshworx.slnx` (both libraries + test apps + both test projects —
> this is what CI uses) and `src/Meshworx.slnx`. Use the **root** one. The README's per-project
> `dotnet build/test` commands also work but only cover one project each.

Package metadata (NuGet) is configured in the library `.csproj` (`Version 0.1.0`, MIT, symbols on) but
there is no publish step in CI — CI only builds and tests.

---

## 8. System-wide pitfalls (full detail in known-issues.md)

- **Delivery is lossy by design.** Full outbound queue → dropped frame (logged). Unknown recipient →
  dropped silently. No acks. Do not assume a message sent is a message received.
- **Authentication is opt-in; authorisation covers groups only.** Without a `ClientAuthenticator` any
  reachable peer can register under any unused name and lookup/broadcast to everyone. Groups *can* be
  made a boundary — pass a `GroupAuthoriser` — but without one any admitted client may join any group,
  and nothing outside groups is gated at all. KI-2.
- **Group sends require membership, unconditionally, and this is a silent behavioural break.** The hub
  drops a group message from a non-member with a `Debug` log and no error frame, with or without an
  authoriser, and it shipped **without** a protocol version bump. A client that used to publish to a
  group without joining it still connects and still sends — it is simply never delivered. There is no
  send-only capability: joining to publish also means receiving. KI-2, KI-4.
- **Version negotiation is now actually used, but only for one capability.** The hub picks the highest
  wire-protocol version common to its own range and the connecting client's, so a range mismatch fails
  gracefully instead of the old hard refuse-on-mismatch. PR #74's message-header envelope is the first
  thing to branch on `NegotiatedProtocolVersion` — both hub and client check it explicitly. A *future*
  version bump does not get this for free: whoever adds one must add their own explicit check, the same
  way. KI-14.
- **`SendAsync`/`SendToGroupAsync`'s `MessageHeaders` overload requires protocol version 5+.** A
  non-empty `MessageHeaders` on an older negotiated connection throws `NotSupportedException` rather than
  silently sending without headers. An empty `MessageHeaders` (or the plain overload) costs nothing extra
  on the wire and works at any version. See [client.md](for-clanker/client.md#sending-headers).
- **`MessageHeaders`'s constructor throws on a duplicate key** rather than keeping the last value like a
  plain `Dictionary` initializer would. De-duplicate your source before constructing one. KI-34.
- **Two `MessageHeaders` keys are reserved for `RequestAsync`/`ReplyAsync` (PR #83) and cannot be set
  through `SendAsync`.** `"mesh.request-id"` and `"mesh.reply"` (`RequestReplyHeaderKeys`) now make
  `SendAsync`'s headers overload throw `ArgumentException` if your own headers happen to use either
  string — a narrow but real breaking change for any caller that already did. Request/response itself is
  a pure client-to-client convention: the hub never decodes header content, so it cannot see or protect
  this at all, and any inbound frame carrying `mesh.reply=1` is intercepted before `MessageReceived`
  whether or not it came from a real `RequestAsync` call. See
  [client.md](for-clanker/client.md#request-response), KI-42, KI-43.
- **`new TcpTransportListener(port)` / `new WebSocketTransportListener(port)` both bind loopback**, not
  every interface. Remote clients cannot reach a hub created that way; pass an explicit `IPEndPoint` to
  expose it deliberately.
- **The TCP transport is cleartext unless you pass TLS options** to both the listener and
  `TcpTransport.ConnectAsync`. Nothing warns you; assert `TcpTransport.IsEncrypted` at start-up if it
  matters. Even with TLS, security is **hop-by-hop**: a delivered message's sender id is asserted by the
  hub, not signed by the sender, so a compromised hub can forge one. KI-2, KI-17. The WebSocket transport
  is the same shape (`ws://` cleartext by default, `wss://` opt-in via `tlsOptions`), and reuses the
  identical trust model.
- **`WebSocketTransportListener`'s negotiation pump runs unconditionally, cleartext or not** — the
  opposite of `TcpTransportListener`, whose pump exists only for TLS. `maxConcurrentHandshakes` therefore
  bounds plain HTTP header parsing too, not just TLS handshakes. See
  [transport.md](for-clanker/transport.md#the-negotiation-pump--read-this-before-touching-it) and
  [known-issues.md](for-clanker/known-issues.md) KI-35.
- **A failed TLS handshake is silent on the listener side** — no log, no exception, the hub simply never
  sees the connection. Diagnose from the client. KI-18.
- **`UnixSocketTransport`/`NamedPipeTransport` (PR #81, issue #20) are invisible to
  `maxConnectionsPerRemoteEndpoint`.** Neither implements `IRemoteEndPointTransport`, so a hub reached
  only over a Unix domain socket or a named pipe has no per-source connection cap short of `maxClients`
  itself — a single local peer with access to the path can claim the whole budget. Deliberately deferred,
  not a bug to silently "fix" by changing `MeshHub.cs` — see KI-38 before touching this. **`QuicTransport`
  (PR #82, issue #21, open) is deliberately *not* in this bucket** — it implements
  `IRemoteEndPointTransport` and reports a real address, so it **is** capped the same way TCP/WebSocket
  are.
- **QUIC has no cleartext mode, and its negotiation pool's per-source cap mitigates rather than
  eliminates a many-source flood.** `QuicTransport`/`QuicTransportListener` (PR #82, issue #21, open)
  always require TLS — there is no cleartext overload to reach for the way there is on TCP/WebSocket.
  Because QUIC's handshake completes fully before a connection is ever handed back, there is no cheap
  pre-check to gate a negotiation slot on the way TCP/WebSocket's pumps do; the per-source cap
  (`maxConcurrentNegotiationsPerSource`) bounds how much of the pool one source can hold, not how much a
  flood spread across many distinct sources can hold between them. See
  [known-issues.md](for-clanker/known-issues.md) KI-40, and KI-41 for a separate
  `StartAsync`-concurrency bug found in the listener by this documentation pass and fixed in the same PR.
- **Client-name length is checked in `char`s (UTF-16 units), not UTF-8 bytes**, on both sides — a name
  can encode to more bytes than you expect. Names are also case-sensitive and `Ordinal`-compared.
- **Group membership is fire-and-forget and optimistic on the client.** `JoinGroupAsync` returning means
  the request was *sent*, not that you are a member — subscribe to `GroupJoinRefused` if it matters.
  Reconnects **do** auto-rejoin (since PR #52), by re-joining over the wire, so every restored membership
  is authorised afresh and may be refused. A refusal carries no correlation id, so it can clear a
  membership a later join obtained; the divergence is fail-safe (the client under-reports, the hub keeps
  the member). KI-10, KI-27.
- **`DisconnectAsync` suppresses `Disconnected` even when it races a remote drop — but not absolutely.**
  A claim protocol makes the outcome independent of which side tears the connection down, *except* in a
  few-instruction window after the receive loop has already published the disconnected state, where the
  event still fires with `ConnectionLost`. Treat the suppression as overwhelmingly reliable rather than
  guaranteed, and make `Disconnected` handlers idempotent. KI-21.
- **Hub shutdown is unbounded unless you pass a token.** The `Disconnect` notification is sent to each
  client **sequentially with no send timeout**, so one registered peer that has stopped reading can hold
  `StopAsync(default)` — and therefore `await using` — open indefinitely, leaving the peers behind it
  unnotified. Pass a cancellable token if shutdown latency matters. KI-24.
- **`ConnectedClientCount` is not the capacity gauge.** `maxClients` is enforced against an atomic slot
  claim, not the client registry, so the count can transiently read *below* the number of claimed slots —
  during a registration and during shutdown. The invariant is `reserved >= registered`, so it only errs
  conservative, but never write an admission check against `ConnectedClientCount`. KI-26.
- **A stopped hub is spent.** `StopAsync` releases the hub's state, but `ITransportListener` has no stop:
  the endpoint stays bound and both shipped listeners refuse a second `StartAsync`. Dispose it and build
  a new one rather than restarting. KI-25.
- **`ReadOnlyMemory<byte>` in event args is a view over the received frame.** It is currently backed by
  a per-frame allocation so retaining it is safe today, but the idiom is to copy if you keep it past the
  handler. A custom pooling transport could invalidate that assumption.
- **Malformed/short frames are silently ignored** by both dispatch loops (length-guarded branches with
  no `else`). A bug in your framing will manifest as "nothing happens", not an exception.
- **`AddMeshClient` guards against being called twice with the same name with a keyed marker type,
  because its hosted-service registration otherwise cannot deduplicate itself the way `AddMeshHub`'s
  does.** The hub's hosted-service registration is deduplicated for free by the framework
  (`AddHostedService<T>()`); the client's uses a factory overload that is not, so `AddMeshClient` adds
  its own guard rather than relying on one. Do not remove it. See
  [dependency-injection.md](for-clanker/dependency-injection.md) and
  [known-issues.md](for-clanker/known-issues.md) KI-30.
- **Every `MeshHub` default is now finite (PR #68), which is a behavioural change, not just a value
  change.** A hub built with no arguments used to admit unlimited clients and never evict an idle one;
  it now caps at 1000 clients and 100 connections per remote address, and evicts a silent client after
  60 s by default. Code that relied on the old unlimited/disabled defaults — including tests that open
  many connections from one address, e.g. from `localhost`, without configuring the caps — needs
  `int.MaxValue` / `Timeout.InfiniteTimeSpan` passed explicitly. KI-29.

Full register with severities, locations and workarounds: **[known-issues.md](for-clanker/known-issues.md)**.
