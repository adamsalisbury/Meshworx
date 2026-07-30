# Strongly-typed client contracts — `AdamSalisbury.Meshworx.Contracts` / `.Contracts.Generator`

[← back to index](../for-clanker.md) · related: [client.md](client.md) · [serialization.md](serialization.md) · [known-issues.md](known-issues.md)

A `[MeshContract]` Roslyn source generator (feat #94) that turns an interface into a typed proxy (caller
side) and a typed dispatcher (handler side), both built on `IMeshClient.RequestAsync`/`ReplyAsync` and
the [serialization codec layer](serialization.md). It is a code-generation convenience over the existing
request/response mechanism — it adds no new opcode and no wire-protocol changes of its own.

**This file documents the CURRENT state on `main` (commit `f277e60`).** PR #120
(`fix/typed-contract-defects`) is open and unmerged as of this pass and changes several of the specifics
below — see [Pending: PR #120](#pending-pr-120) at the end, and do not treat its behaviour as current
until it merges.

- `[AttributeUsage(AttributeTargets.Interface)] public sealed class MeshContractAttribute` — `src/AdamSalisbury.Meshworx.Contracts/MeshContractAttribute.cs:36`
- `public static class ContractHeaderKeys` — `src/AdamSalisbury.Meshworx.Contracts/ContractHeaderKeys.cs`
- The generator itself — `src/AdamSalisbury.Meshworx.Contracts.Generator/MeshContractGenerator.cs`

---

## How it works

Apply `[MeshContract]` to an interface (interfaces only — `AttributeTargets.Interface`):

```csharp
[MeshContract]
public interface IOrderService
{
    Task SubmitAsync(int orderId, string productCode, CancellationToken cancellationToken = default);
    Task<int> GetTotalAsync(int orderId, CancellationToken cancellationToken = default);
}
```

The generator emits, per contract:

- One `{MethodName}Arguments` record per method that has parameters.
- **`{BaseName}Proxy : {InterfaceName}`** — a proxy class implementing the interface (`IOrderService` →
  `OrderServiceProxy`; the leading `I` is stripped when followed by an uppercase letter). Constructed
  with an `IMeshClient`, an `IMessageSerializer` and the recipient's `Guid`:

  ```csharp
  var proxy = new OrderServiceProxy(client, JsonMessageSerializer.Default, recipientId);
  await proxy.SubmitAsync(42, "WIDGET");          // one-way — Task-returning
  int total = await proxy.GetTotalAsync(42);      // request/reply — Task<T>-returning
  ```

- **`{BaseName}Dispatcher`** — a plain class (does not implement any interface) that decodes an inbound
  message and invokes the matching method on a real implementation:

  ```csharp
  var dispatcher = new OrderServiceDispatcher(implementation, JsonMessageSerializer.Default);
  client.MessageReceived += async (_, e) =>
      await dispatcher.TryDispatchAsync(e, replyClient: client); // replyClient only needed for Task<T> methods
  ```

## Wire identity — `mesh.contract.method`

`ContractHeaderKeys.Method = "mesh.contract.method"` (`ContractHeaderKeys.cs:25`, public — not one of
the 13 reserved keys `ThrowIfReservedHeaderKeyPresent` guards, since the hub never inspects it). **On
`main` today the value is the bare, unqualified method name** — `MeshContractGenerator.cs:295-297,398`
write and match only `method.Name`. The type's own documented reasoning
(`ContractHeaderKeys.cs:19-23`) only covers uniqueness *within one interface*. See
[Pending: PR #120](#pending-pr-120).

## Diagnostics

Six current diagnostics, all `DiagnosticSeverity.Error` (`ContractDiagnostics.cs`):

| ID | Meaning |
|---|---|
| `MESH001` | A contract method must return `Task`/`Task<T>` |
| `MESH002` | An unsupported parameter modifier (`ref`/`out`/`in`) |
| `MESH003` | A generic contract method is unsupported |
| `MESH004` | Method overloading is unsupported within one contract |
| `MESH005` | A non-method member on a `[MeshContract]` interface is unsupported |
| `MESH006` | `CancellationToken` must be the last parameter |

## Contract & gotchas

- **A `Task<T>`-returning proxy method does not reach a dispatcher end to end today.** `IMeshClient.RequestAsync`
  has **no overload accepting `MessageHeaders` on `main`**, so a result-returning proxy method serializes
  its body and calls `RequestAsync(recipientId, body, timeout, cancellationToken)` directly — the
  `mesh.contract.method` header the proxy built (`MeshContractGenerator.cs:295-297`) is **discarded before
  the call**, never reaches the wire. The dispatcher's `TryGetValue(...Method, ...)` lookup then always
  fails for a real call made through the generated proxy. The shipped test suite
  (`GeneratedContractTests.cs`) does not catch this because it exercises the dispatcher's `Task<T>` path by
  hand-constructing a `MessageReceivedEventArgs` with the header pre-set, never through the real proxy's
  `RequestAsync` call. **A `Task`-returning (one-way) contract method's proxy path does work correctly
  end to end** — it goes through the serialization layer's own `SendAsync<TValue>`, which does carry
  headers. See [known-issues.md](known-issues.md) KI-58.
- **Two different `[MeshContract]` interfaces sharing a method name produce an identical wire value.** A
  `MeshClient` with both contracts' dispatchers wired to `MessageReceived` cannot distinguish them from
  the header alone — see [known-issues.md](known-issues.md) KI-58.
- **The generated dispatcher's `TryDispatchAsync` takes a per-call, optional `replyClient` parameter**
  (`replyClient: IMeshClient? = null`) — if `null` and the invoked method has a result, the reply is
  silently skipped (no exception, no log visible from this layer).
- **Only an interface may carry `[MeshContract]`** — a class, a nested type, or a generic interface is
  rejected or unsupported by the generator (see diagnostics above; PR #120 adds three more covering
  generic/base-interface/nested cases not caught today).

<a id="pending-pr-120"></a>

## Pending: PR #120 (`fix/typed-contract-defects`, open, unmerged)

**None of the following is true of `main` as it stands — do not document it as current until the PR
merges.** Fetched and read via `gh pr diff 120` specifically to keep this file's "current state" section
accurate rather than contaminated with unmerged work:

1. **`IMeshClient.RequestAsync`/`ReplyAsync` gain headers overloads** — `RequestAsync(Guid,
   ReadOnlyMemory<byte>, TimeSpan, MessageHeaders, CancellationToken)` and `ReplyAsync(MessageReceivedEventArgs,
   ReadOnlyMemory<byte>, MessageHeaders, CancellationToken)` — closing the discarded-header defect above.
2. **`mesh.contract.method` becomes fully qualified**: `Namespace.IInterface.Method`, via a new
   `ContractModel.ContractIdentity`, closing the cross-contract collision above.
3. **The generated dispatcher's constructor takes its reply client as a required argument** —
   `{BaseName}Dispatcher(implementation, serializer, IMeshClient replyClient)` — for any contract with at
   least one result-returning method; `TryDispatchAsync` drops its per-call `replyClient` parameter
   entirely. A contract whose methods are all `Task`-returning (one-way) still takes no client.
4. **Both proxy branches (void and result-returning) go through the same typed-serialization extension**,
   so the method header and the content-type header land on every contract message on one path.
5. **Three new diagnostics**: `MESH007` (generic contract interface), `MESH008` (contract interface
   inherits another interface), `MESH009` (contract interface nested inside another type).
6. **`MESH010`** — reported when source generation itself throws for a contract, replacing a previously
   silent failure that suppressed every file the generator would have emitted for the whole compilation.
7. Generator hardening: hint-name collisions between same-named contracts in different namespaces fixed,
   fully-qualified type names and keyword escaping used throughout, cancellation-token parameter names
   preserved rather than normalised, static interface members skipped rather than diagnosed.

Until PR #120 merges, treat items 1–4 above as the reason [known-issues.md](known-issues.md) KI-58 is
open, not fixed.
