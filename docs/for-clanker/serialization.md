# Pluggable serialization codec layer — `AdamSalisbury.Meshworx.Serialization`

[← back to index](../for-clanker.md) · related: [client.md](client.md) · [contracts.md](contracts.md) · [known-issues.md](known-issues.md)

A separate, optional assembly (feat #91) that lets a caller send/receive typed values instead of raw
`ReadOnlyMemory<byte>`, without the core `AdamSalisbury.Meshworx` library taking any dependency on a
serialization format. It is a thin layer of extension methods and one shipped implementation — nothing
in `MeshClient`/`MeshHub` changes to support it, and the hub never knows a message was typed at all.

- `public interface IMessageSerializer` — `src/AdamSalisbury.Meshworx.Serialization/IMessageSerializer.cs:16`
- `public sealed class JsonMessageSerializer : IMessageSerializer` — `JsonMessageSerializer.cs:40`
- `public static class MessageSerializationExtensions` — `MessageSerializationExtensions.cs`
- `public static class MeshClientSerializationExtensions` — `MeshClientSerializationExtensions.cs`
- `public static class SerializationHeaderKeys` — `SerializationHeaderKeys.cs`

---

## `IMessageSerializer` — the contract

```csharp
public interface IMessageSerializer
{
    string ContentType { get; }
    ReadOnlyMemory<byte> Serialize<TValue>(TValue value);
    TValue? Deserialize<TValue>(ReadOnlySpan<byte> data);
}
```

No generic constraints either direction (`IMessageSerializer.cs:16-54`). Implementations are required to
be **thread-safe** — a single instance is shared across a client's concurrent sends and its receive loop —
and to **throw** on malformed input rather than return a partial/default value; callers that want a
no-throw path use `MessageSerializationExtensions.TryDeserialize` (below), not a serializer that swallows
its own errors.

<a id="jsonmessageserializer"></a>

## `JsonMessageSerializer` — the shipped implementation

Backed directly by `System.Text.Json`, no source-generated `JsonSerializerContext` built in by default.

```csharp
var serializer = JsonMessageSerializer.Default;              // shared, no custom options
var custom = new JsonMessageSerializer(new JsonSerializerOptions { WriteIndented = true });
```

- `ContentType => "application/json"` (`JsonMessageSerializer.cs:66`).
- Constructor takes an optional `JsonSerializerOptions?` (`null` → `System.Text.Json`'s own defaults).
- Reflection-based unless the caller supplies `JsonSerializerOptions.TypeInfoResolver` pointing at a
  source-generated `JsonSerializerContext` (AOT/trimming) — see the runtime-type caveat below.

### Interface-/abstract-declared values — PR #136/issue #111

`Serialize<TValue>` (`JsonMessageSerializer.cs:82-90`):

```csharp
public ReadOnlyMemory<byte> Serialize<TValue>(TValue value)
{
    if (value is not null && (typeof(TValue).IsInterface || typeof(TValue).IsAbstract))
    {
        return JsonSerializer.SerializeToUtf8Bytes(value, value.GetType(), _options);
    }

    return JsonSerializer.SerializeToUtf8Bytes(value, _options);
}
```

Before this fix, calling `Serialize<IAnimal>(dog)` wrote only `IAnimal`'s own contract, silently dropping
`Dog`'s concrete members. It now resolves against `value.GetType()` — but **only at the top level**: a
concrete type's own interface- or abstract-typed *property* is still serialized by its declared type
(`System.Text.Json` decides per-property, not per top-level call, and this codec has no custom converter
to widen that). `object`-declared values are unaffected — `System.Text.Json` already resolved `object`
against the runtime type before this fix.

`Deserialize<TValue>` into an interface- or abstract-declared `TValue` throws `NotSupportedException` —
unchanged behaviour (`System.Text.Json`'s own, since it cannot construct an instance of a type it cannot
instantiate); the fix only added the `<exception>` doc entry documenting it truthfully.

**AOT/trimming caveat.** Under a source-generated `JsonSerializerContext` that registers metadata only
for the declared interface/abstract type — not for `value.GetType()` — `Serialize` now throws
`NotSupportedException` rather than silently falling back to the pre-fix lossy behaviour. This is the
correct failure mode (loud rather than silent data loss) but is a new way for a previously-working call
to start throwing once a project adopts AOT/trimming. **Register every concrete type that can appear
behind an interface- or abstract-declared value**, not just the declared type itself. Neither this project
nor `Directory.Build.props` sets `IsAotCompatible`/`IsTrimmable`/`PublishAot` — this contract is enforced
by documentation and tests, not by tooling. See [known-issues.md](known-issues.md) KI-57.

## `MessageSerializationExtensions` — decoding a received message

```csharp
client.MessageReceived += (_, e) =>
{
    if (e.TryDeserialize(serializer, out OrderPlaced? order))
    {
        Handle(order!);
    }
};
```

- `Deserialize<TValue>(this MessageReceivedEventArgs, IMessageSerializer)` and the
  `GroupMessageReceivedEventArgs` overload — throw `InvalidOperationException` if the message's
  `SerializationHeaderKeys.ContentType` header does not match the serializer's own `ContentType`.
- `TryDeserialize<TValue>(..., out TValue? value)` — returns `false` (never throws) both when the
  content type mismatches and when `serializer.Deserialize` itself throws. The catch is deliberately
  broad (`catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)`) —
  a third-party codec can throw any exception type, and this is an application-boundary catch by design.
- **A message with no content-type header at all is accepted by both methods** — treated as "sender
  predates this package, or used a plain byte-oriented send" rather than a mismatch.

## `MeshClientSerializationExtensions` — sending a typed value

Thin `IMeshClient` sugar; every method wraps the byte-oriented method of the same name and adds the
content-type header:

```csharp
await alice.SendAsync(bobId, new OrderPlaced(42, "WIDGET"), serializer);
await alice.SendToGroupAsync("orders", new OrderPlaced(42, "WIDGET"), serializer);
OrderTotal? total = await alice.RequestAsync<OrderQuery, OrderTotal>(bobId, query, serializer, TimeSpan.FromSeconds(5));
await alice.ReplyAsync(request, total, serializer);
```

- `SendAsync<TValue>(recipientId, value, serializer, headers=null, ct=default)`
- `SendToGroupAsync<TValue>(groupName, value, serializer, headers=null, ct=default)`
- `RequestAsync<TRequest, TReply>(recipientId, value, serializer, timeout, ct=default) : Task<TReply?>` —
  deserializes the reply with the **same** serializer the request was sent with; assumes a responder
  replies in the format it was asked in.
- `ReplyAsync<TValue>(request, value, serializer, ct=default)`

All four add `SerializationHeaderKeys.ContentType` via an internal `WithContentType` helper that copies
(never mutates) any caller-supplied `MessageHeaders`, and **preserves an explicit caller-set content type**
rather than overwriting it.

## `SerializationHeaderKeys`

`public const string ContentType = "mesh.content-type";` (`SerializationHeaderKeys.cs:24`) — public,
unlike the internal reserved-key classes in the core library, because it is a peer-to-peer wire agreement
the hub passes through unchanged, not a hub-protected convention. **Not** one of the 13 reserved header
keys `ThrowIfReservedHeaderKeyPresent` guards — an application may set it directly if it wants to bypass
the extension methods' own logic. See [known-issues.md](known-issues.md) KI-42.

## Wiring model

Caller-constructed, not DI-registered — there is no `AddMeshSerializer`-style extension anywhere in
`AdamSalisbury.Meshworx.Extensions.DependencyInjection`. Use `JsonMessageSerializer.Default`, construct
`new JsonMessageSerializer(options)` yourself, or implement `IMessageSerializer` for a different format
(a hand-rolled codec is exercised end-to-end by the same extension methods — nothing here is
`JsonMessageSerializer`-specific) and pass the instance explicitly into each call, or wire it into your
own DI container.

## Contract & gotchas

- **Thread-safety is the implementation's responsibility**, not enforced by the interface — a custom
  `IMessageSerializer` that is not safe for concurrent use will misbehave under real traffic.
- **`Deserialize`/`TryDeserialize` never partially populate a value.** A malformed body either throws
  (`Deserialize`) or the whole call reports failure (`TryDeserialize`) — there is no partial result.
- See [known-issues.md](known-issues.md) KI-57 for the runtime-type/AOT caveat in full.
