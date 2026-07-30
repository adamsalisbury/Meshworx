using Microsoft.CodeAnalysis;

namespace AdamSalisbury.Meshworx.Contracts.Generator;

/// <summary>
/// The diagnostics the contract generator reports for a signature it cannot express.
/// </summary>
/// <remarks>
/// Every one of these is an error rather than a warning, deliberately. A contract member the generator
/// skipped would compile into a proxy that silently does not implement its interface, or a dispatcher
/// that silently drops a message the sender believed it delivered — both far worse to debug than a
/// build failure naming the member and the reason.
/// </remarks>
internal static class ContractDiagnostics
{
    private const string Category = "Meshworx.Contracts";

    internal static readonly DiagnosticDescriptor MustReturnTask = new(
        "MESH001",
        "Contract method must return Task or Task<T>",
        "Contract method '{0}' returns '{1}'; a contract method must return Task or Task<T>",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A contract call crosses the network, so it is asynchronous by nature. A "
            + "synchronous return type cannot represent the wait, and a void one cannot represent "
            + "failure at all.");

    internal static readonly DiagnosticDescriptor UnsupportedParameterModifier = new(
        "MESH002",
        "Contract method parameter cannot be ref, out or in",
        "Parameter '{0}' of contract method '{1}' is declared ref, out or in; contract parameters must "
            + "be passed by value",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A by-reference parameter has no meaning across a network boundary: there is no "
            + "shared memory for the callee to write back into.");

    internal static readonly DiagnosticDescriptor GenericMethodUnsupported = new(
        "MESH003",
        "Contract method cannot be generic",
        "Contract method '{0}' is generic; a contract method must not declare type parameters",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "The generator emits argument packing for concrete types it can see at compile "
            + "time. An open type parameter has no such shape to serialize.");

    internal static readonly DiagnosticDescriptor OverloadUnsupported = new(
        "MESH004",
        "Contract method names must be unique",
        "Contract method '{0}' is overloaded; a contract must not contain two methods with the same name",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A method is identified on the wire by its name alone. Two methods sharing a name "
            + "would share a header value, leaving the dispatcher no way to tell which one the sender "
            + "meant.");

    internal static readonly DiagnosticDescriptor NonMethodMemberUnsupported = new(
        "MESH005",
        "Contract may only declare methods",
        "Contract '{0}' declares {1} '{2}'; a contract may only declare methods",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A property or event models shared state or a callback, neither of which a "
            + "one-way message can carry. Express the intent as a method.");

    internal static readonly DiagnosticDescriptor GenericContractUnsupported = new(
        "MESH007",
        "Contract interface cannot be generic",
        "Contract '{0}' is generic; a contract interface must not declare type parameters",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "The generator emits a proxy and a dispatcher for concrete types it can see at "
            + "compile time, and names the contract on the wire. An open type parameter has neither a "
            + "shape to serialize nor a single identity to name.");

    internal static readonly DiagnosticDescriptor BaseInterfacesUnsupported = new(
        "MESH008",
        "Contract interface cannot inherit other interfaces",
        "Contract '{0}' inherits '{1}'; a contract interface must not declare base interfaces",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Only members declared on the contract itself are proxied and dispatched. An "
            + "inherited member would be part of the interface the proxy claims to implement but absent "
            + "from the generated code, so the contract is required to be self-contained rather than "
            + "silently half-implemented.");

    internal static readonly DiagnosticDescriptor NestedContractUnsupported = new(
        "MESH009",
        "Contract interface cannot be nested",
        "Contract '{0}' is nested inside '{1}'; a contract interface must be declared directly in a "
            + "namespace",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "The generated proxy and dispatcher are emitted as top-level types beside the "
            + "contract's namespace, which a nested contract has no way to name.");

    internal static readonly DiagnosticDescriptor GeneratorFailure = new(
        "MESH010",
        "The contract generator failed",
        "The contract generator failed while emitting contract '{0}': {1}",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "An exception escaping a source generator is reported by the compiler as a single "
            + "warning, and suppresses every file that generator would have emitted for the whole "
            + "compilation — including well-formed contracts unrelated to the failure. Reporting it "
            + "here keeps the failure attributable and keeps it an error.");

    internal static readonly DiagnosticDescriptor CancellationTokenMustBeLast = new(
        "MESH006",
        "CancellationToken must be the last parameter",
        "Contract method '{0}' takes a CancellationToken that is not its last parameter",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A cancellation token is not serialized and travels no further than the calling "
            + "process. Requiring it last keeps it visibly distinct from the arguments that do go on "
            + "the wire.");
}
