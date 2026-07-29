using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace AdamSalisbury.Meshworx.Contracts.Generator;

/// <summary>
/// Turns an interface marked <c>[MeshContract]</c> into a client proxy and a dispatcher.
/// </summary>
/// <remarks>
/// An incremental generator: the pipeline is keyed on the interface's own shape, so a build that
/// changes nothing about a contract re-emits nothing for it.
/// <para>
/// Neither emitted type uses reflection. The generator has every method's signature at compile time,
/// so argument packing is a generated record and method selection is a generated switch — which is
/// what makes a mistyped call a build error rather than a message that silently fails to dispatch.
/// </para>
/// </remarks>
[Generator]
public sealed class MeshContractGenerator : IIncrementalGenerator
{
    private const string AttributeFullName = "AdamSalisbury.Meshworx.Contracts.MeshContractAttribute";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        IncrementalValuesProvider<ContractModel?> contracts = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                AttributeFullName,
                predicate: static (node, _) => node is InterfaceDeclarationSyntax,
                transform: static (ctx, _) => BuildModel(ctx))
            .Where(static model => model is not null);

        context.RegisterSourceOutput(contracts, static (spc, model) => Emit(spc, model!));
    }

    private static ContractModel? BuildModel(GeneratorAttributeSyntaxContext context)
    {
        if (context.TargetSymbol is not INamedTypeSymbol symbol)
        {
            return null;
        }

        var diagnostics = new List<DiagnosticInfo>();
        var methods = new List<ContractMethod>();
        var seenNames = new HashSet<string>(System.StringComparer.Ordinal);

        foreach (ISymbol member in symbol.GetMembers())
        {
            if (member is IPropertySymbol)
            {
                diagnostics.Add(DiagnosticInfo.Create(
                    ContractDiagnostics.NonMethodMemberUnsupported,
                    member,
                    symbol.Name,
                    "property",
                    member.Name));
                continue;
            }

            if (member is IEventSymbol)
            {
                diagnostics.Add(DiagnosticInfo.Create(
                    ContractDiagnostics.NonMethodMemberUnsupported,
                    member,
                    symbol.Name,
                    "event",
                    member.Name));
                continue;
            }

            if (member is not IMethodSymbol method || method.MethodKind != MethodKind.Ordinary)
            {
                continue;
            }

            if (!seenNames.Add(method.Name))
            {
                diagnostics.Add(DiagnosticInfo.Create(
                    ContractDiagnostics.OverloadUnsupported, method, method.Name));
                continue;
            }

            if (method.IsGenericMethod)
            {
                diagnostics.Add(DiagnosticInfo.Create(
                    ContractDiagnostics.GenericMethodUnsupported, method, method.Name));
                continue;
            }

            string returnTypeName = method.ReturnType.ToDisplayString();
            bool isTask = returnTypeName == "System.Threading.Tasks.Task";
            bool isTaskOfT = method.ReturnType is INamedTypeSymbol { IsGenericType: true } named
                && named.ConstructedFrom.ToDisplayString() == "System.Threading.Tasks.Task<TResult>";

            if (!isTask && !isTaskOfT)
            {
                diagnostics.Add(DiagnosticInfo.Create(
                    ContractDiagnostics.MustReturnTask, method, method.Name, returnTypeName));
                continue;
            }

            string? resultType = isTaskOfT
                ? ((INamedTypeSymbol)method.ReturnType).TypeArguments[0].ToDisplayString()
                : null;

            var parameters = new List<ContractParameter>();
            bool hasCancellationToken = false;
            bool parameterError = false;

            for (int i = 0; i < method.Parameters.Length; i++)
            {
                IParameterSymbol parameter = method.Parameters[i];

                if (parameter.RefKind != RefKind.None)
                {
                    diagnostics.Add(DiagnosticInfo.Create(
                        ContractDiagnostics.UnsupportedParameterModifier,
                        parameter,
                        parameter.Name,
                        method.Name));
                    parameterError = true;
                    break;
                }

                if (parameter.Type.ToDisplayString() == "System.Threading.CancellationToken")
                {
                    if (i != method.Parameters.Length - 1)
                    {
                        diagnostics.Add(DiagnosticInfo.Create(
                            ContractDiagnostics.CancellationTokenMustBeLast, method, method.Name));
                        parameterError = true;
                        break;
                    }

                    hasCancellationToken = true;
                    continue;
                }

                parameters.Add(new ContractParameter(
                    parameter.Type.ToDisplayString(), parameter.Name));
            }

            if (parameterError)
            {
                continue;
            }

            methods.Add(new ContractMethod(
                method.Name,
                resultType,
                hasCancellationToken,
                parameters.ToImmutableArray()));
        }

        string @namespace = symbol.ContainingNamespace.IsGlobalNamespace
            ? string.Empty
            : symbol.ContainingNamespace.ToDisplayString();

        // "IOrderService" becomes "OrderService", so the emitted types read as OrderServiceProxy and
        // OrderServiceDispatcher rather than IOrderServiceProxy.
        string baseName = symbol.Name.Length > 1 && symbol.Name[0] == 'I' && char.IsUpper(symbol.Name[1])
            ? symbol.Name.Substring(1)
            : symbol.Name;

        return new ContractModel(
            @namespace,
            symbol.Name,
            baseName,
            symbol.DeclaredAccessibility == Accessibility.Public,
            methods.ToImmutableArray(),
            diagnostics.ToImmutableArray());
    }

    private static void Emit(SourceProductionContext context, ContractModel model)
    {
        foreach (DiagnosticInfo diagnostic in model.Diagnostics)
        {
            context.ReportDiagnostic(diagnostic.ToDiagnostic());
        }

        if (model.Diagnostics.Length > 0)
        {
            // Emitting a proxy that implements only the members the generator understood would produce
            // a second, misleading error about an unimplemented interface member on top of the real
            // one. The diagnostics above already name what is wrong.
            return;
        }

        string source = GenerateSource(model);
        context.AddSource($"{model.BaseName}.Contract.g.cs", SourceText.From(source, Encoding.UTF8));
    }

    private static string GenerateSource(ContractModel model)
    {
        var builder = new StringBuilder();
        string accessibility = model.IsPublic ? "public" : "internal";

        builder.AppendLine("// <auto-generated/>");
        builder.AppendLine("#nullable enable");
        builder.AppendLine();

        if (model.Namespace.Length > 0)
        {
            builder.AppendLine($"namespace {model.Namespace};");
            builder.AppendLine();
        }

        EmitArgumentRecords(builder, model, accessibility);
        EmitProxy(builder, model, accessibility);
        EmitDispatcher(builder, model, accessibility);

        return builder.ToString();
    }

    private static void EmitArgumentRecords(StringBuilder builder, ContractModel model, string accessibility)
    {
        foreach (ContractMethod method in model.Methods)
        {
            if (method.Parameters.Length == 0)
            {
                continue;
            }

            string properties = string.Join(
                ", ",
                method.Parameters.Select(p => $"{p.Type} {ToPascalCase(p.Name)}"));

            builder.AppendLine("/// <summary>");
            builder.AppendLine($"/// The wire shape of <c>{model.InterfaceName}.{method.Name}</c>'s arguments.");
            builder.AppendLine("/// </summary>");
            builder.AppendLine(
                $"{accessibility} sealed record {model.BaseName}{method.Name}Arguments({properties});");
            builder.AppendLine();
        }
    }

    private static void EmitProxy(StringBuilder builder, ContractModel model, string accessibility)
    {
        builder.AppendLine("/// <summary>");
        builder.AppendLine($"/// Sends <see cref=\"{model.InterfaceName}\"/> calls to a fixed recipient.");
        builder.AppendLine("/// </summary>");
        builder.AppendLine($"{accessibility} sealed class {model.BaseName}Proxy : {model.InterfaceName}");
        builder.AppendLine("{");
        builder.AppendLine("    private readonly global::AdamSalisbury.Meshworx.IMeshClient _client;");
        builder.AppendLine(
            "    private readonly global::AdamSalisbury.Meshworx.Serialization.IMessageSerializer _serializer;");
        builder.AppendLine("    private readonly global::System.Guid _recipientId;");
        builder.AppendLine("    private readonly global::System.TimeSpan _requestTimeout;");
        builder.AppendLine();
        builder.AppendLine($"    public {model.BaseName}Proxy(");
        builder.AppendLine("        global::AdamSalisbury.Meshworx.IMeshClient client,");
        builder.AppendLine(
            "        global::AdamSalisbury.Meshworx.Serialization.IMessageSerializer serializer,");
        builder.AppendLine("        global::System.Guid recipientId,");
        builder.AppendLine("        global::System.TimeSpan? requestTimeout = null)");
        builder.AppendLine("    {");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(client);");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(serializer);");
        builder.AppendLine("        _client = client;");
        builder.AppendLine("        _serializer = serializer;");
        builder.AppendLine("        _recipientId = recipientId;");
        builder.AppendLine(
            "        _requestTimeout = requestTimeout ?? global::System.TimeSpan.FromSeconds(30);");
        builder.AppendLine("    }");

        foreach (ContractMethod method in model.Methods)
        {
            builder.AppendLine();
            EmitProxyMethod(builder, model, method);
        }

        builder.AppendLine("}");
        builder.AppendLine();
    }

    private static void EmitProxyMethod(StringBuilder builder, ContractModel model, ContractMethod method)
    {
        string returnType = method.ResultType is null
            ? "global::System.Threading.Tasks.Task"
            : $"global::System.Threading.Tasks.Task<{method.ResultType}>";

        var signature = new List<string>(
            method.Parameters.Select(p => $"{p.Type} {p.Name}"));

        if (method.HasCancellationToken)
        {
            signature.Add("global::System.Threading.CancellationToken cancellationToken = default");
        }

        string cancellation = method.HasCancellationToken ? "cancellationToken" : "default";
        string headers =
            $"new global::AdamSalisbury.Meshworx.Messages.MessageHeaders(new global::System.Collections.Generic.Dictionary<string, string> "
            + $"{{ [global::AdamSalisbury.Meshworx.Contracts.ContractHeaderKeys.Method] = \"{method.Name}\" }})";

        string argumentExpression = method.Parameters.Length == 0
            ? "0"
            : $"new {model.BaseName}{method.Name}Arguments("
                + string.Join(", ", method.Parameters.Select(p => p.Name))
                + ")";

        builder.AppendLine($"    /// <inheritdoc/>");
        builder.AppendLine($"    public async {returnType} {method.Name}({string.Join(", ", signature)})");
        builder.AppendLine("    {");
        builder.AppendLine($"        var __arguments = {argumentExpression};");
        builder.AppendLine($"        var __headers = {headers};");

        if (method.ResultType is null)
        {
            builder.AppendLine(
                "        await global::AdamSalisbury.Meshworx.Serialization.MeshClientSerializationExtensions"
                + ".SendAsync(_client, _recipientId, __arguments, _serializer, __headers, "
                + $"{cancellation}).ConfigureAwait(false);");
        }
        else
        {
            // A method with a result is a request: the reply is correlated by the core library's own
            // request/response helper, so the proxy neither invents a correlation scheme nor needs one.
            builder.AppendLine("        var __body = _serializer.Serialize(__arguments);");
            builder.AppendLine(
                "        var __reply = await _client.RequestAsync(_recipientId, __body, _requestTimeout, "
                + $"{cancellation}).ConfigureAwait(false);");
            builder.AppendLine(
                $"        return _serializer.Deserialize<{method.ResultType}>(__reply.Span)!;");
        }

        builder.AppendLine("    }");
    }

    private static void EmitDispatcher(StringBuilder builder, ContractModel model, string accessibility)
    {
        builder.AppendLine("/// <summary>");
        builder.AppendLine(
            $"/// Decodes an inbound message and invokes the matching <see cref=\"{model.InterfaceName}\"/> method.");
        builder.AppendLine("/// </summary>");
        builder.AppendLine($"{accessibility} sealed class {model.BaseName}Dispatcher");
        builder.AppendLine("{");
        builder.AppendLine($"    private readonly {model.InterfaceName} _implementation;");
        builder.AppendLine(
            "    private readonly global::AdamSalisbury.Meshworx.Serialization.IMessageSerializer _serializer;");
        builder.AppendLine();
        builder.AppendLine($"    public {model.BaseName}Dispatcher(");
        builder.AppendLine($"        {model.InterfaceName} implementation,");
        builder.AppendLine(
            "        global::AdamSalisbury.Meshworx.Serialization.IMessageSerializer serializer)");
        builder.AppendLine("    {");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(implementation);");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(serializer);");
        builder.AppendLine("        _implementation = implementation;");
        builder.AppendLine("        _serializer = serializer;");
        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    /// <summary>");
        builder.AppendLine("    /// Dispatches a received message to the contract method it names.");
        builder.AppendLine("    /// </summary>");
        builder.AppendLine("    /// <returns>");
        builder.AppendLine(
            "    /// <see langword=\"true\"/> if the message named a method of this contract and it was");
        builder.AppendLine(
            "    /// invoked; <see langword=\"false\"/> if the message is not for this contract, leaving the");
        builder.AppendLine("    /// caller free to offer it to another dispatcher.");
        builder.AppendLine("    /// </returns>");
        builder.AppendLine("    public async global::System.Threading.Tasks.Task<bool> TryDispatchAsync(");
        builder.AppendLine("        global::AdamSalisbury.Meshworx.Messages.MessageReceivedEventArgs message,");
        builder.AppendLine("        global::AdamSalisbury.Meshworx.IMeshClient? replyClient = null,");
        builder.AppendLine(
            "        global::System.Threading.CancellationToken cancellationToken = default)");
        builder.AppendLine("    {");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(message);");
        builder.AppendLine();
        builder.AppendLine(
            "        if (!message.Headers.TryGetValue("
            + "global::AdamSalisbury.Meshworx.Contracts.ContractHeaderKeys.Method, out var __method))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine("        switch (__method)");
        builder.AppendLine("        {");

        foreach (ContractMethod method in model.Methods)
        {
            EmitDispatchCase(builder, model, method);
        }

        builder.AppendLine("            default:");
        builder.AppendLine("                return false;");
        builder.AppendLine("        }");
        builder.AppendLine("    }");
        builder.AppendLine("}");
    }

    private static void EmitDispatchCase(StringBuilder builder, ContractModel model, ContractMethod method)
    {
        builder.AppendLine($"            case \"{method.Name}\":");
        builder.AppendLine("            {");

        var arguments = new List<string>();

        if (method.Parameters.Length > 0)
        {
            builder.AppendLine(
                $"                var __arguments = _serializer.Deserialize<{model.BaseName}{method.Name}Arguments>"
                + "(message.Data.Span);");
            builder.AppendLine("                if (__arguments is null)");
            builder.AppendLine("                {");
            builder.AppendLine("                    return false;");
            builder.AppendLine("                }");
            builder.AppendLine();

            arguments.AddRange(method.Parameters.Select(p => $"__arguments.{ToPascalCase(p.Name)}"));
        }

        if (method.HasCancellationToken)
        {
            arguments.Add("cancellationToken");
        }

        string call = $"_implementation.{method.Name}({string.Join(", ", arguments)})";

        if (method.ResultType is null)
        {
            builder.AppendLine($"                await {call}.ConfigureAwait(false);");
        }
        else
        {
            builder.AppendLine($"                var __result = await {call}.ConfigureAwait(false);");
            builder.AppendLine();
            builder.AppendLine("                if (replyClient is not null)");
            builder.AppendLine("                {");
            builder.AppendLine(
                "                    await global::AdamSalisbury.Meshworx.Serialization"
                + ".MeshClientSerializationExtensions.ReplyAsync(replyClient, message, __result, "
                + "_serializer, cancellationToken).ConfigureAwait(false);");
            builder.AppendLine("                }");
        }

        builder.AppendLine();
        builder.AppendLine("                return true;");
        builder.AppendLine("            }");
        builder.AppendLine();
    }

    private static string ToPascalCase(string name)
    {
        return name.Length == 0 || char.IsUpper(name[0])
            ? name
            : char.ToUpperInvariant(name[0]) + name.Substring(1);
    }
}
