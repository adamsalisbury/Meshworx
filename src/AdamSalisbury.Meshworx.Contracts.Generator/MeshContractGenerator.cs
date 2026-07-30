using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
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
/// <para>
/// Every identifier the emitted code derives from a user's own names is escaped, and every type name is
/// fully qualified, because the generated file is compiled in the contract author's namespace: a type
/// of theirs that shadows a segment of an emitted name, or a member named after a keyword, must not be
/// able to break a file they cannot edit.
/// </para>
/// </remarks>
[Generator]
public sealed class MeshContractGenerator : IIncrementalGenerator
{
    private const string AttributeFullName = "AdamSalisbury.Meshworx.Contracts.MeshContractAttribute";

    /// <summary>
    /// The one format every symbol-derived type name is emitted through.
    /// </summary>
    /// <remarks>
    /// <see cref="SymbolDisplayFormat.FullyQualifiedFormat"/> already carries the <c>global::</c> alias,
    /// special type names and keyword escaping. The nullable modifier is added because the generated
    /// file is <c>#nullable enable</c>: dropping the <c>?</c> from a <c>string?</c> parameter is a
    /// nullability mismatch against the interface being implemented, not a cosmetic difference.
    /// </remarks>
    private static readonly SymbolDisplayFormat QualifiedFormat =
        SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
            SymbolDisplayMiscellaneousOptions.UseSpecialTypes
            | SymbolDisplayMiscellaneousOptions.EscapeKeywordIdentifiers
            | SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

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

        string @namespace = symbol.ContainingNamespace.IsGlobalNamespace
            ? string.Empty
            : symbol.ContainingNamespace.ToDisplayString();

        var diagnostics = new List<DiagnosticInfo>();

        // Shapes the generator cannot express at all. Reported before the members are looked at, and
        // returned on their own: a generic contract's members would each produce a second diagnostic
        // about the same underlying problem, burying the one that names it.
        if (symbol.IsGenericType)
        {
            diagnostics.Add(DiagnosticInfo.Create(
                ContractDiagnostics.GenericContractUnsupported, symbol, symbol.Name));
        }

        if (symbol.Interfaces.Length > 0)
        {
            diagnostics.Add(DiagnosticInfo.Create(
                ContractDiagnostics.BaseInterfacesUnsupported,
                symbol,
                symbol.Name,
                string.Join(", ", symbol.Interfaces.Select(i => i.Name))));
        }

        if (symbol.ContainingType is not null)
        {
            diagnostics.Add(DiagnosticInfo.Create(
                ContractDiagnostics.NestedContractUnsupported,
                symbol,
                symbol.Name,
                symbol.ContainingType.Name));
        }

        if (diagnostics.Count > 0)
        {
            return BuildModelShell(symbol, @namespace, diagnostics);
        }

        var methods = new List<ContractMethod>();
        var seenNames = new HashSet<string>(StringComparer.Ordinal);

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

            // A static interface member belongs to the interface, not to the conversation between two
            // endpoints: there is no instance to invoke it on at the receiving end.
            if (method.IsStatic)
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
                ? ((INamedTypeSymbol)method.ReturnType).TypeArguments[0].ToDisplayString(QualifiedFormat)
                : null;

            var parameters = new List<ContractParameter>();
            string? cancellationTokenParameterName = null;
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

                    cancellationTokenParameterName = parameter.Name;
                    continue;
                }

                parameters.Add(new ContractParameter(
                    parameter.Type.ToDisplayString(QualifiedFormat), parameter.Name));
            }

            if (parameterError)
            {
                continue;
            }

            methods.Add(new ContractMethod(
                method.Name,
                resultType,
                cancellationTokenParameterName,
                parameters.ToImmutableArray()));
        }

        return BuildModelShell(symbol, @namespace, diagnostics, methods);
    }

    private static ContractModel BuildModelShell(
        INamedTypeSymbol symbol,
        string @namespace,
        List<DiagnosticInfo> diagnostics,
        List<ContractMethod>? methods = null)
    {
        // "IOrderService" becomes "OrderService", so the emitted types read as OrderServiceProxy and
        // OrderServiceDispatcher rather than IOrderServiceProxy.
        string baseName = symbol.Name.Length > 1 && symbol.Name[0] == 'I' && char.IsUpper(symbol.Name[1])
            ? symbol.Name.Substring(1)
            : symbol.Name;

        // What a message says it is calling: the contract's own namespace-qualified name and the method,
        // so a second contract that happens to declare a method of the same name cannot claim it.
        string contractIdentity = @namespace.Length == 0
            ? symbol.Name
            : @namespace + "." + symbol.Name;

        // The metadata name carries generic arity, and the namespace distinguishes two contracts sharing
        // a simple name. A duplicate hint name throws inside AddSource, which the compiler reports as a
        // single warning while emitting nothing at all for the entire generator run.
        string hintBase = @namespace.Length == 0
            ? symbol.MetadataName
            : @namespace + "." + symbol.MetadataName;

        return new ContractModel(
            @namespace,
            symbol.Name,
            symbol.ToDisplayString(QualifiedFormat),
            contractIdentity,
            SanitizeHintName(hintBase) + ".Contract.g.cs",
            baseName,
            symbol.DeclaredAccessibility == Accessibility.Public,
            methods?.ToImmutableArray() ?? ImmutableArray<ContractMethod>.Empty,
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

        try
        {
            string source = GenerateSource(model);
            context.AddSource(model.HintName, SourceText.From(source, Encoding.UTF8));
        }
        catch (Exception ex)
        {
            // The boundary of the generator, and the one place a broad catch is warranted here: an
            // exception escaping into the compiler is reported as a single CS8785 warning and suppresses
            // every file this generator would have emitted for the whole compilation, including
            // well-formed contracts that have nothing to do with the failure. Reporting it keeps the
            // failure attributable to the contract that caused it, and keeps it an error.
            context.ReportDiagnostic(Diagnostic.Create(
                ContractDiagnostics.GeneratorFailure, Location.None, model.ContractIdentity, ex.Message));
        }
    }

    private static string GenerateSource(ContractModel model)
    {
        var builder = new StringBuilder();
        string accessibility = model.IsPublic ? "public" : "internal";
        string prefix = ComputeLocalPrefix(model);

        builder.AppendLine("// <auto-generated/>");
        builder.AppendLine("#nullable enable");
        builder.AppendLine();

        if (model.Namespace.Length > 0)
        {
            builder.AppendLine($"namespace {model.Namespace};");
            builder.AppendLine();
        }

        EmitArgumentRecords(builder, model, accessibility);
        EmitProxy(builder, model, accessibility, prefix);
        EmitDispatcher(builder, model, accessibility, prefix);

        return builder.ToString();
    }

    /// <summary>
    /// The prefix every generated local takes, chosen so that no parameter of any method on the contract
    /// can shadow one.
    /// </summary>
    /// <remarks>
    /// A contract is free to declare a parameter called <c>__arguments</c>. Without this, the generated
    /// local of that name would collide with it — reported as CS0136 at a coordinate inside a file the
    /// contract's author never wrote.
    /// </remarks>
    private static string ComputeLocalPrefix(ContractModel model)
    {
        var names = new List<string>();

        foreach (ContractMethod method in model.Methods)
        {
            names.AddRange(method.Parameters.Select(p => p.Name));

            if (method.CancellationTokenParameterName is { } tokenName)
            {
                names.Add(tokenName);
            }
        }

        string prefix = "__";

        while (names.Any(name => name.StartsWith(prefix, StringComparison.Ordinal)))
        {
            prefix += "_";
        }

        return prefix;
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
                method.Parameters.Select(p => $"{p.Type} {EscapeIdentifier(ToPascalCase(p.Name))}"));

            builder.AppendLine("/// <summary>");
            builder.AppendLine(
                $"/// The wire shape of <c>{model.ContractIdentity}.{method.Name}</c>'s arguments.");
            builder.AppendLine("/// </summary>");
            builder.AppendLine(
                $"{accessibility} sealed record {ArgumentsTypeName(model, method)}({properties});");
            builder.AppendLine();
        }
    }

    private static void EmitProxy(
        StringBuilder builder, ContractModel model, string accessibility, string prefix)
    {
        builder.AppendLine("/// <summary>");
        builder.AppendLine($"/// Sends <c>{model.ContractIdentity}</c> calls to a fixed recipient.");
        builder.AppendLine("/// </summary>");
        builder.AppendLine(
            $"{accessibility} sealed class {model.BaseName}Proxy : {model.InterfaceDisplayName}");
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
            EmitProxyMethod(builder, model, method, prefix);
        }

        builder.AppendLine("}");
        builder.AppendLine();
    }

    private static void EmitProxyMethod(
        StringBuilder builder, ContractModel model, ContractMethod method, string prefix)
    {
        string returnType = method.ResultType is null
            ? "global::System.Threading.Tasks.Task"
            : $"global::System.Threading.Tasks.Task<{method.ResultType}>";

        var signature = new List<string>(
            method.Parameters.Select(p => $"{p.Type} {EscapeIdentifier(p.Name)}"));

        string cancellation = "default";

        if (method.CancellationTokenParameterName is { } tokenName)
        {
            // The token's own declared name, so a contract free to call it 'ct' — or to declare an
            // ordinary serialized parameter named 'cancellationToken' — still emits a valid signature.
            signature.Add(
                $"global::System.Threading.CancellationToken {EscapeIdentifier(tokenName)} = default");
            cancellation = EscapeIdentifier(tokenName);
        }

        string argumentExpression = method.Parameters.Length == 0
            ? "0"
            : $"new {ArgumentsTypeReference(model, method)}("
                + string.Join(", ", method.Parameters.Select(p => EscapeIdentifier(p.Name)))
                + ")";

        string headers =
            "new global::AdamSalisbury.Meshworx.Messages.MessageHeaders(new global::System.Collections.Generic.Dictionary<string, string> "
            + $"{{ [global::AdamSalisbury.Meshworx.Contracts.ContractHeaderKeys.Method] = \"{model.ContractIdentity}.{method.Name}\" }})";

        builder.AppendLine("    /// <inheritdoc/>");
        builder.AppendLine(
            $"    public async {returnType} {EscapeIdentifier(method.Name)}({string.Join(", ", signature)})");
        builder.AppendLine("    {");
        builder.AppendLine($"        var {prefix}arguments = {argumentExpression};");
        builder.AppendLine($"        var {prefix}headers = {headers};");

        if (method.ResultType is null)
        {
            builder.AppendLine(
                "        await global::AdamSalisbury.Meshworx.Serialization.MeshClientSerializationExtensions"
                + $".SendAsync(_client, _recipientId, {prefix}arguments, _serializer, {prefix}headers, "
                + $"{cancellation}).ConfigureAwait(false);");
        }
        else
        {
            // A method with a result is a request: the reply is correlated by the core library's own
            // request/response helper, so the proxy neither invents a correlation scheme nor needs one.
            // Both branches go through the typed extension so that every contract message carries the
            // method header and the codec's content type, one-way and request alike.
            string argumentsType = method.Parameters.Length == 0
                ? "int"
                : ArgumentsTypeReference(model, method);

            builder.AppendLine(
                "        var " + prefix + "reply = await global::AdamSalisbury.Meshworx.Serialization"
                + $".MeshClientSerializationExtensions.RequestAsync<{argumentsType}, {method.ResultType}>(");
            builder.AppendLine(
                $"            _client, _recipientId, {prefix}arguments, _serializer, _requestTimeout, "
                + $"{prefix}headers, {cancellation}).ConfigureAwait(false);");
            builder.AppendLine($"        return {prefix}reply!;");
        }

        builder.AppendLine("    }");
    }

    private static void EmitDispatcher(
        StringBuilder builder, ContractModel model, string accessibility, string prefix)
    {
        bool needsReplyClient = model.Methods.Any(m => m.ResultType is not null);

        builder.AppendLine("/// <summary>");
        builder.AppendLine(
            $"/// Decodes an inbound message and invokes the matching <c>{model.ContractIdentity}</c> method.");
        builder.AppendLine("/// </summary>");
        builder.AppendLine($"{accessibility} sealed class {model.BaseName}Dispatcher");
        builder.AppendLine("{");
        builder.AppendLine($"    private readonly {model.InterfaceDisplayName} _implementation;");
        builder.AppendLine(
            "    private readonly global::AdamSalisbury.Meshworx.Serialization.IMessageSerializer _serializer;");

        if (needsReplyClient)
        {
            builder.AppendLine("    private readonly global::AdamSalisbury.Meshworx.IMeshClient _replyClient;");
        }

        builder.AppendLine();

        if (needsReplyClient)
        {
            builder.AppendLine("    /// <summary>");
            builder.AppendLine(
                $"    /// Creates a dispatcher for <c>{model.ContractIdentity}</c>.");
            builder.AppendLine("    /// </summary>");
            builder.AppendLine("    /// <remarks>");
            builder.AppendLine(
                "    /// The reply client is required because this contract declares at least one method");
            builder.AppendLine(
                "    /// returning a value, and a request whose handler runs but whose reply is never sent");
            builder.AppendLine(
                "    /// leaves the caller waiting out its whole timeout with nothing to indicate why.");
            builder.AppendLine("    /// </remarks>");
        }

        builder.AppendLine($"    public {model.BaseName}Dispatcher(");
        builder.AppendLine($"        {model.InterfaceDisplayName} implementation,");
        builder.Append(
            "        global::AdamSalisbury.Meshworx.Serialization.IMessageSerializer serializer");

        if (needsReplyClient)
        {
            builder.AppendLine(",");
            builder.AppendLine("        global::AdamSalisbury.Meshworx.IMeshClient replyClient)");
        }
        else
        {
            builder.AppendLine(")");
        }

        builder.AppendLine("    {");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(implementation);");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(serializer);");

        if (needsReplyClient)
        {
            builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(replyClient);");
        }

        builder.AppendLine("        _implementation = implementation;");
        builder.AppendLine("        _serializer = serializer;");

        if (needsReplyClient)
        {
            builder.AppendLine("        _replyClient = replyClient;");
        }

        builder.AppendLine("    }");
        builder.AppendLine();
        builder.AppendLine("    /// <summary>");
        builder.AppendLine("    /// Dispatches a received message to the contract method it names.");
        builder.AppendLine("    /// </summary>");
        builder.AppendLine("    /// <remarks>");
        builder.AppendLine(
            "    /// Intended to be called from a <c>MessageReceived</c> handler, so it is total over its");
        builder.AppendLine(
            "    /// input: a body this contract's codec cannot decode, or a request that cannot be replied");
        builder.AppendLine(
            "    /// to, is declined rather than thrown into the receive loop.");
        builder.AppendLine("    /// </remarks>");
        builder.AppendLine("    /// <returns>");
        builder.AppendLine(
            "    /// <see langword=\"true\"/> if the message named a method of this contract and it was");
        builder.AppendLine(
            "    /// invoked; <see langword=\"false\"/> if the message is not for this contract, leaving the");
        builder.AppendLine("    /// caller free to offer it to another dispatcher.");
        builder.AppendLine("    /// </returns>");
        builder.AppendLine("    public async global::System.Threading.Tasks.Task<bool> TryDispatchAsync(");
        builder.AppendLine("        global::AdamSalisbury.Meshworx.Messages.MessageReceivedEventArgs message,");
        builder.AppendLine(
            "        global::System.Threading.CancellationToken cancellationToken = default)");
        builder.AppendLine("    {");
        builder.AppendLine("        global::System.ArgumentNullException.ThrowIfNull(message);");
        builder.AppendLine();
        builder.AppendLine(
            "        if (!message.Headers.TryGetValue("
            + $"global::AdamSalisbury.Meshworx.Contracts.ContractHeaderKeys.Method, out var {prefix}method))");
        builder.AppendLine("        {");
        builder.AppendLine("            return false;");
        builder.AppendLine("        }");
        builder.AppendLine();
        builder.AppendLine($"        switch ({prefix}method)");
        builder.AppendLine("        {");

        foreach (ContractMethod method in model.Methods)
        {
            EmitDispatchCase(builder, model, method, prefix);
        }

        builder.AppendLine("            default:");
        builder.AppendLine("                return false;");
        builder.AppendLine("        }");
        builder.AppendLine("    }");
        builder.AppendLine("}");
    }

    private static void EmitDispatchCase(
        StringBuilder builder, ContractModel model, ContractMethod method, string prefix)
    {
        builder.AppendLine($"            case \"{model.ContractIdentity}.{method.Name}\":");
        builder.AppendLine("            {");

        if (method.ResultType is not null)
        {
            // Checked before the implementation is invoked, not after: replying to a message that was
            // never a request throws, and a handler whose side effects have already been committed
            // cannot be undone by that throw.
            builder.AppendLine("                if (message.CorrelationId is null)");
            builder.AppendLine("                {");
            builder.AppendLine("                    return false;");
            builder.AppendLine("                }");
            builder.AppendLine();
        }

        var arguments = new List<string>();

        if (method.Parameters.Length > 0)
        {
            // TryDeserialize rather than Deserialize: the body comes from a remote peer, so a malformed
            // one is an ordinary runtime condition rather than a programming error. It also applies the
            // content-type check, so another codec's body is declined instead of mis-decoded.
            builder.AppendLine(
                "                if (!global::AdamSalisbury.Meshworx.Serialization.MessageSerializationExtensions"
                + $".TryDeserialize<{ArgumentsTypeReference(model, method)}>(");
            builder.AppendLine(
                $"                        message, _serializer, out var {prefix}arguments)");
            builder.AppendLine($"                    || {prefix}arguments is null)");
            builder.AppendLine("                {");
            builder.AppendLine("                    return false;");
            builder.AppendLine("                }");
            builder.AppendLine();

            arguments.AddRange(
                method.Parameters.Select(
                    p => $"{prefix}arguments.{EscapeIdentifier(ToPascalCase(p.Name))}"));
        }

        if (method.CancellationTokenParameterName is not null)
        {
            arguments.Add("cancellationToken");
        }

        string call = $"_implementation.{EscapeIdentifier(method.Name)}({string.Join(", ", arguments)})";

        if (method.ResultType is null)
        {
            builder.AppendLine($"                await {call}.ConfigureAwait(false);");
        }
        else
        {
            builder.AppendLine($"                var {prefix}result = await {call}.ConfigureAwait(false);");
            builder.AppendLine();
            builder.AppendLine(
                "                await global::AdamSalisbury.Meshworx.Serialization"
                + $".MeshClientSerializationExtensions.ReplyAsync(_replyClient, message, {prefix}result,");
            builder.AppendLine(
                "                    _serializer, cancellationToken: cancellationToken).ConfigureAwait(false);");
        }

        builder.AppendLine();
        builder.AppendLine("                return true;");
        builder.AppendLine("            }");
        builder.AppendLine();
    }

    private static string ArgumentsTypeName(ContractModel model, ContractMethod method)
    {
        return $"{model.BaseName}{method.Name}Arguments";
    }

    /// <summary>
    /// The argument record named as the emitted code must refer to it: fully qualified, so a type in the
    /// contract's own namespace cannot shadow it.
    /// </summary>
    private static string ArgumentsTypeReference(ContractModel model, ContractMethod method)
    {
        string name = ArgumentsTypeName(model, method);

        return model.Namespace.Length == 0
            ? $"global::{name}"
            : $"global::{model.Namespace}.{name}";
    }

    /// <summary>
    /// Escapes an identifier the emitted code takes from a user's own name.
    /// </summary>
    /// <remarks>
    /// Type names are escaped by <see cref="QualifiedFormat"/>; method and parameter names are not, and
    /// a member called <c>@event</c> emitted verbatim derails the parser for the rest of the file.
    /// </remarks>
    private static string EscapeIdentifier(string name)
    {
        return SyntaxFacts.GetKeywordKind(name) != SyntaxKind.None
            || SyntaxFacts.GetContextualKeywordKind(name) != SyntaxKind.None
            ? "@" + name
            : name;
    }

    /// <summary>
    /// Reduces a fully qualified metadata name to the characters a hint name may contain.
    /// </summary>
    private static string SanitizeHintName(string name)
    {
        var builder = new StringBuilder(name.Length);

        foreach (char character in name)
        {
            builder.Append(
                char.IsLetterOrDigit(character) || character == '_' || character == '.'
                    ? character
                    : '_');
        }

        return builder.ToString();
    }

    private static string ToPascalCase(string name)
    {
        return name.Length == 0 || char.IsUpper(name[0])
            ? name
            : char.ToUpperInvariant(name[0]) + name.Substring(1);
    }
}
