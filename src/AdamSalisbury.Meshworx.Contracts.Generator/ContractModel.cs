using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;

namespace AdamSalisbury.Meshworx.Contracts.Generator;

/// <summary>
/// Everything the generator needs about one contract, reduced to values.
/// </summary>
/// <remarks>
/// Deliberately holds no Roslyn symbols. An incremental generator caches the output of its transform
/// stage and compares it between compilations, so anything reachable from the model is kept alive for
/// as long as the cache is — and a compilation's symbols are among the largest objects there are.
/// Reducing to strings and structural-equality records is what lets the pipeline compare cheaply and
/// let the compilation go.
/// </remarks>
internal sealed record ContractModel(
    string Namespace,
    string InterfaceName,
    string BaseName,
    bool IsPublic,
    ImmutableArray<ContractMethod> Methods,
    ImmutableArray<DiagnosticInfo> Diagnostics)
{
    public bool Equals(ContractModel? other)
    {
        return other is not null
            && Namespace == other.Namespace
            && InterfaceName == other.InterfaceName
            && BaseName == other.BaseName
            && IsPublic == other.IsPublic
            && Methods.SequenceEqual(other.Methods)
            && Diagnostics.SequenceEqual(other.Diagnostics);
    }

    public override int GetHashCode()
    {
        return Hash.Combine(
            Namespace?.GetHashCode() ?? 0,
            InterfaceName?.GetHashCode() ?? 0,
            BaseName?.GetHashCode() ?? 0,
            IsPublic ? 1 : 0,
            Methods.Length,
            Diagnostics.Length);
    }
}

internal sealed record ContractMethod(
    string Name,
    string? ResultType,
    bool HasCancellationToken,
    ImmutableArray<ContractParameter> Parameters)
{
    public bool Equals(ContractMethod? other)
    {
        return other is not null
            && Name == other.Name
            && ResultType == other.ResultType
            && HasCancellationToken == other.HasCancellationToken
            && Parameters.SequenceEqual(other.Parameters);
    }

    public override int GetHashCode()
    {
        return Hash.Combine(
            Name?.GetHashCode() ?? 0,
            ResultType?.GetHashCode() ?? 0,
            HasCancellationToken ? 1 : 0,
            Parameters.Length);
    }
}

internal sealed record ContractParameter(string Type, string Name);

/// <summary>
/// A diagnostic reduced to values, for the same reason <see cref="ContractModel"/> is: a
/// <see cref="Diagnostic"/> holds a <see cref="Location"/>, which holds a syntax tree, which holds the
/// compilation the incremental cache must be able to release.
/// </summary>
internal sealed record DiagnosticInfo(
    DiagnosticDescriptor Descriptor,
    string? FilePath,
    TextSpanInfo Span,
    LinePositionSpanInfo LineSpan,
    ImmutableArray<string> MessageArguments)
{
    internal static DiagnosticInfo Create(
        DiagnosticDescriptor descriptor, ISymbol symbol, params string[] messageArguments)
    {
        Location location = symbol.Locations.FirstOrDefault() ?? Location.None;
        Microsoft.CodeAnalysis.Text.LinePositionSpan lineSpan = location.GetLineSpan().Span;

        return new DiagnosticInfo(
            descriptor,
            location.SourceTree?.FilePath,
            new TextSpanInfo(location.SourceSpan.Start, location.SourceSpan.Length),
            new LinePositionSpanInfo(
                lineSpan.Start.Line, lineSpan.Start.Character, lineSpan.End.Line, lineSpan.End.Character),
            messageArguments.ToImmutableArray());
    }

    internal Diagnostic ToDiagnostic()
    {
        Location location = FilePath is null
            ? Location.None
            : Location.Create(
                FilePath,
                new Microsoft.CodeAnalysis.Text.TextSpan(Span.Start, Span.Length),
                new Microsoft.CodeAnalysis.Text.LinePositionSpan(
                    new Microsoft.CodeAnalysis.Text.LinePosition(LineSpan.StartLine, LineSpan.StartCharacter),
                    new Microsoft.CodeAnalysis.Text.LinePosition(LineSpan.EndLine, LineSpan.EndCharacter)));

        return Diagnostic.Create(Descriptor, location, MessageArguments.ToArray<object?>());
    }

    public bool Equals(DiagnosticInfo? other)
    {
        return other is not null
            && Descriptor.Id == other.Descriptor.Id
            && FilePath == other.FilePath
            && Span == other.Span
            && MessageArguments.SequenceEqual(other.MessageArguments);
    }

    public override int GetHashCode()
    {
        return Hash.Combine(
            Descriptor.Id?.GetHashCode() ?? 0,
            FilePath?.GetHashCode() ?? 0,
            Span.GetHashCode(),
            MessageArguments.Length);
    }
}

internal readonly record struct TextSpanInfo(int Start, int Length);

internal readonly record struct LinePositionSpanInfo(
    int StartLine, int StartCharacter, int EndLine, int EndCharacter);

/// <summary>
/// Combines hash codes without System.HashCode, which .NET Standard 2.0 does not have.
/// </summary>
internal static class Hash
{
    internal static int Combine(params int[] values)
    {
        unchecked
        {
            int hash = 17;

            foreach (int value in values)
            {
                hash = (hash * 31) + value;
            }

            return hash;
        }
    }
}
