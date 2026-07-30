using System.Collections.Immutable;
using System.Globalization;
using AdamSalisbury.Meshworx.Contracts.Generator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace AdamSalisbury.Meshworx.Contracts.UnitTests;

/// <summary>
/// Drives the generator over hand-written sources to check what it reports for a signature it cannot
/// express.
/// </summary>
/// <remarks>
/// These cases cannot live in the test project's own source the way the working contract does: they
/// are, by design, code that fails to compile. Running the generator against an in-memory compilation
/// is the only way to assert on a build error without breaking the build that asserts it.
/// </remarks>
public class ContractDiagnosticTests
{
    [Fact]
    public void NonTaskReturn_ReportsMesh001()
    {
        AssertDiagnostic(
            """
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IBadContract
            {
                int Compute(int value);
            }
            """,
            "MESH001");
    }

    [Fact]
    public void RefParameter_ReportsMesh002()
    {
        AssertDiagnostic(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IBadContract
            {
                Task SendAsync(ref int value);
            }
            """,
            "MESH002");
    }

    [Fact]
    public void OutParameter_ReportsMesh002()
    {
        AssertDiagnostic(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IBadContract
            {
                Task SendAsync(out int value);
            }
            """,
            "MESH002");
    }

    [Fact]
    public void GenericMethod_ReportsMesh003()
    {
        AssertDiagnostic(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IBadContract
            {
                Task SendAsync<TValue>(TValue value);
            }
            """,
            "MESH003");
    }

    [Fact]
    public void OverloadedMethod_ReportsMesh004()
    {
        AssertDiagnostic(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IBadContract
            {
                Task SendAsync(int value);

                Task SendAsync(string value);
            }
            """,
            "MESH004");
    }

    [Fact]
    public void Property_ReportsMesh005()
    {
        AssertDiagnostic(
            """
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IBadContract
            {
                int Count { get; }
            }
            """,
            "MESH005");
    }

    [Fact]
    public void CancellationTokenNotLast_ReportsMesh006()
    {
        AssertDiagnostic(
            """
            using System.Threading;
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IBadContract
            {
                Task SendAsync(CancellationToken cancellationToken, int value);
            }
            """,
            "MESH006");
    }

    [Fact]
    public void GenericContractInterface_ReportsMesh007()
    {
        AssertDiagnostic(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IRepository<TItem>
            {
                Task PutAsync(TItem item);
            }
            """,
            "MESH007");
    }

    [Fact]
    public void ContractWithBaseInterface_ReportsMesh008()
    {
        AssertDiagnostic(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            public interface IBaseContract
            {
                Task BaseAsync(int value);
            }

            [MeshContract]
            public interface IDerivedContract : IBaseContract
            {
                Task DerivedAsync(int value);
            }
            """,
            "MESH008");
    }

    [Fact]
    public void NestedContractInterface_ReportsMesh009()
    {
        AssertDiagnostic(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            public static class Outer
            {
                [MeshContract]
                public interface IInner
                {
                    Task PingAsync();
                }
            }
            """,
            "MESH009");
    }

    /// <summary>
    /// A well-formed contract produces a source file, no diagnostics, and code that compiles — so the
    /// cases above are reporting on the signature rather than on the harness.
    /// </summary>
    [Fact]
    public void WellFormedContract_ReportsNothingAndGeneratesCompilingSource()
    {
        AssertGeneratedCodeCompiles(
            """
            using System.Threading;
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IGoodContract
            {
                Task SendAsync(int value, CancellationToken cancellationToken = default);

                Task<string?> DescribeAsync(string? filter);
            }
            """);
    }

    /// <summary>
    /// Two contracts sharing a simple name in different namespaces each get their own generated file.
    /// </summary>
    /// <remarks>
    /// A hint name derived from the simple name alone collides, and a duplicate hint name throws inside
    /// the generator — reported as a single warning while suppressing every file the generator would
    /// have emitted for the whole compilation, including well-formed contracts elsewhere in it.
    /// </remarks>
    [Fact]
    public void ContractsSharingASimpleNameInDifferentNamespaces_BothGenerate()
    {
        AssertGeneratedCodeCompiles(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            namespace Probe.Alpha
            {
                [MeshContract]
                public interface IOrderService
                {
                    Task PingAsync();
                }
            }

            namespace Probe.Beta
            {
                [MeshContract]
                public interface IOrderService
                {
                    Task PongAsync();
                }
            }
            """,
            expectedGeneratedCount: 2);
    }

    /// <summary>
    /// A type in the contract's own namespace that shadows a segment of an emitted type name does not
    /// break the generated file, because every emitted name is fully qualified.
    /// </summary>
    [Fact]
    public void ContractAlongsideATypeShadowingSystem_GeneratesCompilingSource()
    {
        AssertGeneratedCodeCompiles(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            namespace Probe.Shadowed
            {
                public sealed class System { }

                [MeshContract]
                public interface IShadowedContract
                {
                    Task SendAsync(global::System.Guid id);
                }
            }
            """);
    }

    /// <summary>
    /// Keyword-named members and parameters, a parameter named after a generated local, and a non-token
    /// parameter called <c>cancellationToken</c> all emit code that compiles.
    /// </summary>
    [Fact]
    public void ContractWithAwkwardIdentifiers_GeneratesCompilingSource()
    {
        AssertGeneratedCodeCompiles(
            """
            using System.Threading;
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IAwkwardProbe
            {
                Task @event(int @class, int __arguments, string cancellationToken, CancellationToken ct = default);

                Task<int> @void();
            }
            """);
    }

    /// <summary>
    /// A static interface member is not part of the wire contract: it is skipped rather than dispatched,
    /// and does not stop the rest of the contract generating.
    /// </summary>
    [Fact]
    public void ContractWithStaticMember_SkipsItAndGeneratesCompilingSource()
    {
        AssertGeneratedCodeCompiles(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IContractWithStatics
            {
                static string Describe() => "not on the wire";

                Task SendAsync(int value);
            }
            """);
    }

    /// <summary>
    /// A contract with a bad member emits no source at all. Emitting a proxy implementing only the
    /// members the generator understood would add a misleading "does not implement interface member"
    /// error on top of the real diagnostic.
    /// </summary>
    [Fact]
    public void ContractWithBadMember_GeneratesNoSource()
    {
        GeneratorRun run = RunGenerator(
            """
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IBadContract
            {
                Task GoodAsync(int value);

                int BadOne(int value);
            }
            """);

        Assert.Equal(0, run.GeneratedCount);
    }

    private static void AssertDiagnostic(string source, string expectedId)
    {
        GeneratorRun run = RunGenerator(source);

        Assert.Contains(run.Diagnostics, d => d.Id == expectedId);
    }

    /// <summary>
    /// Asserts that the generator both accepted the contract and emitted code that compiles.
    /// </summary>
    /// <remarks>
    /// The second half is the one that matters. A shape the generator neither diagnoses nor handles
    /// correctly produces errors at coordinates inside a file the contract's author never wrote, and
    /// nothing in a suite that only counts generated trees can see it.
    /// </remarks>
    private static void AssertGeneratedCodeCompiles(string source, int expectedGeneratedCount = 1)
    {
        GeneratorRun run = RunGenerator(source);

        Assert.Empty(run.Diagnostics.Where(d => d.Id.StartsWith("MESH", StringComparison.Ordinal)));
        Assert.Equal(expectedGeneratedCount, run.GeneratedCount);
        Assert.Empty(run.GeneratedCodeErrors.Select(d => d.GetMessage(CultureInfo.InvariantCulture)));
    }

    private static GeneratorRun RunGenerator(string source)
    {
        var compilation = CSharpCompilation.Create(
            "ContractDiagnosticTests",
            [CSharpSyntaxTree.ParseText(source)],
            ReferenceAssemblies(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MeshContractGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out Compilation output, out _);

        GeneratorDriverRunResult result = driver.GetRunResult();

        // Errors are attributed to the tree they were reported on, and only the generated ones are this
        // suite's business: an error in the hand-written probe source above would be a fault in the
        // probe, not in what the generator emitted for it.
        var generatedPaths = new HashSet<string>(
            result.GeneratedTrees.Select(tree => tree.FilePath), StringComparer.Ordinal);

        ImmutableArray<Diagnostic> generatedCodeErrors = output
            .GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error
                && d.Location.SourceTree is { } tree
                && generatedPaths.Contains(tree.FilePath))
            .ToImmutableArray();

        return new GeneratorRun(result.Diagnostics, result.GeneratedTrees.Length, generatedCodeErrors);
    }

    private static IEnumerable<MetadataReference> ReferenceAssemblies()
    {
        // Touch a type from each assembly the generated code depends on, so that all three are loaded
        // before the reference set below is snapshotted — the generated proxy and dispatcher name the
        // client and the codec, not just the attribute the generator binds.
        _ = typeof(MeshContractAttribute);
        _ = typeof(IMeshClient);
        _ = typeof(Serialization.JsonMessageSerializer);

        // Every assembly already loaded into this test process, which covers the framework and the
        // packages the sources above reference. Simpler and less brittle than naming a reference set.
        return AppDomain.CurrentDomain
            .GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => (MetadataReference)MetadataReference.CreateFromFile(a.Location));
    }

    private sealed record GeneratorRun(
        ImmutableArray<Diagnostic> Diagnostics,
        int GeneratedCount,
        ImmutableArray<Diagnostic> GeneratedCodeErrors);
}
