using System.Collections.Immutable;
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

    /// <summary>
    /// A well-formed contract produces a source file and no diagnostics, so the cases above are
    /// reporting on the signature rather than on the harness.
    /// </summary>
    [Fact]
    public void WellFormedContract_ReportsNothingAndGeneratesSource()
    {
        (ImmutableArray<Diagnostic> diagnostics, int generatedCount) = RunGenerator(
            """
            using System.Threading;
            using System.Threading.Tasks;
            using AdamSalisbury.Meshworx.Contracts;

            [MeshContract]
            public interface IGoodContract
            {
                Task SendAsync(int value, CancellationToken cancellationToken = default);
            }
            """);

        Assert.Empty(diagnostics.Where(d => d.Id.StartsWith("MESH", StringComparison.Ordinal)));
        Assert.Equal(1, generatedCount);
    }

    /// <summary>
    /// A contract with a bad member emits no source at all. Emitting a proxy implementing only the
    /// members the generator understood would add a misleading "does not implement interface member"
    /// error on top of the real diagnostic.
    /// </summary>
    [Fact]
    public void ContractWithBadMember_GeneratesNoSource()
    {
        (_, int generatedCount) = RunGenerator(
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

        Assert.Equal(0, generatedCount);
    }

    private static void AssertDiagnostic(string source, string expectedId)
    {
        (ImmutableArray<Diagnostic> diagnostics, _) = RunGenerator(source);

        Assert.Contains(diagnostics, d => d.Id == expectedId);
    }

    private static (ImmutableArray<Diagnostic> Diagnostics, int GeneratedCount) RunGenerator(string source)
    {
        var compilation = CSharpCompilation.Create(
            "ContractDiagnosticTests",
            [CSharpSyntaxTree.ParseText(source)],
            ReferenceAssemblies(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MeshContractGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out _, out _);

        GeneratorDriverRunResult result = driver.GetRunResult();
        return (result.Diagnostics, result.GeneratedTrees.Length);
    }

    private static IEnumerable<MetadataReference> ReferenceAssemblies()
    {
        // Every assembly already loaded into this test process, which covers the framework and the
        // contracts package the sources above reference. Simpler and less brittle than naming a
        // reference set, and sufficient because the generator only needs the compilation to bind
        // [MeshContract] and the signatures it is judging.
        return AppDomain.CurrentDomain
            .GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => (MetadataReference)MetadataReference.CreateFromFile(a.Location));
    }
}
