using System.Diagnostics;
using System.Reflection;

namespace AdamSalisbury.Meshworx.Diagnostics;

/// <summary>
/// The <see cref="System.Diagnostics.ActivitySource"/> this library creates its tracing spans from.
/// </summary>
/// <remarks>
/// Tracing is opt-in in the way .NET itself defines opt-in: nothing here produces a span until
/// something registers an <see cref="ActivityListener"/> for this source — an OpenTelemetry SDK, a
/// test, or a hand-rolled listener. Until then <see cref="ActivitySource.StartActivity(string,
/// ActivityKind)"/> returns <see langword="null"/>, no <see cref="Activity"/> is allocated, no trace
/// headers are written, and the frames on the wire are byte-for-byte what they were before this
/// existed.
/// <para>
/// One static source for the whole library rather than one per client, matching how
/// <see cref="ActivitySource"/> is meant to be used: a listener subscribes to a source by name, and
/// that name identifies the library, not an instance of it. This differs deliberately from the
/// per-hub <see cref="System.Diagnostics.Metrics.Meter"/>, which is disposed with its hub so a torn-down
/// hub stops reporting; a source has no such per-instance state to retire.
/// </para>
/// </remarks>
internal static class MeshworxActivitySource
{
    /// <summary>
    /// The source name a listener subscribes to in order to receive this library's spans.
    /// </summary>
    internal const string Name = "AdamSalisbury.Meshworx";

    /// <summary>
    /// The span raised on the sending client when a message is handed to the transport.
    /// </summary>
    internal const string SendActivityName = "Meshworx.Send";

    /// <summary>
    /// The span raised on the receiving client around the delivery of a message to the application.
    /// </summary>
    internal const string ReceiveActivityName = "Meshworx.Receive";

    /// <summary>
    /// The shared source. Never disposed: it lives for the lifetime of the process, as an
    /// <see cref="ActivitySource"/> identifying a library rather than an instance is meant to.
    /// </summary>
    internal static ActivitySource Instance { get; } = new(
        Name,
        typeof(MeshworxActivitySource).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion);
}
