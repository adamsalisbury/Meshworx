namespace AdamSalisbury.Meshworx.Diagnostics;

/// <summary>
/// The name of the <see cref="System.Diagnostics.Metrics.Meter"/> every Meshworx component publishes its
/// instruments to.
/// </summary>
/// <remarks>
/// Shared by <see cref="MeshHub"/> and <see cref="MeshClientReconnector"/> so an exporter that subscribes
/// to this one name sees every Meshworx metric regardless of which component recorded it — an
/// OpenTelemetry <c>AddMeter("AdamSalisbury.Meshworx")</c> call, for instance, needs nothing more than
/// this string.
/// </remarks>
internal static class MeshworxMeterName
{
    public const string Value = "AdamSalisbury.Meshworx";
}
