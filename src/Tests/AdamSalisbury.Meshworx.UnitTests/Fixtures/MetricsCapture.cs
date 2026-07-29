using System.Diagnostics.Metrics;

namespace AdamSalisbury.Meshworx.UnitTests.Fixtures;

/// <summary>
/// Captures every measurement recorded by one named instrument on a specific <see cref="Meter"/>.
/// </summary>
/// <remarks>
/// Filters by the exact <see cref="Meter"/> reference — obtained from a hub or reconnector's
/// <c>GetMeterForTesting()</c> — rather than by instrument name alone, so a test is immune to any other
/// <see cref="AdamSalisbury.Meshworx.MeshHub"/> or <see cref="AdamSalisbury.Meshworx.MeshClientReconnector"/>
/// publishing to the same meter name concurrently, whether in the same test class or one running in
/// parallel.
/// </remarks>
internal sealed class MetricsCapture<T> : IDisposable
    where T : struct
{
    private readonly MeterListener _listener = new();
    private readonly Lock _lock = new();
    private readonly List<T> _values = [];
    private readonly List<KeyValuePair<string, object?>[]> _tags = [];

    public MetricsCapture(Meter meter, string instrumentName)
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (ReferenceEquals(instrument.Meter, meter) && instrument.Name == instrumentName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };

        _listener.SetMeasurementEventCallback<T>(OnMeasurementRecorded);
        _listener.Start();
    }

    /// <summary>
    /// Every value recorded so far, in recording order.
    /// </summary>
    public IReadOnlyList<T> Values
    {
        get
        {
            lock (_lock)
            {
                return [.. _values];
            }
        }
    }

    /// <summary>
    /// The tags recorded alongside each value in <see cref="Values"/>, at the same index.
    /// </summary>
    public IReadOnlyList<KeyValuePair<string, object?>[]> Tags
    {
        get
        {
            lock (_lock)
            {
                return [.. _tags];
            }
        }
    }

    /// <summary>
    /// Forces every enabled observable instrument (an <see cref="ObservableGauge{T}"/>, for instance) to
    /// report its current value immediately, rather than waiting for the listener's own collection cycle.
    /// </summary>
    public void RecordObservableInstruments()
    {
        _listener.RecordObservableInstruments();
    }

    private void OnMeasurementRecorded(
        Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        lock (_lock)
        {
            _values.Add(measurement);
            _tags.Add(tags.ToArray());
        }
    }

    public void Dispose()
    {
        _listener.Dispose();
    }
}
