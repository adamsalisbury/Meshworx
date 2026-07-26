using System.Collections.Concurrent;
using System.Net;
using AdamSalisbury.Meshworx.Transport.Quic;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Quic;

/// <summary>
/// Deterministic, network-free tests for the per-source negotiation admission primitives
/// (<see cref="QuicTransportListener.TryAdmitSource"/>, <see cref="QuicTransportListener.ReleaseSource"/>,
/// <see cref="QuicTransportListener.NormaliseForSourceCap"/>) that bound how many concurrently negotiating
/// connections a single source may hold. These exist so a single source completing real QUIC handshakes
/// and never opening a stream cannot occupy the whole negotiation pool by itself — see
/// <see cref="QuicTransportListener"/>'s constructor doc for the full rationale. Exercised directly,
/// rather than only through a real multi-source network flood, because every connection in this test
/// suite's loopback tests necessarily originates from the same address and so cannot itself demonstrate
/// the cap distinguishing one source from another.
/// </summary>
public sealed class QuicTransportListenerSourceAdmissionTests
{
    /// <summary>
    /// Admission succeeds up to the cap and is refused exactly at it, for a single source.
    /// </summary>
    [Fact]
    public void TryAdmitSource_UpToCap_AdmitsThenRefuses()
    {
        var counts = new ConcurrentDictionary<IPAddress, int>();
        var address = IPAddress.Parse("203.0.113.5");

        Assert.True(QuicTransportListener.TryAdmitSource(counts, address, maxPerSource: 2));
        Assert.True(QuicTransportListener.TryAdmitSource(counts, address, maxPerSource: 2));
        Assert.False(QuicTransportListener.TryAdmitSource(counts, address, maxPerSource: 2));
    }

    /// <summary>
    /// Releasing a slot frees it up for a later admission from the same source.
    /// </summary>
    [Fact]
    public void ReleaseSource_FreesSlotForFutureAdmission()
    {
        var counts = new ConcurrentDictionary<IPAddress, int>();
        var address = IPAddress.Parse("203.0.113.5");

        Assert.True(QuicTransportListener.TryAdmitSource(counts, address, maxPerSource: 1));
        Assert.False(QuicTransportListener.TryAdmitSource(counts, address, maxPerSource: 1));

        QuicTransportListener.ReleaseSource(counts, address);

        Assert.True(QuicTransportListener.TryAdmitSource(counts, address, maxPerSource: 1));
    }

    /// <summary>
    /// Releasing every admitted slot for a source removes its entry entirely, so the dictionary is
    /// bounded by the number of sources currently negotiating rather than every source ever seen.
    /// </summary>
    [Fact]
    public void ReleaseSource_AllSlotsReleased_RemovesTheSourceEntry()
    {
        var counts = new ConcurrentDictionary<IPAddress, int>();
        var address = IPAddress.Parse("203.0.113.5");

        Assert.True(QuicTransportListener.TryAdmitSource(counts, address, maxPerSource: 3));
        Assert.True(QuicTransportListener.TryAdmitSource(counts, address, maxPerSource: 3));
        QuicTransportListener.ReleaseSource(counts, address);
        QuicTransportListener.ReleaseSource(counts, address);

        Assert.False(counts.ContainsKey(address));
    }

    /// <summary>
    /// Two different sources are capped independently — one being at its own cap does not affect the
    /// other's admission.
    /// </summary>
    [Fact]
    public void TryAdmitSource_DifferentSources_AreCappedIndependently()
    {
        var counts = new ConcurrentDictionary<IPAddress, int>();
        var first = IPAddress.Parse("203.0.113.5");
        var second = IPAddress.Parse("198.51.100.9");

        Assert.True(QuicTransportListener.TryAdmitSource(counts, first, maxPerSource: 1));
        Assert.False(QuicTransportListener.TryAdmitSource(counts, first, maxPerSource: 1));

        // The second source is unaffected by the first being at its own cap.
        Assert.True(QuicTransportListener.TryAdmitSource(counts, second, maxPerSource: 1));
    }

    /// <summary>
    /// Under concurrent admission attempts for the same source, the cap is never exceeded — proving the
    /// compare-and-swap retry loop is genuinely race-safe, not merely correct in the single-threaded case.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task TryAdmitSource_ConcurrentCallersForSameSource_NeverExceedsCap()
    {
        var counts = new ConcurrentDictionary<IPAddress, int>();
        var address = IPAddress.Parse("203.0.113.5");
        const int cap = 10;
        const int attempts = 200;

        var admittedCount = 0;
        var tasks = new Task[attempts];
        for (int i = 0; i < attempts; i++)
        {
            tasks[i] = Task.Run(() =>
            {
                if (QuicTransportListener.TryAdmitSource(counts, address, cap))
                {
                    Interlocked.Increment(ref admittedCount);
                }
            });
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        Assert.Equal(cap, admittedCount);
        Assert.Equal(cap, counts[address]);
    }

    /// <summary>
    /// An IPv4 address is used unchanged as the cap's key.
    /// </summary>
    [Fact]
    public void NormaliseForSourceCap_IPv4Address_ReturnsUnchanged()
    {
        var address = IPAddress.Parse("203.0.113.5");

        Assert.Equal(address, QuicTransportListener.NormaliseForSourceCap(address));
    }

    /// <summary>
    /// Two IPv6 addresses within the same /64 network prefix normalise to the same key, so an attacker
    /// cannot defeat the per-source cap simply by rotating addresses within their own allocation.
    /// </summary>
    [Fact]
    public void NormaliseForSourceCap_IPv6AddressesInSameSixtyFourPrefix_NormaliseToSameKey()
    {
        var first = IPAddress.Parse("2001:db8:abcd:1234::1");
        var second = IPAddress.Parse("2001:db8:abcd:1234:ffff:ffff:ffff:ffff");

        Assert.Equal(
            QuicTransportListener.NormaliseForSourceCap(first),
            QuicTransportListener.NormaliseForSourceCap(second));
    }

    /// <summary>
    /// Two IPv6 addresses in different /64 network prefixes normalise to different keys.
    /// </summary>
    [Fact]
    public void NormaliseForSourceCap_IPv6AddressesInDifferentSixtyFourPrefixes_NormaliseToDifferentKeys()
    {
        var first = IPAddress.Parse("2001:db8:abcd:1234::1");
        var second = IPAddress.Parse("2001:db8:abcd:5678::1");

        Assert.NotEqual(
            QuicTransportListener.NormaliseForSourceCap(first),
            QuicTransportListener.NormaliseForSourceCap(second));
    }
}
