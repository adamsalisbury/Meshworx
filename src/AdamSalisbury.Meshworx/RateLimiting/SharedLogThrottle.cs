using System.Diagnostics;

namespace AdamSalisbury.Meshworx.RateLimiting;

/// <summary>
/// Bounds the aggregate rate at which a class of warning is logged across every client connection, to
/// at most once a second in total.
/// </summary>
/// <remarks>
/// Unlike <see cref="TokenBucket"/> and <see cref="ClientRateLimiter"/>, a single instance of this is
/// shared by every connection's receive loop, so it must be — and is — safe to call from many threads
/// at once. A gate kept per connection bounds how often one throttled client can log, but does nothing
/// about how many clients can be throttled at the same moment: with one gate per connection, C
/// simultaneously throttled clients can still together log up to C lines a second, which grows with
/// the client population exactly like the flood this exists to report. This instead bounds the total
/// to about one line a second regardless of how many connections are contending for it, using a
/// compare-and-swap on the last-logged timestamp so at most one caller "wins" the right to log in any
/// given interval and every other caller in that interval is silently suppressed.
/// </remarks>
internal sealed class SharedLogThrottle
{
    private long _lastLogTimestamp;

    /// <summary>
    /// Reports whether the calling site should log now, granting that right to at most one caller per
    /// second across every thread that calls in.
    /// </summary>
    public bool ShouldLog()
    {
        long previous = Interlocked.Read(ref _lastLogTimestamp);
        long now = Stopwatch.GetTimestamp();

        if (Stopwatch.GetElapsedTime(previous, now).TotalSeconds < 1)
        {
            return false;
        }

        // Only the caller that wins this compare-and-swap logs. Every loser lost to a call that
        // claimed the right to log at least as recently as this one attempted to, so suppressing it
        // costs nothing that the winner has not already covered.
        return Interlocked.CompareExchange(ref _lastLogTimestamp, now, previous) == previous;
    }
}
