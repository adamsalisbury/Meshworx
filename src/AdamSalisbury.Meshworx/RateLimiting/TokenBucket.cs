using System.Diagnostics;

namespace AdamSalisbury.Meshworx.RateLimiting;

/// <summary>
/// A continuous-refill token bucket used to bound a rate of consumption to a configured budget per
/// second, with a burst allowance equal to one second's worth of that budget.
/// </summary>
/// <remarks>
/// Not thread-safe. Each instance is owned by a single client connection and is only ever touched
/// from that connection's own receive loop, matching the single-owner pattern already used for
/// <c>ClientConnection.Groups</c> elsewhere in the hub — no lock is taken because no second thread
/// ever calls in. Refill is computed from elapsed wall-clock time via <see cref="Stopwatch"/> rather
/// than a periodic timer, so an idle connection costs nothing between frames and a burst after a
/// quiet spell is judged against the tokens that accumulated while it was quiet, not against a fixed
/// window boundary.
/// </remarks>
internal sealed class TokenBucket
{
    private readonly double _capacity;
    private readonly double _refillPerSecond;
    private double _tokens;
    private long _lastRefillTimestamp;

    /// <param name="refillPerSecond">
    /// The steady-state budget per second. Also doubles as the bucket's capacity, so a connection that
    /// has been under budget for a while may burst up to one second's worth of allowance in a single
    /// instant before the limit bites.
    /// </param>
    public TokenBucket(double refillPerSecond)
    {
        _capacity = refillPerSecond;
        _refillPerSecond = refillPerSecond;
        _tokens = refillPerSecond;
        _lastRefillTimestamp = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Credits the bucket for whatever time has elapsed since the last call, then reports whether it
    /// currently holds at least <paramref name="cost"/> tokens — without withdrawing anything. Paired
    /// with <see cref="Consume"/> so a caller weighing more than one bucket can check every one of them
    /// before committing to any of them, rather than withdrawing from an earlier bucket only to be
    /// refused by a later one.
    /// </summary>
    public bool HasAvailable(double cost)
    {
        Refill();
        return _tokens >= cost;
    }

    /// <summary>
    /// Withdraws <paramref name="cost"/> tokens unconditionally. Only call this once a preceding
    /// <see cref="HasAvailable"/> on the same instance has confirmed the balance covers it — this
    /// performs no check of its own and can drive the balance negative if that has not been done.
    /// </summary>
    public void Consume(double cost)
    {
        _tokens -= cost;
    }

    /// <summary>
    /// Attempts to withdraw <paramref name="cost"/> tokens, having first credited the bucket for
    /// whatever time has elapsed since the last call. Returns <see langword="false"/>, withdrawing
    /// nothing, when the balance after refill is short of the cost. Equivalent to
    /// <see cref="HasAvailable"/> followed by <see cref="Consume"/> when it returns
    /// <see langword="true"/>, for a caller that only ever weighs one bucket at a time.
    /// </summary>
    public bool TryConsume(double cost)
    {
        if (!HasAvailable(cost))
        {
            return false;
        }

        Consume(cost);
        return true;
    }

    private void Refill()
    {
        long now = Stopwatch.GetTimestamp();
        double elapsedSeconds = Stopwatch.GetElapsedTime(_lastRefillTimestamp, now).TotalSeconds;
        _lastRefillTimestamp = now;

        _tokens = Math.Min(_capacity, _tokens + (elapsedSeconds * _refillPerSecond));
    }
}
