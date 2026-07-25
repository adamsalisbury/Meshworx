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
    /// Attempts to withdraw <paramref name="cost"/> tokens, having first credited the bucket for
    /// whatever time has elapsed since the last call. Returns <see langword="false"/>, withdrawing
    /// nothing, when the balance after refill is short of the cost.
    /// </summary>
    public bool TryConsume(double cost)
    {
        long now = Stopwatch.GetTimestamp();
        double elapsedSeconds = Stopwatch.GetElapsedTime(_lastRefillTimestamp, now).TotalSeconds;
        _lastRefillTimestamp = now;

        _tokens = Math.Min(_capacity, _tokens + (elapsedSeconds * _refillPerSecond));

        if (_tokens < cost)
        {
            return false;
        }

        _tokens -= cost;
        return true;
    }
}
