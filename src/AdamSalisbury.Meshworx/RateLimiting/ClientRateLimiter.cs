using System.Diagnostics;

namespace AdamSalisbury.Meshworx.RateLimiting;

/// <summary>
/// Bounds how much a single registered client may push through the hub's receive loop, so an
/// unauthenticated flood of inbound frames cannot turn the hub into an amplifier: one inbound frame
/// to <c>BroadcastMessage</c> or a group send fans out to every recipient, so the cost of admitting
/// it is not the client's alone to spend.
/// </summary>
/// <remarks>
/// Owned by, and only ever called from, the receive loop of the client connection it guards — see
/// <see cref="TokenBucket"/> for why that makes locking unnecessary. Three budgets apply in a fixed
/// order, each gating the next: a general per-second message-count budget, a general per-second
/// byte-volume budget, and — only for a broadcast or group send — a stricter per-second fan-out
/// budget on top of both. Because <c>&amp;&amp;</c> short-circuits, a frame refused by an earlier
/// budget never touches a later one, so throttling on message count alone leaves the byte and
/// fan-out budgets untouched and ready the moment the client backs off.
/// </remarks>
internal sealed class ClientRateLimiter
{
    private readonly TokenBucket _messageBudget;
    private readonly TokenBucket _byteBudget;
    private readonly TokenBucket _fanOutBudget;
    // Zero rather than a sentinel such as long.MinValue: Stopwatch.GetElapsedTime subtracts this from
    // the current timestamp, and a negative starting value close to long.MinValue would overflow that
    // subtraction. Zero predates any real timestamp this process can observe, so the first throttled
    // frame always logs.
    private long _lastThrottleLogTimestamp;

    public ClientRateLimiter(
        double maxInboundMessagesPerSecond, double maxInboundBytesPerSecond, double maxFanOutMessagesPerSecond)
    {
        _messageBudget = new TokenBucket(maxInboundMessagesPerSecond);
        _byteBudget = new TokenBucket(maxInboundBytesPerSecond);
        _fanOutBudget = new TokenBucket(maxFanOutMessagesPerSecond);
    }

    /// <summary>
    /// Charges a just-received frame of <paramref name="frameLength"/> bytes against the general
    /// message-count and byte-volume budgets. Every inbound frame is charged here, regardless of
    /// type, since processing any of them costs the hub something.
    /// </summary>
    public bool TryAdmitFrame(int frameLength)
    {
        return _messageBudget.TryConsume(1) && _byteBudget.TryConsume(frameLength);
    }

    /// <summary>
    /// Charges a frame that has already cleared <see cref="TryAdmitFrame"/> against the stricter
    /// fan-out budget. Call only for <c>BroadcastMessage</c> and <c>GroupMessage</c> frames — the two
    /// message types whose delivery cost multiplies by recipient count rather than staying fixed.
    /// </summary>
    public bool TryAdmitFanOut()
    {
        return _fanOutBudget.TryConsume(1);
    }

    /// <summary>
    /// Reports whether a throttled frame is worth logging, at most once per second per connection.
    /// Without this, a sustained flood — the exact condition being throttled — would write one log
    /// line per dropped frame, trading a network amplification lever for a logging one.
    /// </summary>
    public bool ShouldLogThrottle()
    {
        long now = Stopwatch.GetTimestamp();

        if (Stopwatch.GetElapsedTime(_lastThrottleLogTimestamp, now).TotalSeconds < 1)
        {
            return false;
        }

        _lastThrottleLogTimestamp = now;
        return true;
    }
}
