namespace AdamSalisbury.Meshworx.RateLimiting;

/// <summary>
/// Bounds how much a single registered client may push through the hub's receive loop, so an
/// unauthenticated flood of inbound frames cannot turn the hub into an amplifier: one inbound frame
/// to <c>BroadcastMessage</c> or a group send fans out to every recipient, so the cost of admitting
/// it is not the client's alone to spend.
/// </summary>
/// <remarks>
/// Owned by, and only ever called from, the receive loop of the client connection it guards — see
/// <see cref="TokenBucket"/> for why that makes locking unnecessary. Four budgets apply, in a fixed
/// order that each gates the next: a general per-second message-count budget, a general per-second
/// byte-volume budget, and — only for a broadcast or group send — a stricter per-second fan-out
/// frequency budget, and then a per-second fan-out delivery-volume budget charged by the actual
/// number of recipients rather than by the frame. The frequency budget bounds how often a client may
/// trigger a fan-out at all; the delivery budget bounds the total amplification that results, so
/// raising the client population — or the frequency budget itself — cannot raise the hub's actual
/// worst-case fan-out cost without also raising this.
/// </remarks>
internal sealed class ClientRateLimiter
{
    private readonly TokenBucket _messageBudget;
    private readonly TokenBucket _byteBudget;
    private readonly TokenBucket _fanOutFrequencyBudget;
    private readonly TokenBucket _fanOutDeliveryBudget;

    public ClientRateLimiter(
        double maxInboundMessagesPerSecond,
        double maxInboundBytesPerSecond,
        double maxFanOutMessagesPerSecond,
        double maxFanOutDeliveriesPerSecond)
    {
        _messageBudget = new TokenBucket(maxInboundMessagesPerSecond);
        _byteBudget = new TokenBucket(maxInboundBytesPerSecond);
        _fanOutFrequencyBudget = new TokenBucket(maxFanOutMessagesPerSecond);
        _fanOutDeliveryBudget = new TokenBucket(maxFanOutDeliveriesPerSecond);
    }

    /// <summary>
    /// Charges a just-received frame of <paramref name="frameLength"/> bytes against the general
    /// message-count and byte-volume budgets. Every inbound frame is charged here, regardless of type
    /// — including an empty one — since processing any of them costs the hub something.
    /// </summary>
    /// <remarks>
    /// Both budgets are checked before either is charged, so a frame refused by the byte budget never
    /// spends a message-count token it will not get credit for, and vice versa — the two really are
    /// independent, rather than only independent in the direction that happens to matter for a flood.
    /// </remarks>
    public bool TryAdmitFrame(int frameLength)
    {
        if (!_messageBudget.HasAvailable(1) || !_byteBudget.HasAvailable(frameLength))
        {
            return false;
        }

        _messageBudget.Consume(1);
        _byteBudget.Consume(frameLength);
        return true;
    }

    /// <summary>
    /// Charges a frame that has already cleared <see cref="TryAdmitFrame"/> against the fan-out
    /// frequency budget. Call only for <c>BroadcastMessage</c> and <c>GroupMessage</c> frames — the two
    /// message types whose delivery cost multiplies by recipient count rather than staying fixed.
    /// </summary>
    public bool TryAdmitFanOut()
    {
        return _fanOutFrequencyBudget.TryConsume(1);
    }

    /// <summary>
    /// Charges a fan-out that has already cleared <see cref="TryAdmitFanOut"/> against the delivery-
    /// volume budget, by the actual number of recipients it is about to reach — not by the frame.
    /// </summary>
    /// <remarks>
    /// <see cref="TryAdmitFanOut"/> bounds how often a client may trigger a broadcast or group send at
    /// all, but a frequency budget alone does not bound the resulting amplification: at a frequency of
    /// 20 a second, a hub with a population of 1,000 still sees up to 20,000 deliveries a second from
    /// one client, and that figure grows without limit as the population does. Charging this budget by
    /// <paramref name="recipientCount"/> keeps the hub's actual worst-case fan-out cost bounded by a
    /// number that does not move just because the client population, or an integrator's configured
    /// frequency budget, does.
    /// </remarks>
    public bool TryAdmitFanOutDelivery(int recipientCount)
    {
        // Even a fan-out that currently has no recipients still cost the hub the work of deciding
        // that, so it is never free — but it is also never charged more than the frequency budget
        // already bounds it to, since a genuinely empty fan-out cannot amplify anything.
        return _fanOutDeliveryBudget.TryConsume(Math.Max(1, recipientCount));
    }
}
