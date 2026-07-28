namespace AdamSalisbury.Meshworx;

/// <summary>
/// Controls whether <see cref="IMeshClient.SendAsync(Guid, ReadOnlyMemory{byte}, DeliveryOptions, CancellationToken)"/>
/// waits for the recipient to acknowledge delivery before completing.
/// </summary>
/// <remarks>
/// The default, fire-and-forget behaviour is unchanged and remains the only behaviour for every other
/// <c>SendAsync</c> overload — this type exists purely to opt a single send into the reliable-delivery
/// tier. <see cref="None"/> is the default value of this struct, so a caller who never touches
/// <see cref="DeliveryOptions"/> gets exactly today's behaviour.
/// </remarks>
public readonly struct DeliveryOptions : IEquatable<DeliveryOptions>
{
    private DeliveryOptions(bool requireAcknowledgement, TimeSpan? acknowledgementTimeout, bool awaitCapacity)
    {
        RequireAcknowledgement = requireAcknowledgement;
        AcknowledgementTimeout = acknowledgementTimeout;
        AwaitCapacity = awaitCapacity;
    }

    /// <summary>
    /// The default options: best-effort, fire-and-forget delivery with no acknowledgement, identical to
    /// calling one of the overloads that does not accept <see cref="DeliveryOptions"/> at all.
    /// </summary>
    public static readonly DeliveryOptions None;

    /// <summary>
    /// Gets a value indicating whether the send should wait for the recipient to acknowledge delivery.
    /// </summary>
    public bool RequireAcknowledgement { get; }

    /// <summary>
    /// Gets the maximum time to wait for the acknowledgement before the send fails with a
    /// <see cref="TimeoutException"/>, or <see langword="null"/> when <see cref="RequireAcknowledgement"/>
    /// is <see langword="false"/>.
    /// </summary>
    public TimeSpan? AcknowledgementTimeout { get; }

    /// <summary>
    /// Gets a value indicating whether the hub should await capacity on the recipient's outbound queue,
    /// rather than dropping the message immediately, if that queue is full at the moment the message is
    /// routed.
    /// </summary>
    public bool AwaitCapacity { get; }

    /// <summary>
    /// Requests a delivery acknowledgement: the send completes once the recipient's client has handed
    /// the message to its application, or fails with a <see cref="TimeoutException"/> if that does not
    /// happen within <paramref name="timeout"/>.
    /// </summary>
    /// <param name="timeout">The maximum time to wait for the acknowledgement.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is not positive.</exception>
    public static DeliveryOptions RequireAck(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "The acknowledgement timeout must be positive.");
        }

        return new DeliveryOptions(requireAcknowledgement: true, acknowledgementTimeout: timeout, awaitCapacity: false);
    }

    /// <summary>
    /// Requests that the hub await capacity on the recipient's outbound queue instead of dropping the
    /// message immediately, turning a persistently slow recipient into backpressure on this send rather
    /// than silent loss. Only a direct send to a single recipient honours this — a broadcast or group
    /// send never blocks its whole fan-out on one slow member.
    /// </summary>
    public static DeliveryOptions AwaitingCapacity()
    {
        return new DeliveryOptions(requireAcknowledgement: false, acknowledgementTimeout: null, awaitCapacity: true);
    }

    /// <summary>
    /// Returns a copy of these options with <see cref="AwaitCapacity"/> also set, so a single send can
    /// both require an acknowledgement and await capacity on the recipient's queue.
    /// </summary>
    public DeliveryOptions WithAwaitCapacity()
    {
        return new DeliveryOptions(RequireAcknowledgement, AcknowledgementTimeout, awaitCapacity: true);
    }

    /// <inheritdoc/>
    public bool Equals(DeliveryOptions other)
    {
        return RequireAcknowledgement == other.RequireAcknowledgement
            && AcknowledgementTimeout == other.AcknowledgementTimeout
            && AwaitCapacity == other.AwaitCapacity;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj)
    {
        return obj is DeliveryOptions other && Equals(other);
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        return HashCode.Combine(RequireAcknowledgement, AcknowledgementTimeout, AwaitCapacity);
    }

    /// <summary>
    /// Determines whether two <see cref="DeliveryOptions"/> values are equal.
    /// </summary>
    public static bool operator ==(DeliveryOptions left, DeliveryOptions right)
    {
        return left.Equals(right);
    }

    /// <summary>
    /// Determines whether two <see cref="DeliveryOptions"/> values are not equal.
    /// </summary>
    public static bool operator !=(DeliveryOptions left, DeliveryOptions right)
    {
        return !left.Equals(right);
    }
}
