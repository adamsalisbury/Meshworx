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
    private DeliveryOptions(bool requireAcknowledgement, TimeSpan? acknowledgementTimeout)
    {
        RequireAcknowledgement = requireAcknowledgement;
        AcknowledgementTimeout = acknowledgementTimeout;
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

        return new DeliveryOptions(requireAcknowledgement: true, acknowledgementTimeout: timeout);
    }

    /// <inheritdoc/>
    public bool Equals(DeliveryOptions other)
    {
        return RequireAcknowledgement == other.RequireAcknowledgement
            && AcknowledgementTimeout == other.AcknowledgementTimeout;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj)
    {
        return obj is DeliveryOptions other && Equals(other);
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        return HashCode.Combine(RequireAcknowledgement, AcknowledgementTimeout);
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
