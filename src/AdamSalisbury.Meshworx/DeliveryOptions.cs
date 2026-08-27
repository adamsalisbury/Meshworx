using AdamSalisbury.Meshworx.Messages;

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
    private DeliveryOptions(
        bool requireAcknowledgement,
        TimeSpan? acknowledgementTimeout,
        bool awaitCapacity,
        MessagePriority priority,
        bool compress = false,
        string? compressionAlgorithmId = null)
    {
        RequireAcknowledgement = requireAcknowledgement;
        AcknowledgementTimeout = acknowledgementTimeout;
        AwaitCapacity = awaitCapacity;
        Priority = priority;
        Compress = compress;
        CompressionAlgorithmId = compressionAlgorithmId;
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
    /// <remarks>
    /// Honoured only for a directly addressed send. While the hub waits it stops reading further frames
    /// from this client, so anything else this client sends meanwhile, including to entirely unrelated
    /// recipients, waits behind it. Use it for traffic that genuinely must not be lost, not as a blanket
    /// default. See <see cref="AwaitingCapacity"/> for why the send itself does not wait.
    /// </remarks>
    public bool AwaitCapacity { get; }

    /// <summary>
    /// Gets the priority this send should be queued at on the recipient's outbound queue.
    /// <see cref="MessagePriority.Normal"/> is the default, giving exactly the queueing behaviour that
    /// existed before priority lanes did.
    /// </summary>
    public MessagePriority Priority { get; }

    /// <summary>
    /// Gets a value indicating whether the sender asked for this message's body to be compressed before
    /// it goes on the wire.
    /// </summary>
    /// <remarks>
    /// A request, not a guarantee. A body below <c>256</c> bytes is sent as-is without an attempt, and a
    /// body that does not actually get smaller is sent uncompressed too — opting in can therefore never
    /// make a message larger. Either way the recipient sees the same bytes it would have seen without
    /// this set.
    /// </remarks>
    public bool Compress { get; }

    /// <summary>
    /// Gets the id of the compression algorithm to use, or <see langword="null"/> to use the best one
    /// this client has registered.
    /// </summary>
    /// <remarks>
    /// Resolved against the sending client's own <see cref="IMeshClient.CompressionStrategies"/>. A named
    /// algorithm that is not registered there throws before anything is sent; <see langword="null"/>
    /// takes the first registered strategy, which is why registration order is preference order, and
    /// falls back to sending uncompressed if there are none.
    /// </remarks>
    public string? CompressionAlgorithmId { get; }

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

        return new DeliveryOptions(
            requireAcknowledgement: true, acknowledgementTimeout: timeout, awaitCapacity: false, priority: default);
    }

    /// <summary>
    /// Requests that the hub await capacity on the recipient's outbound queue instead of dropping the
    /// message immediately, so a message addressed to a momentarily saturated recipient is delivered late
    /// rather than lost. Only a direct send to a single recipient honours this — a broadcast or group
    /// send never blocks its whole fan-out on one slow member.
    /// </summary>
    /// <remarks>
    /// This does <b>not</b> make the returned task wait for capacity. On its own, the send completes as
    /// soon as the frame reaches the transport, exactly like every other fire-and-forget overload — the
    /// waiting happens hub-side, out of the caller's sight. Producer-side throttling is therefore
    /// indirect: while the hub is parked it stops reading this connection, so a producer sending
    /// continuously eventually blocks on its own transport once the socket's buffers fill, rather than at
    /// the first saturated send. To have the call itself wait until the message has genuinely reached the
    /// recipient, combine this with <see cref="RequireAck"/> via <see cref="WithAwaitCapacity"/> and read
    /// that method's remarks first.
    /// </remarks>
    public static DeliveryOptions AwaitingCapacity()
    {
        return new DeliveryOptions(
            requireAcknowledgement: false, acknowledgementTimeout: null, awaitCapacity: true, priority: default);
    }

    /// <summary>
    /// Requests that this send be queued at the given <see cref="MessagePriority"/> on the recipient's
    /// outbound queue, so it can overtake a backlog of lower-priority traffic already queued for the same
    /// recipient.
    /// </summary>
    /// <param name="priority">The priority to queue this send at.</param>
    public static DeliveryOptions AtPriority(MessagePriority priority)
    {
        return new DeliveryOptions(
            requireAcknowledgement: false, acknowledgementTimeout: null, awaitCapacity: false, priority: priority);
    }

    /// <summary>
    /// Requests that this send's body be compressed with the best algorithm this client has registered,
    /// leaving the recipient to decompress it before its application sees it.
    /// </summary>
    /// <remarks>
    /// "Best" means the first strategy registered with the sending client's
    /// <see cref="IMeshClient.CompressionStrategies"/>, which by default is Brotli. If that registry is
    /// empty the message is sent uncompressed rather than failing: the caller asked for compression, not
    /// for the send to depend on it. Name an algorithm with <see cref="Compressed(string)"/> when the
    /// choice actually matters.
    /// </remarks>
    public static DeliveryOptions Compressed()
    {
        return new DeliveryOptions(
            requireAcknowledgement: false,
            acknowledgementTimeout: null,
            awaitCapacity: false,
            priority: default,
            compress: true);
    }

    /// <summary>
    /// Requests that this send's body be compressed with a specific registered algorithm.
    /// </summary>
    /// <param name="algorithmId">
    /// The id of a strategy registered with the sending client's
    /// <see cref="IMeshClient.CompressionStrategies"/>.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="algorithmId"/> is empty or whitespace.</exception>
    /// <remarks>
    /// Unlike <see cref="Compressed()"/>, this fails rather than falling back: an algorithm the sending
    /// client has no strategy for throws a
    /// <see cref="Compression.UnknownCompressionAlgorithmException"/> at send time, before anything goes
    /// on the wire. Naming an algorithm is taken as a requirement, where asking for the best available is
    /// taken as a preference.
    /// </remarks>
    public static DeliveryOptions Compressed(string algorithmId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(algorithmId);

        return new DeliveryOptions(
            requireAcknowledgement: false,
            acknowledgementTimeout: null,
            awaitCapacity: false,
            priority: default,
            compress: true,
            compressionAlgorithmId: algorithmId);
    }

    /// <summary>
    /// Returns a copy of these options with compression also requested, so a single send can combine it
    /// with an acknowledgement, a priority and/or an await-capacity request.
    /// </summary>
    /// <param name="algorithmId">
    /// The id of a registered strategy, or <see langword="null"/> for the best available. Carries the
    /// same fail-fast versus fall-back distinction as <see cref="Compressed(string)"/> and
    /// <see cref="Compressed()"/>.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="algorithmId"/> is non-null but empty or whitespace.</exception>
    public DeliveryOptions WithCompression(string? algorithmId = null)
    {
        if (algorithmId is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(algorithmId);
        }

        return new DeliveryOptions(
            RequireAcknowledgement, AcknowledgementTimeout, AwaitCapacity, Priority, compress: true, algorithmId);
    }

    /// <summary>
    /// Returns a copy of these options with <see cref="AwaitCapacity"/> also set, so a single send can
    /// both require an acknowledgement and await capacity on the recipient's queue.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="AwaitingCapacity"/> alone, this genuinely blocks the caller until the recipient
    /// acknowledges — but the two waits are timed independently and must be reconciled by the caller.
    /// <see cref="AcknowledgementTimeout"/> is measured by this client, while the hub's own wait for
    /// capacity is bounded by its <c>backpressureAwaitTimeout</c> (30 seconds by default). Set the
    /// acknowledgement timeout <b>longer</b> than the hub's: if it expires first, the send fails with a
    /// <see cref="TimeoutException"/> while the hub is still waiting, and the message may be delivered
    /// afterwards regardless — so a caller that retries on that timeout, which is the pattern
    /// <see cref="RequireAck"/> exists to support, would deliver it twice.
    /// </remarks>
    public DeliveryOptions WithAwaitCapacity()
    {
        return new DeliveryOptions(
            RequireAcknowledgement, AcknowledgementTimeout, awaitCapacity: true, Priority, Compress, CompressionAlgorithmId);
    }

    /// <summary>
    /// Returns a copy of these options with <see cref="Priority"/> also set, so a single send can
    /// combine a priority with an acknowledgement and/or an await-capacity request.
    /// </summary>
    /// <param name="priority">The priority to queue this send at.</param>
    public DeliveryOptions WithPriority(MessagePriority priority)
    {
        return new DeliveryOptions(
            RequireAcknowledgement, AcknowledgementTimeout, AwaitCapacity, priority, Compress, CompressionAlgorithmId);
    }

    /// <inheritdoc/>
    public bool Equals(DeliveryOptions other)
    {
        return RequireAcknowledgement == other.RequireAcknowledgement
            && AcknowledgementTimeout == other.AcknowledgementTimeout
            && AwaitCapacity == other.AwaitCapacity
            && Priority == other.Priority
            && Compress == other.Compress
            && string.Equals(CompressionAlgorithmId, other.CompressionAlgorithmId, StringComparison.OrdinalIgnoreCase);
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj)
    {
        return obj is DeliveryOptions other && Equals(other);
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        return HashCode.Combine(
            RequireAcknowledgement,
            AcknowledgementTimeout,
            AwaitCapacity,
            Priority,
            Compress,
            CompressionAlgorithmId is null ? 0 : StringComparer.OrdinalIgnoreCase.GetHashCode(CompressionAlgorithmId));
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
