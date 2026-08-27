namespace AdamSalisbury.Meshworx.Compression;

/// <summary>
/// Thrown when an algorithm id cannot be resolved to a registered <see cref="ICompressionStrategy"/>.
/// </summary>
/// <remarks>
/// On the receiving side this means the sender compressed a body with an algorithm this endpoint has no
/// strategy for. That is a configuration mismatch between the two endpoints, not a corrupt message, and
/// it is reported as such rather than by handing arbitrary bytes to a strategy that would misread them.
/// </remarks>
public sealed class UnknownCompressionAlgorithmException : Exception
{
    /// <summary>
    /// Initialises a new instance of the <see cref="UnknownCompressionAlgorithmException"/> class.
    /// </summary>
    public UnknownCompressionAlgorithmException()
        : base("No compression strategy is registered for the requested algorithm.")
    {
    }

    /// <summary>
    /// Initialises a new instance of the <see cref="UnknownCompressionAlgorithmException"/> class for a
    /// specific algorithm id.
    /// </summary>
    /// <param name="algorithmId">The algorithm id that could not be resolved.</param>
    /// <param name="registeredAlgorithmIds">The ids that are registered, included in the message to make the mismatch obvious.</param>
    public UnknownCompressionAlgorithmException(string algorithmId, IReadOnlyList<string> registeredAlgorithmIds)
        : base(BuildMessage(algorithmId, registeredAlgorithmIds))
    {
        AlgorithmId = algorithmId;
    }

    /// <summary>
    /// Initialises a new instance of the <see cref="UnknownCompressionAlgorithmException"/> class for an
    /// algorithm a <i>peer</i> has not advertised support for.
    /// </summary>
    /// <param name="algorithmId">The algorithm id the peer cannot read.</param>
    /// <param name="peerId">The peer that has not advertised it.</param>
    /// <param name="peerAlgorithmIds">What the peer did advertise.</param>
    /// <remarks>
    /// The same exception type as the local case on purpose: a caller that named an algorithm wants to
    /// know its message could not be compressed the way it asked, and which side of the connection is
    /// missing the strategy does not change what it has to do about it. The message says which, for
    /// anyone reading a log.
    /// </remarks>
    public UnknownCompressionAlgorithmException(
        string algorithmId, Guid peerId, IReadOnlyList<string> peerAlgorithmIds)
        : base(BuildPeerMessage(algorithmId, peerId, peerAlgorithmIds))
    {
        AlgorithmId = algorithmId;
        PeerId = peerId;
    }

    /// <summary>
    /// Initialises a new instance of the <see cref="UnknownCompressionAlgorithmException"/> class with a
    /// custom message.
    /// </summary>
    /// <param name="message">A human-readable description of the error.</param>
    public UnknownCompressionAlgorithmException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initialises a new instance of the <see cref="UnknownCompressionAlgorithmException"/> class with a
    /// custom message and an inner exception.
    /// </summary>
    /// <param name="message">A human-readable description of the error.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public UnknownCompressionAlgorithmException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Gets the algorithm id that could not be resolved, or <see langword="null"/> if this instance was
    /// not created for a specific id.
    /// </summary>
    public string? AlgorithmId { get; }

    /// <summary>
    /// Gets the peer that had not advertised support for <see cref="AlgorithmId"/>, or
    /// <see langword="null"/> when it was this endpoint that held no strategy for it.
    /// </summary>
    public Guid? PeerId { get; }

    private static string BuildMessage(string algorithmId, IReadOnlyList<string> registeredAlgorithmIds)
    {
        ArgumentNullException.ThrowIfNull(registeredAlgorithmIds);

        string registered = registeredAlgorithmIds.Count == 0
            ? "none"
            : string.Join(", ", registeredAlgorithmIds);

        return $"No compression strategy is registered for algorithm '{algorithmId}'. Registered: {registered}.";
    }

    private static string BuildPeerMessage(string algorithmId, Guid peerId, IReadOnlyList<string> peerAlgorithmIds)
    {
        ArgumentNullException.ThrowIfNull(peerAlgorithmIds);

        string advertised = peerAlgorithmIds.Count == 0
            ? "none"
            : string.Join(", ", peerAlgorithmIds);

        return $"Client {peerId} has not advertised support for compression algorithm '{algorithmId}', "
            + $"so it could not read a message compressed with it. It advertised: {advertised}.";
    }
}
