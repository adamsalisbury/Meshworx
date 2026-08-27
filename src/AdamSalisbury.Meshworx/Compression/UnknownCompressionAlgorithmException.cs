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

    private static string BuildMessage(string algorithmId, IReadOnlyList<string> registeredAlgorithmIds)
    {
        ArgumentNullException.ThrowIfNull(registeredAlgorithmIds);

        string registered = registeredAlgorithmIds.Count == 0
            ? "none"
            : string.Join(", ", registeredAlgorithmIds);

        return $"No compression strategy is registered for algorithm '{algorithmId}'. Registered: {registered}.";
    }
}
