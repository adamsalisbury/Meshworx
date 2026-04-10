namespace AdamSalisbury.Meshworx;

/// <summary>
/// Thrown when the hub refuses a client registration request.
/// </summary>
public sealed class RegistrationRefusedException : Exception
{
    /// <summary>
    /// Initialises a new instance of the <see cref="RegistrationRefusedException"/> class.
    /// </summary>
    public RegistrationRefusedException()
        : base("Registration refused by the hub.")
    {
    }

    /// <summary>
    /// Initialises a new instance of the <see cref="RegistrationRefusedException"/> class.
    /// </summary>
    /// <param name="errorCode">The reason the registration was refused.</param>
    public RegistrationRefusedException(RegistrationErrorCode errorCode)
        : base($"Registration refused by the hub. Error code: {errorCode}.")
    {
        ErrorCode = errorCode;
    }

    /// <summary>
    /// Initialises a new instance of the <see cref="RegistrationRefusedException"/> class with a custom message.
    /// </summary>
    /// <param name="message">A human-readable description of the error.</param>
    public RegistrationRefusedException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initialises a new instance of the <see cref="RegistrationRefusedException"/> class with a custom message and inner exception.
    /// </summary>
    /// <param name="message">A human-readable description of the error.</param>
    /// <param name="innerException">The exception that caused this exception.</param>
    public RegistrationRefusedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Gets the error code indicating why registration was refused.
    /// </summary>
    public RegistrationErrorCode ErrorCode { get; }
}
