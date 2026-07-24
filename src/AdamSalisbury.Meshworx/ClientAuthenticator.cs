namespace AdamSalisbury.Meshworx;

/// <summary>
/// Decides whether a client may register with the hub. Supplied to <see cref="MeshHub"/> so an
/// integrator can reject unauthenticated peers without modifying the library.
/// </summary>
/// <param name="context">The pending registration's name and credential.</param>
/// <param name="cancellationToken">A token that is cancelled if the hub is shutting down.</param>
/// <returns>
/// <see langword="true"/> to admit the client; <see langword="false"/> to refuse it with
/// <see cref="RegistrationErrorCode.AuthenticationFailed"/>.
/// </returns>
/// <remarks>
/// The callback is invoked once per registration, after the name and credential are parsed and before
/// the client is admitted or its name reserved. The <see cref="RegistrationContext.Credential"/> is
/// only guaranteed valid for the duration of the call; copy it if it must outlive the invocation.
/// </remarks>
public delegate ValueTask<bool> ClientAuthenticator(
    RegistrationContext context,
    CancellationToken cancellationToken);
