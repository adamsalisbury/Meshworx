namespace AdamSalisbury.Meshworx.Admin;

/// <summary>
/// The JSON response body for a request refused with a <c>401</c>, or one matching no known route with
/// a <c>404</c>.
/// </summary>
/// <param name="Error">A short, human-readable description of what was wrong with the request.</param>
internal sealed record AdminErrorResponse(string Error);
