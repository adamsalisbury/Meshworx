namespace AdamSalisbury.Meshworx;

/// <summary>
/// A point-in-time snapshot of one topic subscription pattern, for administrative inspection via
/// <see cref="IMeshHub.GetTopics"/>.
/// </summary>
/// <param name="Pattern">The subscription pattern, which may contain <c>+</c>/<c>#</c> wildcard segments.</param>
/// <param name="SubscriberIds">The unique identifiers of every client currently holding this subscription.</param>
public sealed record TopicSubscriptionInfo(string Pattern, IReadOnlyList<Guid> SubscriberIds);
