namespace AdamSalisbury.Meshworx;

/// <summary>
/// A point-in-time snapshot of one group's membership, for administrative inspection via
/// <see cref="IMeshHub.GetGroups"/>.
/// </summary>
/// <param name="Name">The group's name.</param>
/// <param name="MemberIds">The unique identifiers of every client currently a member of the group.</param>
public sealed record GroupInfo(string Name, IReadOnlyList<Guid> MemberIds);
