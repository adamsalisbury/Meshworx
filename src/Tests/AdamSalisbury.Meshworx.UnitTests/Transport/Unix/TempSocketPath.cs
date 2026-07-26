namespace AdamSalisbury.Meshworx.UnitTests.Transport.Unix;

/// <summary>
/// Builds a throwaway filesystem path for a Unix domain socket in the temp directory, short enough to
/// stay under the platform's <c>sun_path</c> length limit.
/// </summary>
internal static class TempSocketPath
{
    internal static string Create()
    {
        return Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.sock");
    }
}
