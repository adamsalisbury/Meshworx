namespace AdamSalisbury.Meshworx.UnitTests;

/// <summary>
/// The timeout budgets shared across the test suite.
/// </summary>
/// <remarks>
/// <para>
/// Two distinct timeouts govern an asynchronous test, and they must not compete:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// <see cref="Wait"/> is the <em>diagnostic</em> timeout. A test awaits an expected handoff — a
/// <see cref="TaskCompletionSource"/> signalled from a callback, a frame arriving at a mock transport — for
/// this long before declaring that the handoff never happened. Exceeding it produces a
/// <see cref="TimeoutException"/> naming the wait that failed, which is what makes a red test diagnosable.
/// </description>
/// </item>
/// <item>
/// <description>
/// <see cref="Harness"/> (and <see cref="ExtendedHarness"/>) is the <em>backstop</em>, applied as
/// <c>[Fact(Timeout = …)]</c>. It exists only to kill a test that has wedged somewhere no diagnostic wait
/// covers, so that a deadlock fails the run instead of hanging it.
/// </description>
/// </item>
/// </list>
/// <para>
/// The backstop must therefore be comfortably larger than the longest chain of diagnostic waits a test can
/// perform. Where it is not, the harness wins every race: the test dies on a bare "execution timed out"
/// with no indication of which handoff was missing, and — because the backstop is wall-clock time on a
/// shared CI runner rather than a measure of the code under test — it does so intermittently, whenever
/// another test happens to be saturating the machine. That is a false red, and it is indistinguishable
/// from a real one.
/// </para>
/// <para>
/// When adding a test, count the sequential waits in its body and pick the backstop that clears them with
/// headroom to spare. Tests that perform no waiting at all need neither constant and should keep a tight
/// literal budget.
/// </para>
/// </remarks>
internal static class TestTimeouts
{
    /// <summary>
    /// The time to wait for a single expected asynchronous handoff before treating it as failed, in milliseconds.
    /// </summary>
    public const int WaitMilliseconds = 5000;

    /// <summary>
    /// The harness backstop for a test performing a short chain of <see cref="Wait"/>-bounded handoffs,
    /// in milliseconds.
    /// </summary>
    public const int Harness = 3 * WaitMilliseconds;

    /// <summary>
    /// The harness backstop for a test performing a long chain of <see cref="Wait"/>-bounded handoffs, or one
    /// driving a real loopback transport, in milliseconds.
    /// </summary>
    public const int ExtendedHarness = 6 * WaitMilliseconds;

    /// <summary>
    /// The time to wait for a single expected asynchronous handoff before treating it as failed.
    /// </summary>
    public static readonly TimeSpan Wait = TimeSpan.FromMilliseconds(WaitMilliseconds);
}
