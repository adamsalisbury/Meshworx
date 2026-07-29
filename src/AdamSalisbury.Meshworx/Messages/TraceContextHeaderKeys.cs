using System.Diagnostics;

namespace AdamSalisbury.Meshworx.Messages;

/// <summary>
/// The W3C Trace Context headers used to carry distributed-tracing context between clients.
/// </summary>
/// <remarks>
/// The keys are the W3C names verbatim — <c>traceparent</c> and <c>tracestate</c> — rather than
/// <c>mesh.</c>-prefixed ones like every other header this library reserves. The prefix exists to keep
/// this library's own headers from colliding with an application's; these two are not this library's
/// headers, they are the interoperable standard's, and a peer bridging Meshworx to HTTP, gRPC or a
/// message broker should find exactly the names it already knows.
/// <para>
/// The hub neither reads nor writes these. It passes the header block through unchanged, as it does for
/// every header it has no behaviour for, so trace context survives the routing hop without the hub
/// participating in the trace at all.
/// </para>
/// </remarks>
internal static class TraceContextHeaderKeys
{
    /// <summary>
    /// The W3C <c>traceparent</c> header: the trace id, parent span id and sampling flags of the
    /// sending side, in the standard <c>00-{trace-id}-{span-id}-{flags}</c> form.
    /// </summary>
    internal const string TraceParent = "traceparent";

    /// <summary>
    /// The W3C <c>tracestate</c> header: vendor-specific trace state accompanying
    /// <see cref="TraceParent"/>. Only written when the sending <see cref="Activity"/> carries one.
    /// </summary>
    internal const string TraceState = "tracestate";

    /// <summary>
    /// Reads the trace context to propagate from an activity, if there is one to propagate.
    /// </summary>
    /// <param name="activity">
    /// The activity whose context should travel with the message — this library's own send span when a
    /// listener created one, or the ambient <see cref="Activity.Current"/> when it did not.
    /// </param>
    /// <param name="traceParent">The <c>traceparent</c> value to write, when this returns true.</param>
    /// <param name="traceState">
    /// The <c>tracestate</c> value to write, or <see langword="null"/> when the activity carries none.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when there is a W3C context to propagate; otherwise
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// Returns false for a null activity — the ordinary case when nothing is listening — and also for
    /// one using the legacy hierarchical id format, whose ids cannot be expressed as a
    /// <c>traceparent</c>. Writing a malformed one would be worse than writing none: the receiver would
    /// fail to parse it and lose the context anyway, having paid for the header.
    /// </remarks>
    internal static bool TryGetTraceContext(
        Activity? activity, out string traceParent, out string? traceState)
    {
        traceParent = string.Empty;
        traceState = null;

        if (activity is null || activity.IdFormat != ActivityIdFormat.W3C || activity.Id is null)
        {
            return false;
        }

        traceParent = activity.Id;
        traceState = activity.TraceStateString;
        return true;
    }

    /// <summary>
    /// Recovers the sending side's trace context from a received message's headers.
    /// </summary>
    /// <param name="headers">The received message's headers.</param>
    /// <param name="context">The recovered context, when this returns true.</param>
    /// <returns>
    /// <see langword="true"/> when the headers carry a well-formed <c>traceparent</c>; otherwise
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// The value comes from a remote peer and is not otherwise validated, so a malformed or hostile one
    /// must do nothing worse than lose the link:
    /// <see cref="ActivityContext.TryParse(string, string, out ActivityContext)"/> is total over its
    /// input and reports failure rather than throwing, and a false return simply means the receiving
    /// span starts a new trace instead of continuing one.
    /// </remarks>
    internal static bool TryExtractTraceContext(MessageHeaders headers, out ActivityContext context)
    {
        context = default;

        if (!headers.TryGetValue(TraceParent, out string? traceParent))
        {
            return false;
        }

        headers.TryGetValue(TraceState, out string? traceState);
        return ActivityContext.TryParse(traceParent, traceState, out context);
    }
}
