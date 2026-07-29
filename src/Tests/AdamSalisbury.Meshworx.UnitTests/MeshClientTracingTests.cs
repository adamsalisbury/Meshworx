using System.Buffers.Binary;
using System.Diagnostics;
using AdamSalisbury.Meshworx.Diagnostics;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.UnitTests.Fixtures;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests;

public class MeshClientTracingTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Acceptance criterion: trace context survives a direct send. The sending client writes a W3C
    /// traceparent into the header block, carrying the trace id of the span the send happened inside.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_WithListenerAttached_WritesTraceParentHeader()
    {
        using var listener = new RecordingActivityListener();
        var fixture = new MeshClientFixture();
        byte[]? sentFrame = null;

        fixture.SetupSuccessfulRegistration();
        await fixture.ConnectAsync();

        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) => sentFrame = frame.ToArray())
            .Returns(Task.CompletedTask);

        await fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1, 2, 3 });

        MessageHeaders headers = ReadDirectSendHeaders(sentFrame!);

        Assert.True(headers.TryGetValue("traceparent", out string? traceParent));
        Assert.True(ActivityContext.TryParse(traceParent, null, out ActivityContext context));

        // The context on the wire is the send span's own, so a receiver continues this exact trace.
        // Matched rather than asserted to be the only one: attaching a listener switches span creation
        // on for the whole process, so a test class running in parallel contributes spans here too.
        Assert.Contains(
            listener.Started,
            a => a.OperationName == "Meshworx.Send" && a.TraceId == context.TraceId);
    }

    /// <summary>
    /// Acceptance criterion: trace context survives a group send too, not only a direct one.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendToGroupAsync_WithListenerAttached_WritesTraceParentHeader()
    {
        using var listener = new RecordingActivityListener();
        var fixture = new MeshClientFixture();
        byte[]? sentFrame = null;

        fixture.SetupSuccessfulRegistration();
        await fixture.ConnectAsync();

        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) => sentFrame = frame.ToArray())
            .Returns(Task.CompletedTask);

        await fixture.Client.SendToGroupAsync("news", new byte[] { 9 });

        Assert.Equal((byte)MessageType.GroupMessageWithHeaders, sentFrame![0]);

        int nameLength = BinaryPrimitives.ReadUInt16BigEndian(sentFrame.AsSpan(1, 2));
        int headerLengthOffset = 3 + nameLength;
        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(sentFrame.AsSpan(headerLengthOffset, 2));
        MessageHeaders headers = HeaderEnvelope.Read(
            sentFrame.AsSpan(headerLengthOffset + 2, headerLength), headerLength);

        Assert.True(headers.ContainsKey("traceparent"));
    }

    /// <summary>
    /// Acceptance criterion: with no listener registered there is no span and no header — the frame is
    /// byte-for-byte the header-free one sent before tracing existed.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_WithNoListener_SendsHeaderFreeFrame()
    {
        var fixture = new MeshClientFixture();
        byte[]? sentFrame = null;
        var recipientId = Guid.NewGuid();

        fixture.SetupSuccessfulRegistration();
        await fixture.ConnectAsync();

        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) => sentFrame = frame.ToArray())
            .Returns(Task.CompletedTask);

        await fixture.Client.SendAsync(recipientId, new byte[] { 1, 2, 3 });

        // SendMessage, not SendMessageWithHeaders: no header block was written at all.
        Assert.Equal((byte)MessageType.SendMessage, sentFrame![0]);
        Assert.Equal(1 + 16 + 3, sentFrame.Length);
    }

    /// <summary>
    /// A send made inside an application's own span joins that trace even though nothing is listening to
    /// this library's source specifically — the ambient Activity.Current is what a caller instrumenting
    /// its own code would expect a message to inherit.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_InsideAmbientActivity_PropagatesThatTrace()
    {
        using var listener = new RecordingActivityListener("SomeApplication");
        using var applicationSource = new ActivitySource("SomeApplication");
        var fixture = new MeshClientFixture();
        byte[]? sentFrame = null;

        fixture.SetupSuccessfulRegistration();
        await fixture.ConnectAsync();

        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) => sentFrame = frame.ToArray())
            .Returns(Task.CompletedTask);

        using (Activity? outer = applicationSource.StartActivity("HandleOrder"))
        {
            Assert.NotNull(outer);
            await fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 });

            MessageHeaders headers = ReadDirectSendHeaders(sentFrame!);
            Assert.True(headers.TryGetValue("traceparent", out string? traceParent));
            Assert.True(ActivityContext.TryParse(traceParent, null, out ActivityContext context));
            Assert.Equal(outer.TraceId, context.TraceId);
        }
    }

    /// <summary>
    /// Tracing must never break delivery. A connection that negotiated below the header-envelope minimum
    /// cannot carry a header block at all, so the context is dropped and the message still goes — rather
    /// than the send starting to throw the moment a listener is attached.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_OnPreHeaderEnvelopePeer_DropsContextRatherThanThrowing()
    {
        using var listener = new RecordingActivityListener();
        var fixture = new MeshClientFixture();
        byte[]? sentFrame = null;

        fixture.SetupSuccessfulRegistrationWithNegotiatedVersion(
            (byte)(Protocol.HeaderEnvelopeMinVersion - 1));

        // Connect through the client directly: MeshClientFixture.ConnectAsync re-runs
        // SetupSuccessfulRegistration with its defaults, which would replace the negotiated version
        // pinned above.
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        fixture.Transport
            .Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((frame, _) => sentFrame = frame.ToArray())
            .Returns(Task.CompletedTask);

        await fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 7 });

        Assert.Equal((byte)MessageType.SendMessage, sentFrame![0]);
        Assert.Equal(1 + 16 + 1, sentFrame.Length);
    }

    /// <summary>
    /// The receiving side continues the sender's trace rather than starting an unrelated one — the whole
    /// point of propagation, and the half that makes cross-client causality visible.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task Receive_WithTraceParentHeader_StartsConsumerSpanInSenderTrace()
    {
        using var listener = new RecordingActivityListener();
        var senderId = Guid.NewGuid();
        var traceId = ActivityTraceId.CreateRandom();
        var spanId = ActivitySpanId.CreateRandom();
        string traceParent = $"00-{traceId}-{spanId}-01";

        var headers = new MessageHeaders(
            new Dictionary<string, string> { ["traceparent"] = traceParent });

        var fixture = new MeshClientFixture();
        var received = new TaskCompletionSource();
        fixture.SetupSuccessfulRegistration(
            MeshClientFixture.CreateDeliverMessageWithHeadersPayload(senderId, headers, [42]));

        fixture.Client.MessageReceived += (_, _) => received.TrySetResult();

        // Connect directly: the fixture's own ConnectAsync re-runs SetupSuccessfulRegistration with no
        // scripted frames, which would discard the delivery above.
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");
        await received.Task.WaitAsync(WaitTimeout);

        // Identified by this test's own sender id: other test classes running in parallel raise
        // Meshworx.Receive spans of their own once a listener is attached.
        Activity consumer = await listener.WaitForStoppedAsync(
            a => a.OperationName == "Meshworx.Receive"
                && (Guid?)a.GetTagItem("meshworx.sender_id") == senderId,
            WaitTimeout);

        Assert.Equal(ActivityKind.Consumer, consumer.Kind);
        Assert.Equal(traceId, consumer.TraceId);
        Assert.Equal(spanId, consumer.ParentSpanId);
    }

    /// <summary>
    /// A malformed traceparent from a remote peer costs the link, not the delivery: the message is still
    /// raised, under a span that simply starts a new trace.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task Receive_WithMalformedTraceParent_StillDeliversMessage()
    {
        using var listener = new RecordingActivityListener();
        var senderId = Guid.NewGuid();
        var headers = new MessageHeaders(
            new Dictionary<string, string> { ["traceparent"] = "not-a-traceparent" });

        var fixture = new MeshClientFixture();
        var received = new TaskCompletionSource<byte[]>();
        fixture.SetupSuccessfulRegistration(
            MeshClientFixture.CreateDeliverMessageWithHeadersPayload(senderId, headers, [42]));

        fixture.Client.MessageReceived += (_, e) => received.TrySetResult(e.Data.ToArray());
        await fixture.Client.ConnectAsync(fixture.Transport.Object, "TestClient");

        Assert.Equal([42], await received.Task.WaitAsync(WaitTimeout));
    }

    /// <summary>
    /// The trace headers are reserved, like every other header a built-in helper writes: an application
    /// setting one by hand would otherwise have it silently replaced on a traced send, or kept — putting
    /// a stale trace id on a message belonging to an entirely different operation.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task SendAsync_WithCallerSuppliedTraceParent_Throws()
    {
        var fixture = new MeshClientFixture();
        fixture.SetupSuccessfulRegistration();
        await fixture.ConnectAsync();

        var headers = new MessageHeaders(
            new Dictionary<string, string> { ["traceparent"] = "00-x-y-01" });

        await Assert.ThrowsAsync<ArgumentException>(
            () => fixture.Client.SendAsync(Guid.NewGuid(), new byte[] { 1 }, headers));
    }

    private static MessageHeaders ReadDirectSendHeaders(byte[] frame)
    {
        Assert.Equal((byte)MessageType.SendMessageWithHeaders, frame[0]);

        int headerLength = BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(17, 2));
        return HeaderEnvelope.Read(frame.AsSpan(19, headerLength), headerLength);
    }

    /// <summary>
    /// Subscribes to an <see cref="ActivitySource"/> by name and records what it produces. Attaching one
    /// of these is exactly the opt-in that switches tracing on; without it the library allocates no
    /// spans and writes no headers.
    /// </summary>
    private sealed class RecordingActivityListener : IDisposable
    {
        private readonly ActivityListener _listener;
        private readonly List<Activity> _started = [];
        private readonly List<Activity> _stopped = [];
        private readonly Lock _lock = new();

        public RecordingActivityListener(string sourceName = MeshworxActivitySource.Name)
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == sourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = activity =>
                {
                    lock (_lock)
                    {
                        _started.Add(activity);
                    }
                },
                ActivityStopped = activity =>
                {
                    lock (_lock)
                    {
                        _stopped.Add(activity);
                    }
                },
            };

            ActivitySource.AddActivityListener(_listener);
        }

        public IReadOnlyList<Activity> Started
        {
            get
            {
                lock (_lock)
                {
                    return [.. _started];
                }
            }
        }

        public async Task<Activity> WaitForStoppedAsync(Func<Activity, bool> predicate, TimeSpan timeout)
        {
            DateTime deadline = DateTime.UtcNow + timeout;

            while (DateTime.UtcNow < deadline)
            {
                lock (_lock)
                {
                    Activity? match = _stopped.Find(a => predicate(a));
                    if (match is not null)
                    {
                        return match;
                    }
                }

                await Task.Delay(10).ConfigureAwait(false);
            }

            throw new TimeoutException("No stopped activity matching the predicate was recorded.");
        }

        public void Dispose()
        {
            _listener.Dispose();
        }
    }
}
