using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx;

/// <summary>
/// A per-client outbound queue split into three fixed priority lanes (<see cref="MessagePriority.High"/>,
/// <see cref="MessagePriority.Normal"/> and <see cref="MessagePriority.Low"/>), sharing a single capacity
/// bound across all three so replacing the previous single <see cref="Channel{T}"/> with lanes does not
/// raise the worst-case memory a saturated client can hold.
/// </summary>
/// <remarks>
/// Each lane is an unbounded <see cref="Channel{T}"/> so it never itself rejects a write; capacity is
/// instead enforced once, up front, by <see cref="_capacityGate"/> — a slot is claimed from the gate
/// before a frame is written to any lane, and given back once <see cref="ReadAllAsync"/> has yielded it.
/// A caller that fills every slot with <see cref="MessagePriority.Normal"/> frames, exactly as the single
/// pre-lane queue's capacity test did, still sees the (total capacity + 1)th write refused — the capacity
/// guarantee callers already depend on is unchanged.
/// </remarks>
internal sealed class PriorityOutboundQueue : IDisposable
{
    // High and normal lanes are drained in bounded bursts before the loop always gives the low lane a
    // turn, so a sustained flood of high/normal traffic still services low-priority frames on a bounded
    // cycle rather than starving them indefinitely — the anti-starvation guarantee the issue asks for.
    private const int HighBurstLimit = 8;
    private const int NormalBurstLimit = 4;

    private readonly SemaphoreSlim _capacityGate;

    // A single wake-up signal covering all three lanes, released once per successful write and once by
    // Complete. Lets ReadAllAsync park on one await rather than racing three lanes' WaitToReadAsync calls,
    // which had to be promoted from ValueTask to Task to be passed to Task.WhenAny — several heap
    // allocations on every wait, and the wait branch is reached on ordinary traffic, not just when idle.
    private readonly SemaphoreSlim _readySignal = new(0);
    private readonly Channel<byte[]> _highLane = CreateLane();
    private readonly Channel<byte[]> _normalLane = CreateLane();
    private readonly Channel<byte[]> _lowLane = CreateLane();
    private readonly CancellationTokenSource _disposalCts = new();

    public PriorityOutboundQueue(int capacity)
    {
        _capacityGate = new SemaphoreSlim(capacity, capacity);
        Capacity = capacity;
    }

    /// <summary>
    /// The combined capacity shared across all three lanes.
    /// </summary>
    public int Capacity { get; }

    /// <summary>
    /// The number of frames currently queued, summed across every lane.
    /// </summary>
    public int Count => Capacity - _capacityGate.CurrentCount;

    /// <summary>
    /// A token cancelled once this queue is torn down, so a sender parked in
    /// <see cref="TryEnqueueAsync"/> waiting for capacity on a recipient that then disconnects is released
    /// rather than waiting out its full timeout for a slot that will never free up again.
    /// </summary>
    public CancellationToken DisposalToken => _disposalCts.Token;

    private static Channel<byte[]> CreateLane()
    {
        return Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
    }

    private Channel<byte[]> LaneFor(MessagePriority priority)
    {
        return priority switch
        {
            MessagePriority.High => _highLane,
            MessagePriority.Low => _lowLane,
            _ => _normalLane,
        };
    }

    /// <summary>
    /// Queues a frame on the given priority's lane if a capacity slot is free; otherwise drops it
    /// immediately without waiting. Mirrors a bounded <see cref="Channel{T}"/>'s own <c>TryWrite</c>.
    /// </summary>
    public bool TryEnqueue(MessagePriority priority, byte[] frame)
    {
        if (!_capacityGate.Wait(0))
        {
            return false;
        }

        // The lane itself is unbounded, so this can only fail if the lane's writer has already been
        // completed by Dispose — which happens after cancelling _disposalCts, so a concurrent caller
        // that got this far is racing teardown, not a capacity problem. Give the claimed slot back rather
        // than leaking it against a queue nothing will ever drain again.
        if (LaneFor(priority).Writer.TryWrite(frame))
        {
            // Signalled only after the frame is in its lane, so a reader woken by this credit is
            // guaranteed to see the frame when it rechecks the lanes.
            _readySignal.Release();
            return true;
        }

        _capacityGate.Release();
        return false;
    }

    /// <summary>
    /// Queues a frame on the given priority's lane, waiting up to <paramref name="timeout"/> for a
    /// capacity slot to free up if every slot is currently claimed.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if the frame was queued before the timeout elapsed, the caller's token was
    /// cancelled, or this queue was disposed; otherwise <see langword="false"/>.
    /// </returns>
    public async Task<bool> TryEnqueueAsync(
        MessagePriority priority, byte[] frame, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using CancellationTokenSource linkedCts =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, DisposalToken);
        linkedCts.CancelAfter(timeout);

        try
        {
            await _capacityGate.WaitAsync(linkedCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // Either the timeout elapsed or this queue was disposed while waiting; either way there is
            // no slot to write into. The caller falls back to its ordinary drop-on-full path.
            return false;
        }

        if (LaneFor(priority).Writer.TryWrite(frame))
        {
            _readySignal.Release();
            return true;
        }

        _capacityGate.Release();
        return false;
    }

    /// <summary>
    /// Takes one already-queued frame, if any, in priority order — high, then normal, then low — without
    /// waiting. Used to opportunistically coalesce whatever else is already sitting in the queue into the
    /// same network write as the frame <see cref="ReadAllAsync"/> just yielded, so a burst of traffic
    /// still leaves in strict priority order rather than plain arrival order.
    /// </summary>
    public bool TryDequeue([NotNullWhen(true)] out byte[]? frame)
    {
        if (_highLane.Reader.TryRead(out frame) || _normalLane.Reader.TryRead(out frame) || _lowLane.Reader.TryRead(out frame))
        {
            _capacityGate.Release();
            return true;
        }

        frame = null;
        return false;
    }

    /// <summary>
    /// Drains every lane in priority order — high, then normal, then low — with anti-starvation bursts,
    /// yielding one frame at a time until every lane is completed and empty or <paramref name="cancellationToken"/>
    /// is triggered. Releases the capacity slot each yielded frame held the instant it is handed back, so
    /// a writer waiting in <see cref="TryEnqueueAsync"/> can claim it as soon as the caller has the frame
    /// in hand, not only once the caller has finished processing it.
    /// </summary>
    /// <remarks>
    /// The high lane is only rechecked once the current pass's normal burst and single low check have
    /// both run their course — because this is an iterator, resuming after a <c>yield return</c> continues
    /// exactly where the method paused rather than restarting the outer loop, so a high-priority frame
    /// enqueued while a pass is already part-way through servicing the normal lane can wait behind up to
    /// the remainder of that burst (bounded by <see cref="NormalBurstLimit"/>) before it is recognised.
    /// This bound is small and constant regardless of backlog size, so it does not undermine "overtakes a
    /// backlog of bulk traffic" for any backlog worth the name — but it is not a zero-frame guarantee for
    /// the specific instant a high-priority frame arrives.
    /// </remarks>
    public async IAsyncEnumerable<byte[]> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (true)
        {
            // Checked at the top of every iteration, not just inside the wait branch below: a queue with
            // frames always ready never reaches that branch, so without this check a cancelled token
            // would go unobserved for as long as the traffic kept up, instead of propagating the way the
            // original Channel{T}.Reader.ReadAllAsync did.
            cancellationToken.ThrowIfCancellationRequested();

            bool servicedAny = false;

            for (int taken = 0; taken < HighBurstLimit && _highLane.Reader.TryRead(out byte[]? highFrame); taken++)
            {
                _capacityGate.Release();
                servicedAny = true;
                yield return highFrame;
            }

            for (int taken = 0; taken < NormalBurstLimit && _normalLane.Reader.TryRead(out byte[]? normalFrame); taken++)
            {
                _capacityGate.Release();
                servicedAny = true;
                yield return normalFrame;
            }

            if (_lowLane.Reader.TryRead(out byte[]? lowFrame))
            {
                _capacityGate.Release();
                servicedAny = true;
                yield return lowFrame;
            }

            if (servicedAny)
            {
                // Something was ready this cycle; loop straight back round rather than waiting, in case
                // more is already queued.
                continue;
            }

            if (_highLane.Reader.Completion.IsCompleted
                && _normalLane.Reader.Completion.IsCompleted
                && _lowLane.Reader.Completion.IsCompleted)
            {
                yield break;
            }

            // Nothing in any lane right now: park on the shared signal until an enqueue — on any lane — or
            // Complete releases a credit. This branch is reached whenever nothing arrived between one
            // MoveNextAsync and the next, which includes ordinary single-frame delivery with no backlog
            // behind it, so it stays on a single await rather than allocating a wait per lane.
            await _readySignal.WaitAsync(cancellationToken).ConfigureAwait(false);

            // Every enqueue since the last park left a credit behind, so take the surplus with the one
            // just consumed: the next pass rechecks all three lanes from scratch regardless, and leaving
            // stale credits would send this branch straight back round for each of them in turn. Draining
            // before the lanes are rechecked cannot lose a wake-up, because a credit is only ever released
            // after its frame is already in its lane.
            // No token: a zero-timeout take never blocks, so there is nothing for cancellation to cut short.
            while (_readySignal.Wait(0, CancellationToken.None))
            {
            }
        }
    }

    /// <summary>
    /// Completes every lane's writer, so a pending <see cref="ReadAllAsync"/> finishes once each is
    /// drained and a pending <see cref="TryEnqueueAsync"/> is released rather than waiting out its
    /// timeout.
    /// </summary>
    public void Complete()
    {
        _highLane.Writer.TryComplete();
        _normalLane.Writer.TryComplete();
        _lowLane.Writer.TryComplete();

        // Wake a reader parked on the shared signal so it observes the completed lanes and finishes,
        // rather than waiting on a credit no further enqueue will ever release.
        _readySignal.Release();

        // Cancel only, never dispose here: a concurrent TryEnqueueAsync call may still be reading
        // DisposalToken to build its linked token source, and disposing this source underneath that
        // read would turn a graceful release into an ObjectDisposedException instead. Both this token
        // source and the capacity gate are actually released by Dispose(), called once the owning
        // connection has fully torn down.
        _disposalCts.Cancel();
    }

    /// <summary>
    /// Releases the capacity gate and the disposal token source. Must only be called once the owning
    /// connection has fully torn down — by then <see cref="Complete"/> has already cancelled every
    /// in-flight <see cref="TryEnqueueAsync"/> wait, so nothing should still be touching either field.
    /// </summary>
    public void Dispose()
    {
        _capacityGate.Dispose();
        _readySignal.Dispose();
        _disposalCts.Dispose();
    }
}
