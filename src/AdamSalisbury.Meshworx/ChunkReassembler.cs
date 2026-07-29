namespace AdamSalisbury.Meshworx;

/// <summary>
/// Reassembles chunked messages on the receiving client, under a fixed memory bound and a per-transfer
/// deadline.
/// </summary>
/// <remarks>
/// Every partial transfer is memory held on behalf of a peer that may never finish it, so the bounds
/// are the substance of this type rather than a refinement of it. A sender that starts a hundred
/// transfers and abandons all of them must cost the receiver a bounded, reclaimable amount — which is
/// why admission is checked against a total byte budget before the first chunk is stored, and why an
/// idle transfer is dropped on a deadline rather than held until the connection ends.
/// <para>
/// Not thread-safe by itself; every method is called from a single client's receive loop, which
/// processes one frame at a time. That is also why the sweep for expired transfers runs on chunk
/// arrival rather than on a timer: there is no second thread to need one, and a client receiving
/// nothing has nothing to reclaim that its own disconnect will not.
/// </para>
/// </remarks>
internal sealed class ChunkReassembler
{
    /// <summary>The default ceiling on bytes held across all in-flight transfers, 64 MiB.</summary>
    internal const int DefaultMaxReassemblyBytes = 64 * 1024 * 1024;

    /// <summary>The default time an incomplete transfer may sit without a new chunk before it is dropped.</summary>
    internal static readonly TimeSpan DefaultTransferTimeout = TimeSpan.FromMinutes(1);

    private readonly Dictionary<TransferKey, PartialTransfer> _transfers = [];
    private readonly int _maxReassemblyBytes;
    private readonly TimeSpan _transferTimeout;
    private readonly TimeProvider _timeProvider;

    private long _bufferedBytes;

    internal ChunkReassembler(
        int? maxReassemblyBytes = null,
        TimeSpan? transferTimeout = null,
        TimeProvider? timeProvider = null)
    {
        _maxReassemblyBytes = maxReassemblyBytes ?? DefaultMaxReassemblyBytes;
        _transferTimeout = transferTimeout ?? DefaultTransferTimeout;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// The number of transfers currently part-assembled. Exposed for tests and diagnostics.
    /// </summary>
    internal int InFlightTransferCount => _transfers.Count;

    /// <summary>
    /// Offers a received chunk to the reassembler.
    /// </summary>
    /// <param name="senderId">The id of the client the chunk came from.</param>
    /// <param name="id">The logical message id the chunk belongs to.</param>
    /// <param name="index">The chunk's zero-based index.</param>
    /// <param name="count">The logical message's total chunk count.</param>
    /// <param name="body">The chunk's payload.</param>
    /// <param name="message">
    /// The fully reassembled message when this chunk completed it; otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the chunk completed its message and <paramref name="message"/> is
    /// set; <see langword="false"/> when the transfer is still incomplete, or the chunk was refused.
    /// </returns>
    /// <remarks>
    /// A refused chunk is indistinguishable from an incomplete one to the caller, deliberately: both
    /// mean "nothing to raise yet", and a receiver that told a sender which of its chunks were refused
    /// would be handing an unauthenticated peer a probe for the receiver's remaining budget.
    /// </remarks>
    internal bool TryAddChunk(
        Guid senderId,
        Guid id,
        int index,
        int count,
        ReadOnlyMemory<byte> body,
        out byte[]? message)
    {
        message = null;
        DropExpiredTransfers();

        var key = new TransferKey(senderId, id);

        // The budget is enforced continuously as chunks land rather than once at admission, because a
        // transfer's eventual size is not knowable up front: the count bounds how many chunks may
        // arrive, not how large each one is. A transfer that grows past the budget is abandoned
        // mid-flight rather than admitted and then silently truncated.
        if (_bufferedBytes + body.Length > _maxReassemblyBytes)
        {
            Abandon(key);
            return false;
        }

        if (!_transfers.TryGetValue(key, out PartialTransfer? transfer))
        {
            transfer = new PartialTransfer(count);
            _transfers[key] = transfer;
        }
        else if (transfer.Count != count || transfer.Chunks[index] is not null)
        {
            // A count that disagrees with the one this transfer started under, or a duplicate index,
            // means the sender is confused or hostile. Drop the whole transfer rather than trying to
            // reconcile two contradictory accounts of the same message.
            Abandon(key);
            return false;
        }

        byte[] chunk = body.ToArray();
        transfer.Chunks[index] = chunk;
        transfer.BufferedBytes += chunk.Length;
        transfer.ReceivedCount++;
        transfer.LastChunkAt = _timeProvider.GetUtcNow();
        _bufferedBytes += chunk.Length;

        if (transfer.ReceivedCount != transfer.Count)
        {
            return false;
        }

        message = Concatenate(transfer);
        _bufferedBytes -= transfer.BufferedBytes;
        _transfers.Remove(key);
        return true;
    }

    /// <summary>
    /// Discards every in-flight transfer, releasing the memory they hold.
    /// </summary>
    /// <remarks>
    /// Called when the connection ends. A part-assembled message cannot be completed by a different
    /// connection — the sender's ids are only meaningful within the session that issued them — so
    /// holding them past disconnect would leak memory for a completion that can never arrive.
    /// </remarks>
    internal void Clear()
    {
        _transfers.Clear();
        _bufferedBytes = 0;
    }

    /// <summary>
    /// Drops a transfer and returns the bytes it held to the budget. Safe to call for a key that has no
    /// transfer, which is the ordinary case when the very first chunk of a message is refused.
    /// </summary>
    private void Abandon(TransferKey key)
    {
        if (_transfers.TryGetValue(key, out PartialTransfer? transfer))
        {
            _bufferedBytes -= transfer.BufferedBytes;
            _transfers.Remove(key);
        }
    }

    private static byte[] Concatenate(PartialTransfer transfer)
    {
        var message = new byte[transfer.BufferedBytes];
        int offset = 0;

        foreach (byte[]? chunk in transfer.Chunks)
        {
            // Every slot is populated by the time this runs: ReceivedCount reaching Count is what
            // brought us here, and a slot is only ever written once.
            chunk!.CopyTo(message, offset);
            offset += chunk.Length;
        }

        return message;
    }

    private void DropExpiredTransfers()
    {
        if (_transfers.Count == 0)
        {
            return;
        }

        DateTimeOffset cutoff = _timeProvider.GetUtcNow() - _transferTimeout;
        List<TransferKey>? expired = null;

        foreach (KeyValuePair<TransferKey, PartialTransfer> entry in _transfers)
        {
            if (entry.Value.LastChunkAt <= cutoff)
            {
                (expired ??= []).Add(entry.Key);
            }
        }

        if (expired is null)
        {
            return;
        }

        foreach (TransferKey key in expired)
        {
            _bufferedBytes -= _transfers[key].BufferedBytes;
            _transfers.Remove(key);
        }
    }

    private readonly record struct TransferKey(Guid SenderId, Guid MessageId);

    private sealed class PartialTransfer(int count)
    {
        public int Count { get; } = count;

        public byte[]?[] Chunks { get; } = new byte[]?[count];

        public int ReceivedCount { get; set; }

        public int BufferedBytes { get; set; }

        public DateTimeOffset LastChunkAt { get; set; }
    }
}
