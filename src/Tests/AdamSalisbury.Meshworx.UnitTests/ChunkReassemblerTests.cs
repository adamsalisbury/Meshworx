using System.Text;

namespace AdamSalisbury.Meshworx.UnitTests;

public class ChunkReassemblerTests
{
    private static readonly Guid SenderA = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid SenderB = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    [Fact]
    public void TryAddChunk_AllChunksInOrder_ReassemblesInOrder()
    {
        var reassembler = new ChunkReassembler();
        var id = Guid.NewGuid();

        Assert.False(reassembler.TryAddChunk(SenderA, id, 0, 3, "abc"u8.ToArray(), out _));
        Assert.False(reassembler.TryAddChunk(SenderA, id, 1, 3, "def"u8.ToArray(), out _));
        Assert.True(reassembler.TryAddChunk(SenderA, id, 2, 3, "ghi"u8.ToArray(), out byte[]? message));

        Assert.Equal("abcdefghi", Encoding.UTF8.GetString(message!));
        Assert.Equal(0, reassembler.InFlightTransferCount);
    }

    /// <summary>
    /// Chunks are placed by their index, not by arrival order, so a transfer that arrives shuffled
    /// rebuilds in the order the sender wrote it.
    /// </summary>
    [Fact]
    public void TryAddChunk_ChunksOutOfOrder_ReassemblesByIndex()
    {
        var reassembler = new ChunkReassembler();
        var id = Guid.NewGuid();

        Assert.False(reassembler.TryAddChunk(SenderA, id, 2, 3, "ghi"u8.ToArray(), out _));
        Assert.False(reassembler.TryAddChunk(SenderA, id, 0, 3, "abc"u8.ToArray(), out _));
        Assert.True(reassembler.TryAddChunk(SenderA, id, 1, 3, "def"u8.ToArray(), out byte[]? message));

        Assert.Equal("abcdefghi", Encoding.UTF8.GetString(message!));
    }

    /// <summary>
    /// Acceptance criterion: interleaved chunks from different logical messages reassemble
    /// independently — including two transfers from different senders that happen to share an id.
    /// </summary>
    [Fact]
    public void TryAddChunk_InterleavedTransfers_ReassembleIndependently()
    {
        var reassembler = new ChunkReassembler();
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        var shared = Guid.NewGuid();

        Assert.False(reassembler.TryAddChunk(SenderA, first, 0, 2, "AA"u8.ToArray(), out _));
        Assert.False(reassembler.TryAddChunk(SenderA, second, 0, 2, "BB"u8.ToArray(), out _));

        // Same message id, different sender: a distinct transfer, not a continuation of SenderA's.
        Assert.False(reassembler.TryAddChunk(SenderB, shared, 0, 2, "CC"u8.ToArray(), out _));
        Assert.False(reassembler.TryAddChunk(SenderA, shared, 0, 2, "DD"u8.ToArray(), out _));

        Assert.Equal(4, reassembler.InFlightTransferCount);

        Assert.True(reassembler.TryAddChunk(SenderA, second, 1, 2, "bb"u8.ToArray(), out byte[]? secondMessage));
        Assert.True(reassembler.TryAddChunk(SenderA, first, 1, 2, "aa"u8.ToArray(), out byte[]? firstMessage));
        Assert.True(reassembler.TryAddChunk(SenderB, shared, 1, 2, "cc"u8.ToArray(), out byte[]? sharedB));
        Assert.True(reassembler.TryAddChunk(SenderA, shared, 1, 2, "dd"u8.ToArray(), out byte[]? sharedA));

        Assert.Equal("AAaa", Encoding.UTF8.GetString(firstMessage!));
        Assert.Equal("BBbb", Encoding.UTF8.GetString(secondMessage!));
        Assert.Equal("CCcc", Encoding.UTF8.GetString(sharedB!));
        Assert.Equal("DDdd", Encoding.UTF8.GetString(sharedA!));
        Assert.Equal(0, reassembler.InFlightTransferCount);
    }

    /// <summary>
    /// Acceptance criterion: an abandoned transfer is reclaimed rather than held until the connection
    /// ends.
    /// </summary>
    [Fact]
    public void TryAddChunk_TransferIdlePastTimeout_IsReclaimed()
    {
        var time = new ControllableTimeProvider();
        var reassembler = new ChunkReassembler(
            transferTimeout: TimeSpan.FromSeconds(30), timeProvider: time);
        var abandoned = Guid.NewGuid();

        Assert.False(reassembler.TryAddChunk(SenderA, abandoned, 0, 2, "AA"u8.ToArray(), out _));
        Assert.Equal(1, reassembler.InFlightTransferCount);

        time.Advance(TimeSpan.FromSeconds(31));

        // Any subsequent chunk sweeps first, which is what reclaims the abandoned transfer above.
        var fresh = Guid.NewGuid();
        Assert.False(reassembler.TryAddChunk(SenderA, fresh, 0, 2, "BB"u8.ToArray(), out _));

        Assert.Equal(1, reassembler.InFlightTransferCount);

        // The reclaimed transfer's own late chunk starts a new transfer rather than completing the old
        // one, so a message whose first half was reclaimed never silently reassembles wrong.
        Assert.False(reassembler.TryAddChunk(SenderA, abandoned, 1, 2, "aa"u8.ToArray(), out byte[]? message));
        Assert.Null(message);
    }

    /// <summary>
    /// The byte budget bounds what a peer can make a receiver hold. A chunk that would breach it is
    /// refused and its transfer abandoned, rather than admitted and later truncated.
    /// </summary>
    [Fact]
    public void TryAddChunk_BeyondByteBudget_RefusesAndAbandonsTransfer()
    {
        var reassembler = new ChunkReassembler(maxReassemblyBytes: 8);
        var id = Guid.NewGuid();

        Assert.False(reassembler.TryAddChunk(SenderA, id, 0, 3, new byte[6], out _));
        Assert.Equal(1, reassembler.InFlightTransferCount);

        // 6 held + 6 more would exceed 8: refused, and the transfer it belonged to is dropped whole.
        Assert.False(reassembler.TryAddChunk(SenderA, id, 1, 3, new byte[6], out _));
        Assert.Equal(0, reassembler.InFlightTransferCount);

        // The budget was returned, so an unrelated transfer is admitted afterwards.
        Assert.False(reassembler.TryAddChunk(SenderB, Guid.NewGuid(), 0, 2, new byte[6], out _));
        Assert.Equal(1, reassembler.InFlightTransferCount);
    }

    /// <summary>
    /// A duplicate index means the sender is confused or hostile; the transfer is dropped rather than
    /// one copy silently winning.
    /// </summary>
    [Fact]
    public void TryAddChunk_DuplicateIndex_DropsTransfer()
    {
        var reassembler = new ChunkReassembler();
        var id = Guid.NewGuid();

        Assert.False(reassembler.TryAddChunk(SenderA, id, 0, 2, "AA"u8.ToArray(), out _));
        Assert.False(reassembler.TryAddChunk(SenderA, id, 0, 2, "XX"u8.ToArray(), out _));

        Assert.Equal(0, reassembler.InFlightTransferCount);
    }

    /// <summary>
    /// A count that contradicts the one the transfer started under is two irreconcilable accounts of
    /// the same message. Neither is trusted.
    /// </summary>
    [Fact]
    public void TryAddChunk_ContradictoryCount_DropsTransfer()
    {
        var reassembler = new ChunkReassembler();
        var id = Guid.NewGuid();

        Assert.False(reassembler.TryAddChunk(SenderA, id, 0, 3, "AA"u8.ToArray(), out _));
        Assert.False(reassembler.TryAddChunk(SenderA, id, 1, 9, "BB"u8.ToArray(), out _));

        Assert.Equal(0, reassembler.InFlightTransferCount);
    }

    [Fact]
    public void TryAddChunk_SingleChunkMessage_CompletesImmediately()
    {
        var reassembler = new ChunkReassembler();

        Assert.True(reassembler.TryAddChunk(
            SenderA, Guid.NewGuid(), 0, 1, "whole"u8.ToArray(), out byte[]? message));

        Assert.Equal("whole", Encoding.UTF8.GetString(message!));
    }

    [Fact]
    public void Clear_DiscardsEveryTransferAndReleasesItsBudget()
    {
        var reassembler = new ChunkReassembler(maxReassemblyBytes: 8);

        Assert.False(reassembler.TryAddChunk(SenderA, Guid.NewGuid(), 0, 2, new byte[8], out _));
        Assert.Equal(1, reassembler.InFlightTransferCount);

        reassembler.Clear();

        Assert.Equal(0, reassembler.InFlightTransferCount);

        // The full budget is available again, which it would not be if Clear had only emptied the map.
        Assert.False(reassembler.TryAddChunk(SenderB, Guid.NewGuid(), 0, 2, new byte[8], out _));
        Assert.Equal(1, reassembler.InFlightTransferCount);
    }

    /// <summary>
    /// A hand-rolled controllable clock rather than Microsoft.Extensions.TimeProvider.Testing: the
    /// reassembler only ever asks for the current instant, so a package reference for one overridable
    /// member would cost the solution a dependency it does not otherwise need.
    /// </summary>
    private sealed class ControllableTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan delta) => _utcNow += delta;
    }
}
