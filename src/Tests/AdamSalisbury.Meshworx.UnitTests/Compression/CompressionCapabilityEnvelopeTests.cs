using System.Text;
using AdamSalisbury.Meshworx.Messages;

namespace AdamSalisbury.Meshworx.UnitTests.Compression;

public sealed class CompressionCapabilityEnvelopeTests
{
    [Fact]
    public void RoundTrip_PreservesOrder()
    {
        // Order is the payload here, not incidental: it is the advertising endpoint's preference order.
        string[] algorithmIds = ["br", "deflate", "x-rle"];

        var buffer = new byte[CompressionCapabilityEnvelope.GetEncodedLength(algorithmIds)];
        CompressionCapabilityEnvelope.Write(algorithmIds, buffer);

        Assert.True(CompressionCapabilityEnvelope.TryRead(buffer, out IReadOnlyList<string> decoded));
        Assert.Equal(algorithmIds, decoded);
    }

    [Fact]
    public void RoundTrip_EmptyList_IsASingleCountByte()
    {
        var buffer = new byte[CompressionCapabilityEnvelope.GetEncodedLength([])];
        CompressionCapabilityEnvelope.Write([], buffer);

        Assert.Single(buffer);
        Assert.True(CompressionCapabilityEnvelope.TryRead(buffer, out IReadOnlyList<string> decoded));
        Assert.Empty(decoded);
    }

    [Fact]
    public void GetEncodedLength_CountsThePrefixAndEveryIdsLengthByte()
    {
        // 1 count byte + (1 + 2) + (1 + 7)
        Assert.Equal(12, CompressionCapabilityEnvelope.GetEncodedLength(["br", "deflate"]));
    }

    [Fact]
    public void TryRead_MaximumCount_IsAccepted()
    {
        string[] algorithmIds = [.. Enumerable.Range(0, Protocol.MaxAdvertisedCompressionAlgorithms)
            .Select(i => $"alg-{i:D2}")];

        var buffer = new byte[CompressionCapabilityEnvelope.GetEncodedLength(algorithmIds)];
        CompressionCapabilityEnvelope.Write(algorithmIds, buffer);

        Assert.True(CompressionCapabilityEnvelope.TryRead(buffer, out IReadOnlyList<string> decoded));
        Assert.Equal(Protocol.MaxAdvertisedCompressionAlgorithms, decoded.Count);
    }

    [Fact]
    public void TryRead_CountBeyondTheMaximum_IsRejectedWithoutReadingTheEntries()
    {
        // A ceiling on what a peer can assert before any of it is held.
        byte[] block = [(byte)(Protocol.MaxAdvertisedCompressionAlgorithms + 1)];

        Assert.False(CompressionCapabilityEnvelope.TryRead(block, out IReadOnlyList<string> decoded));
        Assert.Empty(decoded);
    }

    [Fact]
    public void TryRead_Empty_IsRejected()
    {
        Assert.False(CompressionCapabilityEnvelope.TryRead([], out _));
    }

    [Fact]
    public void TryRead_TruncatedMidId_IsRejectedWholesale()
    {
        // Rejected entirely rather than partially accepted: believing a peer supports a set it never
        // claimed is worse than believing it supports nothing.
        string[] algorithmIds = ["br", "deflate"];
        var buffer = new byte[CompressionCapabilityEnvelope.GetEncodedLength(algorithmIds)];
        CompressionCapabilityEnvelope.Write(algorithmIds, buffer);

        Assert.False(CompressionCapabilityEnvelope.TryRead(buffer.AsSpan(0, buffer.Length - 3), out IReadOnlyList<string> decoded));
        Assert.Empty(decoded);
    }

    [Fact]
    public void TryRead_TruncatedBeforeAnIdsLengthByte_IsRejected()
    {
        byte[] block = [2, 2, (byte)'b', (byte)'r'];

        Assert.False(CompressionCapabilityEnvelope.TryRead(block, out _));
    }

    [Fact]
    public void TryRead_TrailingBytesAfterTheLastId_IsRejected()
    {
        // Trailing bytes mean this is not the block it claims to be, whatever else it might decode to.
        byte[] block = [1, 2, (byte)'b', (byte)'r', 0xFF];

        Assert.False(CompressionCapabilityEnvelope.TryRead(block, out _));
    }

    [Fact]
    public void TryRead_ZeroLengthId_IsRejected()
    {
        byte[] block = [1, 0];

        Assert.False(CompressionCapabilityEnvelope.TryRead(block, out _));
    }

    [Fact]
    public void TryRead_IdLongerThanTheProtocolAllows_IsRejected()
    {
        int overlong = Protocol.MaxCompressionAlgorithmIdLength + 1;
        byte[] block = [1, (byte)overlong, .. Encoding.UTF8.GetBytes(new string('a', overlong))];

        Assert.False(CompressionCapabilityEnvelope.TryRead(block, out _));
    }

    [Fact]
    public void TryRead_IdAtTheMaximumLength_IsAccepted()
    {
        string longest = new('a', Protocol.MaxCompressionAlgorithmIdLength);
        var buffer = new byte[CompressionCapabilityEnvelope.GetEncodedLength([longest])];
        CompressionCapabilityEnvelope.Write([longest], buffer);

        Assert.True(CompressionCapabilityEnvelope.TryRead(buffer, out IReadOnlyList<string> decoded));
        Assert.Equal([longest], decoded);
    }
}
