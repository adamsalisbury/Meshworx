using System.Buffers.Binary;
using System.Text;

namespace AdamSalisbury.Meshworx.Backplane.Redis;

/// <summary>
/// Encodes and decodes a <see cref="BackplaneMessage"/> as a flat byte layout for the Redis pub/sub
/// channel — a wire format private to this package, never seen outside a Redis <c>PUBLISH</c>/
/// <c>SUBSCRIBE</c> payload.
/// </summary>
/// <remarks>
/// <c>[originInstanceId 16][kind 1][recipientId 16][senderId 16][groupNameLength u16 BE][groupName utf8]
/// [topicLength u16 BE][topic utf8][body...]</c>. <see cref="BackplaneMessage.GroupName"/> and
/// <see cref="BackplaneMessage.Topic"/> are written as a zero-length entry when <see langword="null"/> —
/// safe because neither a group name nor a topic is ever the empty string elsewhere in this library, so
/// a zero length is unambiguous.
/// </remarks>
internal static class BackplaneMessageSerializer
{
    public static byte[] Serialize(BackplaneMessage message)
    {
        byte[] groupNameBytes = message.GroupName is null
            ? []
            : Encoding.UTF8.GetBytes(message.GroupName);
        byte[] topicBytes = message.Topic is null
            ? []
            : Encoding.UTF8.GetBytes(message.Topic);

        int length = 16 + 1 + 16 + 16 + 2 + groupNameBytes.Length + 2 + topicBytes.Length + message.Body.Length;
        var buffer = new byte[length];
        int offset = 0;

        message.OriginInstanceId.TryWriteBytes(buffer.AsSpan(offset, 16));
        offset += 16;

        buffer[offset] = (byte)message.Kind;
        offset += 1;

        message.RecipientId.TryWriteBytes(buffer.AsSpan(offset, 16));
        offset += 16;

        message.SenderId.TryWriteBytes(buffer.AsSpan(offset, 16));
        offset += 16;

        BinaryPrimitives.WriteUInt16BigEndian(buffer.AsSpan(offset, 2), (ushort)groupNameBytes.Length);
        offset += 2;
        groupNameBytes.CopyTo(buffer, offset);
        offset += groupNameBytes.Length;

        BinaryPrimitives.WriteUInt16BigEndian(buffer.AsSpan(offset, 2), (ushort)topicBytes.Length);
        offset += 2;
        topicBytes.CopyTo(buffer, offset);
        offset += topicBytes.Length;

        message.Body.Span.CopyTo(buffer.AsSpan(offset));

        return buffer;
    }

    /// <exception cref="FormatException">The payload is too short or its declared lengths run past it.</exception>
    public static BackplaneMessage Deserialize(ReadOnlySpan<byte> data)
    {
        const int FixedHeaderLength = 16 + 1 + 16 + 16;

        if (data.Length < FixedHeaderLength)
        {
            throw new FormatException("Backplane message payload is shorter than its fixed header.");
        }

        int offset = 0;
        var originInstanceId = new Guid(data.Slice(offset, 16));
        offset += 16;

        var kind = (BackplaneMessageKind)data[offset];
        offset += 1;

        var recipientId = new Guid(data.Slice(offset, 16));
        offset += 16;

        var senderId = new Guid(data.Slice(offset, 16));
        offset += 16;

        if (offset + 2 > data.Length)
        {
            throw new FormatException("Backplane message payload is truncated before the group-name length.");
        }

        int groupNameLength = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(offset, 2));
        offset += 2;

        if (offset + groupNameLength > data.Length)
        {
            throw new FormatException("Backplane message payload is truncated within the group name.");
        }

        string? groupName = groupNameLength == 0
            ? null
            : Encoding.UTF8.GetString(data.Slice(offset, groupNameLength));
        offset += groupNameLength;

        if (offset + 2 > data.Length)
        {
            throw new FormatException("Backplane message payload is truncated before the topic length.");
        }

        int topicLength = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(offset, 2));
        offset += 2;

        if (offset + topicLength > data.Length)
        {
            throw new FormatException("Backplane message payload is truncated within the topic.");
        }

        string? topic = topicLength == 0 ? null : Encoding.UTF8.GetString(data.Slice(offset, topicLength));
        offset += topicLength;

        byte[] body = data[offset..].ToArray();

        return new BackplaneMessage
        {
            OriginInstanceId = originInstanceId,
            Kind = kind,
            RecipientId = recipientId,
            SenderId = senderId,
            GroupName = groupName,
            Topic = topic,
            Body = body,
        };
    }
}
