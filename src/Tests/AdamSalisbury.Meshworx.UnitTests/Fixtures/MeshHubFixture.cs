using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Threading.Channels;
using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Transport;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.UnitTests.Fixtures;

internal sealed class MeshHubFixture
{
    private readonly Channel<ITransport> _pendingAccepts = Channel.CreateUnbounded<ITransport>();
    private readonly ConcurrentQueue<Exception> _acceptFailures = new();

    public Mock<ITransportListener> Listener { get; } = new();
    public MeshHub Hub { get; }

    public MeshHubFixture(
        TimeSpan? registrationTimeout = null,
        int? maxClients = null,
        TimeSpan? heartbeatInterval = null,
        int maxMissedHeartbeats = 2,
        ClientAuthenticator? authenticator = null,
        int? maxConcurrentAuthentications = null,
        GroupAuthoriser? groupAuthoriser = null,
        TimeSpan? groupAuthorisationTimeout = null,
        int? maxConnectionsPerRemoteEndpoint = null,
        bool notifyOnQueueSaturation = false,
        TimeSpan? backpressureAwaitTimeout = null,
        IOfflineStore? offlineStore = null,
        TimeSpan? offlineStoreTimeout = null,
        TimeSpan? sessionResumptionWindow = null)
    {
        var logger = new Mock<ILogger<MeshHub>>();
        Listener.Setup(l => l.StartAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        Listener.Setup(l => l.DisposeAsync()).Returns(ValueTask.CompletedTask);
        Listener.Setup(l => l.AcceptAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct =>
            {
                if (_acceptFailures.TryDequeue(out Exception? failure))
                {
                    throw failure;
                }

                return await _pendingAccepts.Reader.ReadAsync(ct).ConfigureAwait(false);
            });
        Hub = new MeshHub(
            logger.Object,
            Listener.Object,
            registrationTimeout,
            maxClients,
            heartbeatInterval,
            maxMissedHeartbeats,
            authenticator,
            maxConcurrentAuthentications,
            groupAuthoriser,
            groupAuthorisationTimeout,
            maxConnectionsPerRemoteEndpoint,
            notifyOnQueueSaturation,
            backpressureAwaitTimeout,
            offlineStore,
            offlineStoreTimeout,
            sessionResumptionWindow);
    }

    /// <summary>
    /// Builds a ClientLookupRequest frame: [type][correlation id (4, big-endian)][UTF-8 name].
    /// </summary>
    public static byte[] CreateLookupRequest(int correlationId, string name)
    {
        byte[] nameBytes = Encoding.UTF8.GetBytes(name);
        var payload = new byte[1 + 4 + nameBytes.Length];
        payload[0] = 0x06; // ClientLookupRequest
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(1, 4), correlationId);
        nameBytes.CopyTo(payload, 5);
        return payload;
    }

    /// <summary>
    /// Builds a SendMessage frame: [type][recipient id (16)][message].
    /// </summary>
    public static byte[] CreateDirectMessage(Guid recipientId, byte[] message)
    {
        var payload = new byte[1 + 16 + message.Length];
        payload[0] = 0x02; // SendMessage
        recipientId.TryWriteBytes(payload.AsSpan(1));
        message.CopyTo(payload, 17);
        return payload;
    }

    /// <summary>
    /// Builds a SendMessageWithHeaders frame:
    /// [type][recipient id (16)][headerBlockLength(2)][headerBlock][message].
    /// </summary>
    public static byte[] CreateDirectMessageWithHeaders(Guid recipientId, MessageHeaders headers, byte[] message)
    {
        int headerLength = HeaderEnvelope.GetEncodedLength(headers);
        var payload = new byte[1 + 16 + 2 + headerLength + message.Length];
        payload[0] = 0x11; // SendMessageWithHeaders
        recipientId.TryWriteBytes(payload.AsSpan(1));
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(17, 2), (ushort)headerLength);
        HeaderEnvelope.Write(headers, payload.AsSpan(19, headerLength));
        message.CopyTo(payload, 19 + headerLength);
        return payload;
    }

    /// <summary>
    /// Builds a GroupMessageWithHeaders frame: [type][name length (2, big-endian)][UTF-8 group name]
    /// [headerBlockLength(2)][headerBlock][message].
    /// </summary>
    public static byte[] CreateGroupMessageWithHeaders(string groupName, MessageHeaders headers, byte[] message)
    {
        byte[] nameBytes = Encoding.UTF8.GetBytes(groupName);
        int headerLength = HeaderEnvelope.GetEncodedLength(headers);
        int headerLengthOffset = 3 + nameBytes.Length;
        var payload = new byte[headerLengthOffset + 2 + headerLength + message.Length];
        payload[0] = 0x13; // GroupMessageWithHeaders
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(1, 2), (ushort)nameBytes.Length);
        nameBytes.CopyTo(payload, 3);
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(headerLengthOffset, 2), (ushort)headerLength);
        HeaderEnvelope.Write(headers, payload.AsSpan(headerLengthOffset + 2, headerLength));
        message.CopyTo(payload, headerLengthOffset + 2 + headerLength);
        return payload;
    }

    /// <summary>
    /// Builds a ResumeSession frame: [type][token].
    /// </summary>
    public static byte[] CreateResumeSessionRequest(byte[] token)
    {
        var payload = new byte[1 + token.Length];
        payload[0] = 0x16; // ResumeSession
        token.CopyTo(payload, 1);
        return payload;
    }

    /// <summary>
    /// Builds a JoinGroup frame: [type][UTF-8 group name].
    /// </summary>
    public static byte[] CreateJoinGroupRequest(string groupName)
    {
        byte[] nameBytes = Encoding.UTF8.GetBytes(groupName);
        var payload = new byte[1 + nameBytes.Length];
        payload[0] = 0x0C; // JoinGroup
        nameBytes.CopyTo(payload, 1);
        return payload;
    }

    /// <summary>
    /// Builds a GroupMessage frame: [type][name length (2, big-endian)][UTF-8 group name][message].
    /// </summary>
    public static byte[] CreateGroupMessage(string groupName, byte[] message)
    {
        byte[] nameBytes = Encoding.UTF8.GetBytes(groupName);
        var payload = new byte[1 + 2 + nameBytes.Length + message.Length];
        payload[0] = 0x0E; // GroupMessage
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(1, 2), (ushort)nameBytes.Length);
        nameBytes.CopyTo(payload, 3);
        message.CopyTo(payload, 3 + nameBytes.Length);
        return payload;
    }

    /// <summary>
    /// Causes the next call to the listener's AcceptAsync to throw the given exception before
    /// any queued client is accepted. Used to simulate a transient accept failure.
    /// </summary>
    public void FailNextAccept(Exception exception)
    {
        _acceptFailures.Enqueue(exception);
    }

    public static byte[] CreateRegistrationRequest(
        string name = "TestClient", byte[]? credential = null, byte versionMin = 0x04, byte versionMax = 0x04)
    {
        // Registration frame: [type][versionMin][versionMax][name length (2, big-endian)][name][credential].
        credential ??= [];
        byte[] nameBytes = Encoding.UTF8.GetBytes(name);
        var payload = new byte[3 + 2 + nameBytes.Length + credential.Length];
        payload[0] = 0x04; // RegistrationRequest
        payload[1] = versionMin;
        payload[2] = versionMax;
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(3, 2), (ushort)nameBytes.Length);
        nameBytes.CopyTo(payload, 5);
        credential.CopyTo(payload, 5 + nameBytes.Length);
        return payload;
    }

    /// <param name="remoteEndPoint">
    /// When given, the transport also implements <see cref="IRemoteEndPointTransport"/> and reports
    /// this as its remote address, so it participates in the hub's per-remote-endpoint connection cap
    /// exactly as <see cref="AdamSalisbury.Meshworx.Transport.Tcp.TcpTransport"/> does.
    /// </param>
    public static Mock<ITransport> CreateMockTransport(IPEndPoint? remoteEndPoint = null)
    {
        var transport = new Mock<ITransport>();
        transport.Setup(t => t.DisposeAsync()).Returns(ValueTask.CompletedTask);
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        if (remoteEndPoint is not null)
        {
            transport.As<IRemoteEndPointTransport>().Setup(t => t.RemoteEndPoint).Returns(remoteEndPoint);
        }

        return transport;
    }

    public void EnqueueClient(ITransport transport)
    {
        _pendingAccepts.Writer.TryWrite(transport);
    }

    public async Task<RegisteredClient> RegisterClientAsync(
        string name = "TestClient",
        IPEndPoint? remoteEndPoint = null,
        byte versionMin = 0x04,
        byte versionMax = 0x04)
    {
        var transport = CreateMockTransport(remoteEndPoint);
        var registrationCompleteTcs = new TaskCompletionSource<byte[]>();
        var disconnectTcs = new TaskCompletionSource<byte[]?>();

        transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateRegistrationRequest(name, versionMin: versionMin, versionMax: versionMax))
            .Returns(disconnectTcs.Task);

        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => registrationCompleteTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        EnqueueClient(transport.Object);

        byte[] responseData = await registrationCompleteTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        var clientId = new Guid(responseData.AsSpan(1, 16));

        while (!Hub.IsClientRegistered(clientId))
        {
            await Task.Yield();
        }

        return new RegisteredClient(clientId, transport, disconnectTcs, responseData);
    }

    /// <summary>
    /// Registers a client with a <see cref="FrameRecorder"/> attached <em>before</em> the connection is
    /// accepted, so frames the hub sends of its own accord the moment registration completes are
    /// captured rather than raced.
    /// </summary>
    /// <remarks>
    /// <see cref="RegisterClientAsync"/> installs its own <c>SendAsync</c> callback and a test that
    /// attaches a recorder afterwards replaces it — fine for frames provoked later by the test itself,
    /// but not for the offline store's drain, which the hub queues between sending
    /// <c>RegistrationComplete</c> and reading the client's first frame.
    /// </remarks>
    public async Task<(RegisteredClient Client, FrameRecorder Frames)> RegisterClientWithRecorderAsync(
        string name = "TestClient", byte versionMin = 0x04, byte versionMax = 0x04)
    {
        var transport = CreateMockTransport();
        var disconnectTcs = new TaskCompletionSource<byte[]?>();
        var recorder = new FrameRecorder(transport);

        transport.SetupSequence(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateRegistrationRequest(name, versionMin: versionMin, versionMax: versionMax))
            .Returns(disconnectTcs.Task);

        EnqueueClient(transport.Object);

        byte[] responseData = await recorder
            .WaitForAsync(frame => frame.Length >= 18 && frame[0] == 0x01)
            .WaitAsync(TestTimeouts.Wait)
            .ConfigureAwait(false);
        var clientId = new Guid(responseData.AsSpan(1, 16));

        while (!Hub.IsClientRegistered(clientId))
        {
            await Task.Yield();
        }

        return (new RegisteredClient(clientId, transport, disconnectTcs, responseData), recorder);
    }

    public async Task<MultiMessageRegisteredClient> RegisterMultiMessageClientAsync(
        string name = "TestClient", byte versionMin = 0x04, byte versionMax = 0x04)
    {
        var transport = CreateMockTransport();
        var registrationCompleteTcs = new TaskCompletionSource<byte[]>();
        var messageChannel = Channel.CreateUnbounded<byte[]?>();

        // Queue the registration request as the first message.
        await messageChannel.Writer.WriteAsync(
            CreateRegistrationRequest(name, versionMin: versionMin, versionMax: versionMax)).ConfigureAwait(false);

        transport.Setup(t => t.ReceiveAsync(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(async ct => await messageChannel.Reader.ReadAsync(ct).ConfigureAwait(false));

        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => registrationCompleteTcs.TrySetResult(data.ToArray()))
            .Returns(Task.CompletedTask);

        EnqueueClient(transport.Object);

        byte[] responseData = await registrationCompleteTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        var clientId = new Guid(responseData.AsSpan(1, 16));

        while (!Hub.IsClientRegistered(clientId))
        {
            await Task.Yield();
        }

        return new MultiMessageRegisteredClient(clientId, transport, messageChannel, responseData);
    }
}

/// <summary>
/// Records every frame the hub sends to one client's transport and lets a test wait for the first frame
/// matching a predicate. Waiting on a frame the hub must <i>not</i> send is never deterministic on its
/// own, so tests pair this with a frame the hub certainly will send afterwards on the same connection:
/// because a client's outbound queue is drained in order, the arrival of the later frame proves the
/// earlier one was never queued.
/// </summary>
internal sealed class FrameRecorder
{
    private readonly Lock _lock = new();
    private readonly List<byte[]> _frames = [];
    private readonly List<(Func<byte[], bool> Predicate, TaskCompletionSource<byte[]> Completion)> _waiters = [];

    public FrameRecorder(Mock<ITransport> transport)
    {
        transport.Setup(t => t.SendAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Callback<ReadOnlyMemory<byte>, CancellationToken>((data, _) => Record(data.ToArray()))
            .Returns(Task.CompletedTask);
    }

    /// <summary>
    /// A snapshot of every frame recorded so far.
    /// </summary>
    public IReadOnlyList<byte[]> Frames
    {
        get
        {
            lock (_lock)
            {
                return [.. _frames];
            }
        }
    }

    /// <summary>
    /// Completes with the first frame matching the predicate, including one already recorded.
    /// </summary>
    public Task<byte[]> WaitForAsync(Func<byte[], bool> predicate)
    {
        var completion = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        lock (_lock)
        {
            foreach (byte[] frame in _frames)
            {
                if (predicate(frame))
                {
                    completion.SetResult(frame);
                    return completion.Task;
                }
            }

            _waiters.Add((predicate, completion));
        }

        return completion.Task;
    }

    private void Record(byte[] frame)
    {
        lock (_lock)
        {
            _frames.Add(frame);

            for (int i = _waiters.Count - 1; i >= 0; i--)
            {
                if (_waiters[i].Predicate(frame))
                {
                    _waiters[i].Completion.TrySetResult(frame);
                    _waiters.RemoveAt(i);
                }
            }
        }
    }
}

internal sealed class RegisteredClient(
    Guid id,
    Mock<ITransport> transport,
    TaskCompletionSource<byte[]?> disconnectTcs,
    byte[] registrationResponse)
{
    public Guid Id { get; } = id;
    public Mock<ITransport> Transport { get; } = transport;
    public TaskCompletionSource<byte[]?> DisconnectTcs { get; } = disconnectTcs;
    public byte[] RegistrationResponse { get; } = registrationResponse;

    /// <summary>
    /// The protocol version the hub echoed back in <see cref="RegistrationResponse"/>.
    /// </summary>
    public byte NegotiatedProtocolVersion => RegistrationResponse[17];

    /// <summary>
    /// The session resumption token the hub appended to <see cref="RegistrationResponse"/>, or
    /// <see langword="null"/> when it issued none — resumption switched off, the connection negotiated
    /// below version 6, or the hub's session table full.
    /// </summary>
    public byte[]? SessionToken
    {
        get
        {
            if (RegistrationResponse.Length < 20)
            {
                return null;
            }

            int tokenLength = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(
                RegistrationResponse.AsSpan(18, 2));
            return RegistrationResponse.Length < 20 + tokenLength
                ? null
                : RegistrationResponse.AsSpan(20, tokenLength).ToArray();
        }
    }

    public void Disconnect() => DisconnectTcs.TrySetResult(null);
}

internal sealed class MultiMessageRegisteredClient(
    Guid id,
    Mock<ITransport> transport,
    Channel<byte[]?> messageChannel,
    byte[] registrationResponse)
{
    public Guid Id { get; } = id;
    public Mock<ITransport> Transport { get; } = transport;
    public Channel<byte[]?> MessageChannel { get; } = messageChannel;
    public byte[] RegistrationResponse { get; } = registrationResponse;

    public void EnqueueMessage(byte[] message) => MessageChannel.Writer.TryWrite(message);

    public void Disconnect() => MessageChannel.Writer.TryWrite(null);
}
