using AdamSalisbury.Meshworx.Messages;
using AdamSalisbury.Meshworx.Serialization;
using Moq;

namespace AdamSalisbury.Meshworx.Contracts.UnitTests;

/// <summary>
/// A contract compiled by the generator in this very test project. If the generator emitted nothing,
/// or emitted something that does not compile, this file does not build — which is the strongest test
/// available for a source generator and the reason the contract lives here rather than in a string.
/// </summary>
[MeshContract]
public interface IOrderService
{
    Task SubmitAsync(int orderId, string productCode, CancellationToken cancellationToken = default);

    Task NotifyAsync(string message);

    Task<int> GetTotalAsync(int orderId, CancellationToken cancellationToken = default);

    Task<string> DescribeAsync();
}

public class GeneratedContractTests
{
    private static readonly Guid RecipientId = Guid.Parse("33333333-3333-3333-3333-333333333333");

    /// <summary>
    /// Acceptance criterion: the contract compiles to a working proxy. A call serializes its arguments
    /// and sends them under a header naming the method.
    /// </summary>
    [Fact]
    public async Task Proxy_VoidMethod_SendsArgumentsUnderMethodHeader()
    {
        var client = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> sentBody = default;
        MessageHeaders? sentHeaders = null;

        client
            .Setup(c => c.SendAsync(
                RecipientId,
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<Guid, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, body, headers, _) => (sentBody, sentHeaders) = (body, headers))
            .Returns(Task.CompletedTask);

        var proxy = new OrderServiceProxy(client.Object, JsonMessageSerializer.Default, RecipientId);
        await proxy.SubmitAsync(42, "WIDGET");

        Assert.Equal("SubmitAsync", sentHeaders![ContractHeaderKeys.Method]);

        OrderServiceSubmitAsyncArguments? arguments =
            JsonMessageSerializer.Default.Deserialize<OrderServiceSubmitAsyncArguments>(sentBody.Span);

        Assert.Equal(42, arguments!.OrderId);
        Assert.Equal("WIDGET", arguments.ProductCode);
    }

    /// <summary>
    /// A method returning a value goes out as a request and its reply is decoded back into the declared
    /// return type — correlated by the core library's own request/response helper rather than by a
    /// scheme the generator invents.
    /// </summary>
    [Fact]
    public async Task Proxy_MethodWithResult_SendsRequestAndDecodesReply()
    {
        var client = new Mock<IMeshClient>();

        client
            .Setup(c => c.RequestAsync(
                RecipientId,
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((ReadOnlyMemory<byte>)JsonMessageSerializer.Default.Serialize(99).ToArray());

        var proxy = new OrderServiceProxy(client.Object, JsonMessageSerializer.Default, RecipientId);

        Assert.Equal(99, await proxy.GetTotalAsync(7));
    }

    /// <summary>
    /// Acceptance criterion: the dispatcher invokes the matching handler, with the arguments the sender
    /// passed.
    /// </summary>
    [Fact]
    public async Task Dispatcher_VoidMethod_InvokesImplementationWithArguments()
    {
        var implementation = new RecordingOrderService();
        var dispatcher = new OrderServiceDispatcher(implementation, JsonMessageSerializer.Default);

        var body = JsonMessageSerializer.Default.Serialize(
            new OrderServiceSubmitAsyncArguments(7, "BOLT"));

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = body,
            Headers = new MessageHeaders(
                new Dictionary<string, string> { [ContractHeaderKeys.Method] = "SubmitAsync" }),
        };

        Assert.True(await dispatcher.TryDispatchAsync(message));
        Assert.Equal((7, "BOLT"), implementation.Submitted);
    }

    /// <summary>
    /// A dispatched method that returns a value replies through the supplied client, so the calling
    /// proxy's awaiting request completes.
    /// </summary>
    [Fact]
    public async Task Dispatcher_MethodWithResult_RepliesWithSerializedResult()
    {
        var implementation = new RecordingOrderService { TotalToReturn = 123 };
        var dispatcher = new OrderServiceDispatcher(implementation, JsonMessageSerializer.Default);
        var replyClient = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> replyBody = default;

        replyClient
            .Setup(c => c.ReplyAsync(
                It.IsAny<MessageReceivedEventArgs>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<MessageReceivedEventArgs, ReadOnlyMemory<byte>, CancellationToken>(
                (_, body, _) => replyBody = body)
            .Returns(Task.CompletedTask);

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = JsonMessageSerializer.Default.Serialize(new OrderServiceGetTotalAsyncArguments(7)),
            Headers = new MessageHeaders(
                new Dictionary<string, string> { [ContractHeaderKeys.Method] = "GetTotalAsync" }),
        };

        Assert.True(await dispatcher.TryDispatchAsync(message, replyClient.Object));
        Assert.Equal(123, JsonMessageSerializer.Default.Deserialize<int>(replyBody.Span));
    }

    /// <summary>
    /// A message naming no contract method, or none at all, is declined rather than swallowed — so a
    /// connection carrying several contracts can offer each message to every dispatcher in turn.
    /// </summary>
    [Fact]
    public async Task Dispatcher_UnknownOrMissingMethod_ReturnsFalse()
    {
        var dispatcher = new OrderServiceDispatcher(
            new RecordingOrderService(), JsonMessageSerializer.Default);

        var unknown = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = ReadOnlyMemory<byte>.Empty,
            Headers = new MessageHeaders(
                new Dictionary<string, string> { [ContractHeaderKeys.Method] = "NoSuchMethod" }),
        };

        var headerless = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = ReadOnlyMemory<byte>.Empty,
        };

        Assert.False(await dispatcher.TryDispatchAsync(unknown));
        Assert.False(await dispatcher.TryDispatchAsync(headerless));
    }

    /// <summary>
    /// A parameterless method needs no argument record, and round-trips through both halves without
    /// one.
    /// </summary>
    [Fact]
    public async Task ProxyAndDispatcher_ParameterlessMethod_RoundTrip()
    {
        var implementation = new RecordingOrderService { DescriptionToReturn = "all good" };
        var dispatcher = new OrderServiceDispatcher(implementation, JsonMessageSerializer.Default);
        var replyClient = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> replyBody = default;

        replyClient
            .Setup(c => c.ReplyAsync(
                It.IsAny<MessageReceivedEventArgs>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback<MessageReceivedEventArgs, ReadOnlyMemory<byte>, CancellationToken>(
                (_, body, _) => replyBody = body)
            .Returns(Task.CompletedTask);

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = ReadOnlyMemory<byte>.Empty,
            Headers = new MessageHeaders(
                new Dictionary<string, string> { [ContractHeaderKeys.Method] = "DescribeAsync" }),
        };

        Assert.True(await dispatcher.TryDispatchAsync(message, replyClient.Object));
        Assert.Equal("all good", JsonMessageSerializer.Default.Deserialize<string>(replyBody.Span));
    }

    /// <summary>
    /// The proxy satisfies the contract interface itself, so application code can depend on
    /// <see cref="IOrderService"/> and be handed either a local implementation or a remote proxy.
    /// </summary>
    [Fact]
    public void Proxy_ImplementsTheContractInterface()
    {
        var proxy = new OrderServiceProxy(
            new Mock<IMeshClient>().Object, JsonMessageSerializer.Default, RecipientId);

        Assert.IsAssignableFrom<IOrderService>(proxy);
    }

    private sealed class RecordingOrderService : IOrderService
    {
        public (int OrderId, string ProductCode)? Submitted { get; private set; }

        public int TotalToReturn { get; init; }

        public string DescriptionToReturn { get; init; } = string.Empty;

        public Task SubmitAsync(int orderId, string productCode, CancellationToken cancellationToken = default)
        {
            Submitted = (orderId, productCode);
            return Task.CompletedTask;
        }

        public Task NotifyAsync(string message) => Task.CompletedTask;

        public Task<int> GetTotalAsync(int orderId, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(TotalToReturn);
        }

        public Task<string> DescribeAsync() => Task.FromResult(DescriptionToReturn);
    }
}
