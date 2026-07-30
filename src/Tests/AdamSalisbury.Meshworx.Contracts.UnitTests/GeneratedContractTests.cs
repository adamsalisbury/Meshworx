using System.Diagnostics.CodeAnalysis;
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

/// <summary>
/// A second contract declaring a method of the same name as one on <see cref="IOrderService"/>. Two
/// contracts on one connection are the advertised design, so the wire identity must distinguish them.
/// </summary>
[MeshContract]
public interface IInvoiceService
{
    Task SubmitAsync(int reference, string currency);
}

/// <summary>
/// Ordinary contract shapes that used to emit code which did not compile: a keyword-named member, a
/// parameter named after a generated local, a non-token parameter called <c>cancellationToken</c>
/// alongside a real token called something else, and a nullable reference type. The static member is
/// not part of the wire contract and must be skipped rather than dispatched.
/// </summary>
/// <remarks>
/// This interface earns its keep at compile time: if the generator regresses on any of these, the test
/// project fails to build.
/// </remarks>
[MeshContract]
[SuppressMessage(
    "Naming",
    "CA1716:Identifiers should not match keywords",
    Justification = "The keyword-named member and parameter are the fixture: the generator must escape "
        + "every identifier it takes from a contract author's own names, and this interface is how that "
        + "is held to at compile time.")]
public interface IAwkwardContract
{
    Task @event(string? note, int __arguments, string cancellationToken, CancellationToken ct = default);

    Task<int> ComputeAsync(int @class);

    static string Describe() => "not part of the contract";
}

public class GeneratedContractTests
{
    private static readonly Guid RecipientId = Guid.Parse("33333333-3333-3333-3333-333333333333");

    private const string OrderContract = "AdamSalisbury.Meshworx.Contracts.UnitTests.IOrderService";
    private const string InvoiceContract = "AdamSalisbury.Meshworx.Contracts.UnitTests.IInvoiceService";

    /// <summary>
    /// Acceptance criterion: the contract compiles to a working proxy. A call serializes its arguments
    /// and sends them under a header naming the method — qualified by the contract that declares it.
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

        Assert.Equal($"{OrderContract}.SubmitAsync", sentHeaders![ContractHeaderKeys.Method]);

        OrderServiceSubmitAsyncArguments? arguments =
            JsonMessageSerializer.Default.Deserialize<OrderServiceSubmitAsyncArguments>(sentBody.Span);

        Assert.Equal(42, arguments!.OrderId);
        Assert.Equal("WIDGET", arguments.ProductCode);
    }

    /// <summary>
    /// A method returning a value goes out as a request carrying the method header and the codec's
    /// content type, and its reply is decoded back into the declared return type.
    /// </summary>
    /// <remarks>
    /// The header assertion is the point. A request that carries no method header is declined by the
    /// dispatcher meant to serve it, so the caller waits out its whole timeout — which a test that
    /// mocks the request and asserts only the decoded reply cannot see.
    /// </remarks>
    [Fact]
    public async Task Proxy_MethodWithResult_SendsRequestUnderMethodHeaderAndDecodesReply()
    {
        var client = new Mock<IMeshClient>();
        MessageHeaders? sentHeaders = null;

        client
            .Setup(c => c.RequestAsync(
                RecipientId,
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<Guid, ReadOnlyMemory<byte>, TimeSpan, MessageHeaders, CancellationToken>(
                (_, _, _, headers, _) => sentHeaders = headers)
            .ReturnsAsync((ReadOnlyMemory<byte>)JsonMessageSerializer.Default.Serialize(99).ToArray());

        var proxy = new OrderServiceProxy(client.Object, JsonMessageSerializer.Default, RecipientId);

        Assert.Equal(99, await proxy.GetTotalAsync(7));
        Assert.Equal($"{OrderContract}.GetTotalAsync", sentHeaders![ContractHeaderKeys.Method]);
        Assert.Equal(
            JsonMessageSerializer.Default.ContentType, sentHeaders[SerializationHeaderKeys.ContentType]);
    }

    /// <summary>
    /// The two generated halves joined: the real proxy's bytes and headers, fed to the real dispatcher,
    /// whose reply completes the proxy's call.
    /// </summary>
    /// <remarks>
    /// Mocking each half against a hand-built version of the other is what let a proxy that never sent
    /// the method header pass a suite in which both halves were tested.
    /// </remarks>
    [Fact]
    public async Task ProxyAndDispatcher_MethodWithResult_RoundTrip()
    {
        var implementation = new RecordingOrderService { TotalToReturn = 123 };
        var replyClient = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> replyBody = default;

        replyClient
            .Setup(c => c.ReplyAsync(
                It.IsAny<MessageReceivedEventArgs>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<MessageReceivedEventArgs, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, body, _, _) => replyBody = body)
            .Returns(Task.CompletedTask);

        var dispatcher = new OrderServiceDispatcher(
            implementation, JsonMessageSerializer.Default, replyClient.Object);

        var client = new Mock<IMeshClient>();

        client
            .Setup(c => c.RequestAsync(
                RecipientId,
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Returns<Guid, ReadOnlyMemory<byte>, TimeSpan, MessageHeaders, CancellationToken>(
                async (_, body, _, headers, token) =>
                {
                    var request = new MessageReceivedEventArgs
                    {
                        SenderId = RecipientId,
                        Data = body,
                        Headers = headers,
                        CorrelationId = 1,
                    };

                    Assert.True(await dispatcher.TryDispatchAsync(request, token));
                    return replyBody;
                });

        var proxy = new OrderServiceProxy(client.Object, JsonMessageSerializer.Default, RecipientId);

        Assert.Equal(123, await proxy.GetTotalAsync(7));
        Assert.Equal(7, implementation.TotalRequestedFor);
    }

    /// <summary>
    /// Acceptance criterion: the dispatcher invokes the matching handler, with the arguments the sender
    /// passed.
    /// </summary>
    [Fact]
    public async Task Dispatcher_VoidMethod_InvokesImplementationWithArguments()
    {
        var implementation = new RecordingOrderService();
        var dispatcher = new OrderServiceDispatcher(
            implementation, JsonMessageSerializer.Default, new Mock<IMeshClient>().Object);

        var body = JsonMessageSerializer.Default.Serialize(
            new OrderServiceSubmitAsyncArguments(7, "BOLT"));

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = body,
            Headers = MethodHeader($"{OrderContract}.SubmitAsync"),
        };

        Assert.True(await dispatcher.TryDispatchAsync(message));
        Assert.Equal((7, "BOLT"), implementation.Submitted);
    }

    /// <summary>
    /// A dispatched method that returns a value replies through the client the dispatcher was built
    /// with, so the calling proxy's awaiting request completes.
    /// </summary>
    [Fact]
    public async Task Dispatcher_MethodWithResult_RepliesWithSerializedResult()
    {
        var implementation = new RecordingOrderService { TotalToReturn = 123 };
        var replyClient = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> replyBody = default;

        replyClient
            .Setup(c => c.ReplyAsync(
                It.IsAny<MessageReceivedEventArgs>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<MessageReceivedEventArgs, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, body, _, _) => replyBody = body)
            .Returns(Task.CompletedTask);

        var dispatcher = new OrderServiceDispatcher(
            implementation, JsonMessageSerializer.Default, replyClient.Object);

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = JsonMessageSerializer.Default.Serialize(new OrderServiceGetTotalAsyncArguments(7)),
            Headers = MethodHeader($"{OrderContract}.GetTotalAsync"),
            CorrelationId = 1,
        };

        Assert.True(await dispatcher.TryDispatchAsync(message));
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
            new RecordingOrderService(), JsonMessageSerializer.Default, new Mock<IMeshClient>().Object);

        var unknown = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = ReadOnlyMemory<byte>.Empty,
            Headers = MethodHeader("NoSuchMethod"),
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
    /// One contract's message is not another's to run, even when both declare a method of that name.
    /// The method header names the contract, so the wrong dispatcher declines instead of deserializing a
    /// foreign record into its own — which succeeds, with every field at its default.
    /// </summary>
    [Fact]
    public async Task Dispatcher_MessageForAnotherContractWithTheSameMethodName_IsDeclined()
    {
        var invoiceClient = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> sentBody = default;
        MessageHeaders? sentHeaders = null;

        invoiceClient
            .Setup(c => c.SendAsync(
                RecipientId,
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<Guid, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, body, headers, _) => (sentBody, sentHeaders) = (body, headers))
            .Returns(Task.CompletedTask);

        var invoiceProxy = new InvoiceServiceProxy(
            invoiceClient.Object, JsonMessageSerializer.Default, RecipientId);

        await invoiceProxy.SubmitAsync(99, "GBP");

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = sentBody,
            Headers = sentHeaders!,
        };

        var orders = new RecordingOrderService();
        var orderDispatcher = new OrderServiceDispatcher(
            orders, JsonMessageSerializer.Default, new Mock<IMeshClient>().Object);

        var invoices = new RecordingInvoiceService();
        var invoiceDispatcher = new InvoiceServiceDispatcher(invoices, JsonMessageSerializer.Default);

        Assert.Equal($"{InvoiceContract}.SubmitAsync", sentHeaders![ContractHeaderKeys.Method]);
        Assert.False(await orderDispatcher.TryDispatchAsync(message));
        Assert.Null(orders.Submitted);

        Assert.True(await invoiceDispatcher.TryDispatchAsync(message));
        Assert.Equal((99, "GBP"), invoices.Submitted);
    }

    /// <summary>
    /// A malformed body from a remote peer is declined, not thrown: the dispatcher runs inside the
    /// client's receive loop, and a decode failure there would take the loop down.
    /// </summary>
    [Fact]
    public async Task Dispatcher_MalformedBody_ReturnsFalseWithoutThrowing()
    {
        var implementation = new RecordingOrderService();
        var dispatcher = new OrderServiceDispatcher(
            implementation, JsonMessageSerializer.Default, new Mock<IMeshClient>().Object);

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = "{ not json"u8.ToArray(),
            Headers = MethodHeader($"{OrderContract}.SubmitAsync"),
        };

        Assert.False(await dispatcher.TryDispatchAsync(message));
        Assert.Null(implementation.Submitted);
    }

    /// <summary>
    /// A body another codec produced is declined rather than fed to this one, because the content type
    /// says whose it is.
    /// </summary>
    [Fact]
    public async Task Dispatcher_BodyFromAnotherCodec_IsDeclined()
    {
        var implementation = new RecordingOrderService();
        var dispatcher = new OrderServiceDispatcher(
            implementation, JsonMessageSerializer.Default, new Mock<IMeshClient>().Object);

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = JsonMessageSerializer.Default.Serialize(new OrderServiceSubmitAsyncArguments(7, "BOLT")),
            Headers = new MessageHeaders(new Dictionary<string, string>
            {
                [ContractHeaderKeys.Method] = $"{OrderContract}.SubmitAsync",
                [SerializationHeaderKeys.ContentType] = "application/x-msgpack",
            }),
        };

        Assert.False(await dispatcher.TryDispatchAsync(message));
        Assert.Null(implementation.Submitted);
    }

    /// <summary>
    /// A method that owes a reply is declined before its implementation runs when the message was an
    /// ordinary send rather than a request. Invoking it and only then discovering the reply cannot be
    /// sent would commit the handler's side effects and throw into the receive loop afterwards.
    /// </summary>
    [Fact]
    public async Task Dispatcher_MethodWithResultSentWithoutCorrelationId_DeclinesBeforeInvoking()
    {
        var implementation = new RecordingOrderService { TotalToReturn = 123 };
        var replyClient = new Mock<IMeshClient>(MockBehavior.Strict);

        var dispatcher = new OrderServiceDispatcher(
            implementation, JsonMessageSerializer.Default, replyClient.Object);

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = JsonMessageSerializer.Default.Serialize(new OrderServiceGetTotalAsyncArguments(7)),
            Headers = MethodHeader($"{OrderContract}.GetTotalAsync"),
        };

        Assert.False(await dispatcher.TryDispatchAsync(message));
        Assert.Null(implementation.TotalRequestedFor);
    }

    /// <summary>
    /// A parameterless method needs no argument record, and round-trips through both halves without
    /// one.
    /// </summary>
    [Fact]
    public async Task ProxyAndDispatcher_ParameterlessMethod_RoundTrip()
    {
        var implementation = new RecordingOrderService { DescriptionToReturn = "all good" };
        var replyClient = new Mock<IMeshClient>();
        ReadOnlyMemory<byte> replyBody = default;

        replyClient
            .Setup(c => c.ReplyAsync(
                It.IsAny<MessageReceivedEventArgs>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<MessageHeaders>(),
                It.IsAny<CancellationToken>()))
            .Callback<MessageReceivedEventArgs, ReadOnlyMemory<byte>, MessageHeaders, CancellationToken>(
                (_, body, _, _) => replyBody = body)
            .Returns(Task.CompletedTask);

        var dispatcher = new OrderServiceDispatcher(
            implementation, JsonMessageSerializer.Default, replyClient.Object);

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = ReadOnlyMemory<byte>.Empty,
            Headers = MethodHeader($"{OrderContract}.DescribeAsync"),
            CorrelationId = 1,
        };

        Assert.True(await dispatcher.TryDispatchAsync(message));
        Assert.Equal("all good", JsonMessageSerializer.Default.Deserialize<string>(replyBody.Span));
    }

    /// <summary>
    /// The shapes that used to emit uncompilable code still round-trip: a keyword-named method and
    /// parameter, a parameter named after a generated local, and a non-token parameter called
    /// <c>cancellationToken</c>.
    /// </summary>
    [Fact]
    public async Task ProxyAndDispatcher_AwkwardNames_RoundTrip()
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

        var proxy = new AwkwardContractProxy(client.Object, JsonMessageSerializer.Default, RecipientId);
        await proxy.@event(null, 5, "not a token");

        var implementation = new RecordingAwkwardContract();
        var dispatcher = new AwkwardContractDispatcher(
            implementation, JsonMessageSerializer.Default, new Mock<IMeshClient>().Object);

        var message = new MessageReceivedEventArgs
        {
            SenderId = RecipientId,
            Data = sentBody,
            Headers = sentHeaders!,
        };

        Assert.True(await dispatcher.TryDispatchAsync(message));
        Assert.Equal((null, 5, "not a token"), implementation.Received);
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

    private static MessageHeaders MethodHeader(string value)
    {
        return new MessageHeaders(
            new Dictionary<string, string> { [ContractHeaderKeys.Method] = value });
    }

    private sealed class RecordingOrderService : IOrderService
    {
        public (int OrderId, string ProductCode)? Submitted { get; private set; }

        public int? TotalRequestedFor { get; private set; }

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
            TotalRequestedFor = orderId;
            return Task.FromResult(TotalToReturn);
        }

        public Task<string> DescribeAsync() => Task.FromResult(DescriptionToReturn);
    }

    private sealed class RecordingInvoiceService : IInvoiceService
    {
        public (int Reference, string Currency)? Submitted { get; private set; }

        public Task SubmitAsync(int reference, string currency)
        {
            Submitted = (reference, currency);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingAwkwardContract : IAwkwardContract
    {
        public (string? Note, int Arguments, string CancellationToken)? Received { get; private set; }

        public Task @event(
            string? note, int __arguments, string cancellationToken, CancellationToken ct = default)
        {
            Received = (note, __arguments, cancellationToken);
            return Task.CompletedTask;
        }

        public Task<int> ComputeAsync(int @class) => Task.FromResult(@class * 2);
    }
}
