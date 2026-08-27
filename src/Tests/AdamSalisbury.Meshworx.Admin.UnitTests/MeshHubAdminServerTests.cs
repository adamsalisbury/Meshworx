using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using AdamSalisbury.Meshworx.Transport.InMemory;
using Microsoft.Extensions.Logging;
using Moq;

namespace AdamSalisbury.Meshworx.Admin.UnitTests;

/// <summary>
/// End-to-end tests for <see cref="MeshHubAdminServer"/>, exercised over real loopback HTTP against a
/// real <see cref="MeshHub"/> and real <see cref="MeshClient"/> connections rather than mocks.
/// </summary>
public sealed class MeshHubAdminServerTests : IAsyncLifetime, IDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly HttpClient _httpClient = new();
    private readonly List<IAsyncDisposable> _disposables = [];

    // xUnit only awaits IAsyncLifetime.DisposeAsync between tests, not a bare IAsyncDisposable — without
    // this, every test's hub, admin server and clients would leak past that test's own lifetime, since
    // nothing would ever actually call their teardown. IDisposable.Dispose is implemented alongside it
    // purely to own _httpClient (CA1001), and is never relied upon by itself.
    public Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        foreach (IAsyncDisposable disposable in _disposables)
        {
            await disposable.DisposeAsync();
        }
    }

    public void Dispose()
    {
        _httpClient.Dispose();
    }

    /// <summary>
    /// Finds a free loopback port the same way every real caller of this class must: <see cref="HttpListener"/>
    /// has no "bind to port 0" auto-assignment the way a <see cref="Socket"/> does, so a port has to be
    /// chosen up front. A brief bind-and-release on an ordinary <see cref="TcpListener"/> is the standard
    /// way to find one very likely to still be free by the time <see cref="MeshHubAdminServer.StartAsync"/>
    /// binds it for real.
    /// </summary>
    private static int FindFreeLoopbackPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint!).Port;
        listener.Stop();
        return port;
    }

    private async Task<(MeshHub Hub, InMemoryTransportListener Listener, Uri BaseAddress)> StartHubWithAdminServerAsync(
        AdminRequestAuthenticator? authenticator = null)
    {
        var listener = new InMemoryTransportListener();
        var hub = new MeshHub(new Mock<ILogger<MeshHub>>().Object, listener);
        await hub.StartAsync();
        _disposables.Add(hub);

        var baseAddress = new Uri($"http://127.0.0.1:{FindFreeLoopbackPort()}/");
        var adminServer = new MeshHubAdminServer(
            hub, baseAddress, authenticator ?? ((_, _) => ValueTask.FromResult(true)));
        await adminServer.StartAsync();
        _disposables.Add(adminServer);

        return (hub, listener, baseAddress);
    }

    private async Task<MeshClient> ConnectClientAsync(InMemoryTransportListener listener, string name)
    {
        var client = new MeshClient(new Mock<ILogger<MeshClient>>().Object);
        await client.ConnectAsync(listener.Connect(), name);
        _disposables.Add(client);
        return client;
    }

    /// <summary>
    /// GET /clients returns the same snapshot <see cref="IMeshHub.GetClients"/> itself reports, as JSON.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task GetClients_ReturnsConnectedClientsAsJson()
    {
        (MeshHub hub, InMemoryTransportListener listener, Uri baseAddress) =
            await StartHubWithAdminServerAsync();

        MeshClient alice = await ConnectClientAsync(listener, "Alice");
        await alice.JoinGroupAsync("team");
        await alice.GetClientIdByNameAsync("Alice"); // barrier: join applied

        using HttpResponseMessage response = await _httpClient.GetAsync(new Uri(baseAddress, "clients"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var clients = JsonSerializer.Deserialize<List<ConnectedClientInfo>>(
            await response.Content.ReadAsStringAsync(), SerializerOptions);

        ConnectedClientInfo aliceInfo = Assert.Single(clients!, c => c.Id == alice.Id);
        Assert.Equal("Alice", aliceInfo.Name);
        Assert.Equal(["team"], aliceInfo.Groups);

        await hub.StopAsync();
    }

    /// <summary>
    /// GET /groups returns the same snapshot <see cref="IMeshHub.GetGroups"/> itself reports, as JSON.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task GetGroups_ReturnsGroupsAsJson()
    {
        (MeshHub hub, InMemoryTransportListener listener, Uri baseAddress) =
            await StartHubWithAdminServerAsync();

        MeshClient alice = await ConnectClientAsync(listener, "Alice");
        await alice.JoinGroupAsync("team");
        await alice.GetClientIdByNameAsync("Alice"); // barrier

        using HttpResponseMessage response = await _httpClient.GetAsync(new Uri(baseAddress, "groups"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var groups = JsonSerializer.Deserialize<List<GroupInfo>>(
            await response.Content.ReadAsStringAsync(), SerializerOptions);

        GroupInfo team = Assert.Single(groups!, g => g.Name == "team");
        Assert.Equal([alice.Id], team.MemberIds);

        await hub.StopAsync();
    }

    /// <summary>
    /// GET /topics returns the same snapshot <see cref="IMeshHub.GetTopics"/> itself reports, as JSON.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task GetTopics_ReturnsTopicsAsJson()
    {
        (MeshHub hub, InMemoryTransportListener listener, Uri baseAddress) =
            await StartHubWithAdminServerAsync();

        MeshClient alice = await ConnectClientAsync(listener, "Alice");
        await alice.SubscribeAsync("orders.#");
        await alice.GetClientIdByNameAsync("Alice"); // barrier

        using HttpResponseMessage response = await _httpClient.GetAsync(new Uri(baseAddress, "topics"));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var topics = JsonSerializer.Deserialize<List<TopicSubscriptionInfo>>(
            await response.Content.ReadAsStringAsync(), SerializerOptions);

        TopicSubscriptionInfo orders = Assert.Single(topics!, t => t.Pattern == "orders.#");
        Assert.Equal([alice.Id], orders.SubscriberIds);

        await hub.StopAsync();
    }

    /// <summary>
    /// POST /clients/{id}/disconnect, with a reason in the JSON body, disconnects the named client and
    /// carries that reason through to <see cref="IMeshHub.ClientDisconnected"/> — the same behaviour
    /// <see cref="IMeshHub.DisconnectClient"/> itself has, now reached over HTTP.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task PostDisconnect_ConnectedClient_DisconnectsWithReason()
    {
        (MeshHub hub, InMemoryTransportListener listener, Uri baseAddress) =
            await StartHubWithAdminServerAsync();

        MeshClient target = await ConnectClientAsync(listener, "Target");

        // Captured before disconnecting: MeshClient.Id resets once the client notices its own connection
        // has closed, and that can race the hub-side ClientDisconnected event this test also waits on.
        Guid targetId = target.Id;

        var disconnectedTcs = new TaskCompletionSource<ClientConnectionEventArgs>();
        hub.ClientDisconnected += (_, e) =>
        {
            if (e.ClientId == targetId)
            {
                disconnectedTcs.TrySetResult(e);
            }
        };

        using var requestBody = new StringContent(
            """{"reason":"kicked via admin API"}""", Encoding.UTF8, "application/json");
        using HttpResponseMessage response = await _httpClient.PostAsync(
            new Uri(baseAddress, $"clients/{targetId}/disconnect"), requestBody);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = JsonSerializer.Deserialize<DisconnectClientResponse>(
            await response.Content.ReadAsStringAsync(), SerializerOptions);
        Assert.True(body!.Disconnected);

        ClientConnectionEventArgs disconnected = await disconnectedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Equal("kicked via admin API", disconnected.Reason);

        await hub.StopAsync();
    }

    /// <summary>
    /// POST /clients/{id}/disconnect with no body at all still succeeds, disconnecting the client with no
    /// reason recorded — the reason is optional, not required.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task PostDisconnect_NoBody_StillDisconnectsWithNoReason()
    {
        (MeshHub hub, InMemoryTransportListener listener, Uri baseAddress) =
            await StartHubWithAdminServerAsync();

        MeshClient target = await ConnectClientAsync(listener, "Target");

        // Captured before disconnecting: MeshClient.Id resets once the client notices its own connection
        // has closed, and that can race the hub-side ClientDisconnected event this test also waits on.
        Guid targetId = target.Id;

        var disconnectedTcs = new TaskCompletionSource<ClientConnectionEventArgs>();
        hub.ClientDisconnected += (_, e) =>
        {
            if (e.ClientId == targetId)
            {
                disconnectedTcs.TrySetResult(e);
            }
        };

        using HttpResponseMessage response = await _httpClient.PostAsync(
            new Uri(baseAddress, $"clients/{targetId}/disconnect"), content: null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        ClientConnectionEventArgs disconnected = await disconnectedTcs.Task.WaitAsync(WaitTimeout);
        Assert.Null(disconnected.Reason);

        await hub.StopAsync();
    }

    /// <summary>
    /// Disconnecting an id that names no connected client is reported as a 404, not a 200 or a fault.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task PostDisconnect_UnknownClientId_ReturnsNotFound()
    {
        (MeshHub hub, _, Uri baseAddress) = await StartHubWithAdminServerAsync();

        using HttpResponseMessage response = await _httpClient.PostAsync(
            new Uri(baseAddress, $"clients/{Guid.NewGuid()}/disconnect"), content: null);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        await hub.StopAsync();
    }

    /// <summary>
    /// A request the authenticator refuses never reaches the hub at all — it is answered with 401 before
    /// any route is even considered.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task Request_AuthenticatorRefuses_ReturnsUnauthorized()
    {
        (MeshHub hub, _, Uri baseAddress) =
            await StartHubWithAdminServerAsync((_, _) => ValueTask.FromResult(false));

        using HttpResponseMessage response = await _httpClient.GetAsync(new Uri(baseAddress, "clients"));

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);

        await hub.StopAsync();
    }

    /// <summary>
    /// The authenticator is handed the request's method, path and Authorization header value unchanged,
    /// so it can make its own decision from them — proved here by admitting only a specific bearer token.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task Request_AuthenticatorReceivesMethodPathAndAuthorizationHeader()
    {
        const string expectedToken = "secret-token";

        (MeshHub hub, _, Uri baseAddress) = await StartHubWithAdminServerAsync((context, _) =>
            ValueTask.FromResult(
                context.Method == "GET"
                && context.Path == "/clients"
                && context.AuthorizationHeaderValue == $"Bearer {expectedToken}"));

        using var authorisedRequest = new HttpRequestMessage(HttpMethod.Get, new Uri(baseAddress, "clients"));
        authorisedRequest.Headers.Add("Authorization", $"Bearer {expectedToken}");
        using HttpResponseMessage authorisedResponse = await _httpClient.SendAsync(authorisedRequest);
        Assert.Equal(HttpStatusCode.OK, authorisedResponse.StatusCode);

        using var wrongTokenRequest = new HttpRequestMessage(HttpMethod.Get, new Uri(baseAddress, "clients"));
        wrongTokenRequest.Headers.Add("Authorization", "Bearer wrong-token");
        using HttpResponseMessage wrongTokenResponse = await _httpClient.SendAsync(wrongTokenRequest);
        Assert.Equal(HttpStatusCode.Unauthorized, wrongTokenResponse.StatusCode);

        await hub.StopAsync();
    }

    /// <summary>
    /// A route this server does not recognise is answered with 404 rather than a fault, once the request
    /// has cleared authentication.
    /// </summary>
    [Fact(Timeout = 10000)]
    public async Task UnrecognisedRoute_ReturnsNotFound()
    {
        (MeshHub hub, _, Uri baseAddress) = await StartHubWithAdminServerAsync();

        using HttpResponseMessage response = await _httpClient.GetAsync(new Uri(baseAddress, "nonexistent"));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        await hub.StopAsync();
    }

    /// <summary>
    /// The constructor refuses a null authenticator rather than silently admitting every request — there
    /// is no unauthenticated default.
    /// </summary>
    [Fact]
    public void Constructor_NullAuthenticator_ThrowsArgumentNullException()
    {
        var hub = new Mock<IMeshHub>();

        Assert.Throws<ArgumentNullException>(
            () => new MeshHubAdminServer(hub.Object, new Uri("http://127.0.0.1:9999/"), null!));
    }
}
