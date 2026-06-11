using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Logging;

using var loggerFactory = LoggerFactory.Create(builder =>
    builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

var logger = loggerFactory.CreateLogger<MeshHub>();
var listener = new TcpTransportListener(22001);
await using var hub = new MeshHub(logger, listener);

hub.ClientConnected += (_, e) =>
    Console.WriteLine($"[+] \"{e.ClientName}\" connected — {hub.ConnectedClientCount} client(s) online");

hub.ClientDisconnected += (_, e) =>
    Console.WriteLine($"[-] \"{e.ClientName}\" disconnected — {hub.ConnectedClientCount} client(s) online");

await hub.StartAsync();
Console.WriteLine("Hub is running on port 22001. Press Enter to stop.");
Console.ReadLine();
