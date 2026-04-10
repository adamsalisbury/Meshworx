using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Logging;

using var loggerFactory = LoggerFactory.Create(builder =>
    builder.AddConsole().SetMinimumLevel(LogLevel.Information));

var logger = loggerFactory.CreateLogger<MeshHub>();
var listener = new TcpTransportListener(22001);
await using var hub = new MeshHub(logger, listener);

await hub.StartAsync();
Console.WriteLine("Hub is running on port 22001. Press Enter to stop.");
Console.ReadLine();
