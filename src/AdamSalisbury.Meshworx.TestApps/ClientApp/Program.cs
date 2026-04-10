// See https://aka.ms/new-console-template for more information
using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Logging;

Console.WriteLine("Starting Client");

using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
var typedLogger = loggerFactory.CreateLogger<MeshClient>();
var meshClient = new MeshClient(typedLogger);

var transport = await TcpTransport.ConnectAsync("localhost", 22001).ConfigureAwait(false);
await meshClient.ConnectAsync(transport, "TestClient").ConfigureAwait(false);

Console.WriteLine("Hit Enter to Exit");
Console.ReadLine();
