// See https://aka.ms/new-console-template for more information
using AdamSalisbury.Meshworx;
using Microsoft.Extensions.Logging;

Console.WriteLine("Starting Client");

using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
var typedLogger = loggerFactory.CreateLogger<MeshClient>();
var meshClient = new MeshClient(typedLogger);

await meshClient.ConnectAsync("localhost", 22001, "TestClient").ConfigureAwait(false);

Console.WriteLine("Hit Enter to Exit");
Console.ReadLine();
