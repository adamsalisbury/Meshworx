// See https://aka.ms/new-console-template for more information
using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Logging;

Console.WriteLine("Starting Hub");

using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
var typedLogger = loggerFactory.CreateLogger<MeshHub>();

var listener = new TcpTransportListener(22001);
var meshHub = new MeshHub(typedLogger, listener);
await meshHub.StartAsync().ConfigureAwait(false);

Console.WriteLine("Hit Enter to Exit");
Console.ReadLine();
