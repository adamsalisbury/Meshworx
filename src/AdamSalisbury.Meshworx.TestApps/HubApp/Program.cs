// See https://aka.ms/new-console-template for more information
using AdamSalisbury.Meshworx;
using Microsoft.Extensions.Logging;

Console.WriteLine("Starting Hub");

using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var typedLogger = loggerFactory.CreateLogger<MeshHub>();

var meshHub = new MeshHub(typedLogger, 22001);
await meshHub.StartAsync().ConfigureAwait(false);

Console.WriteLine("Hit Enter to Exit");
Console.ReadLine();
