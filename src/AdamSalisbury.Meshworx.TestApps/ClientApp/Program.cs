// See https://aka.ms/new-console-template for more information
using System.Reflection;
using AdamSalisbury.Meshworx;
using Microsoft.Extensions.Logging;

Console.WriteLine("Starting Client");

using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var typedLogger = loggerFactory.CreateLogger<MeshClient>();
var meshClient = new MeshClient(typedLogger);

await meshClient.ConnectAsync("localhost", 22001).ConfigureAwait(false);

Console.WriteLine("Hit Enter to Exit");
Console.ReadLine();
