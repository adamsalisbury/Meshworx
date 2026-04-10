// See https://aka.ms/new-console-template for more information
using AdamSalisbury.Meshworx;

Console.WriteLine("Starting Client");

var meshClient = new MeshClient();

await meshClient.ConnectAsync("localhost", 22001);

Console.WriteLine("Hit Enter to Exit");
Console.ReadLine();