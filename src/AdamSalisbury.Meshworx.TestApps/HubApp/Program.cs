// See https://aka.ms/new-console-template for more information
using AdamSalisbury.Meshworx;

Console.WriteLine("Starting Hub");

var meshHub = new MeshHub(22001);

await meshHub.StartAsync();

Console.WriteLine("Hit Enter to Exit");
Console.ReadLine();