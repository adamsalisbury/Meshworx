using System.Text;
using AdamSalisbury.Meshworx;
using AdamSalisbury.Meshworx.Transport.Tcp;
using Microsoft.Extensions.Logging;

using var loggerFactory = LoggerFactory.Create(builder =>
    builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

var logger = loggerFactory.CreateLogger<MeshClient>();
await using var client = new MeshClient(logger);

// --- Connect ---

Console.Write("Enter your name: ");
string name = Console.ReadLine()?.Trim() ?? string.Empty;

if (string.IsNullOrEmpty(name))
{
    Console.WriteLine("Name cannot be empty.");
    return;
}

try
{
    var transport = await TcpTransport.ConnectAsync("localhost", 22001);
    await client.ConnectAsync(transport, name);
}
catch (RegistrationRefusedException ex)
{
    Console.WriteLine($"Registration refused: {ex.ErrorCode}");
    return;
}
catch (Exception ex)
{
    Console.WriteLine($"Failed to connect: {ex.Message}");
    return;
}

Console.WriteLine($"Connected as \"{client.Name}\" (id: {client.Id})");
Console.WriteLine();

// --- Listen for incoming messages ---

client.MessageReceived += (_, args) =>
{
    string text = Encoding.UTF8.GetString(args.Data.Span);
    Console.WriteLine();
    Console.WriteLine($"[Message from {args.SenderId}]: {text}");
    Console.Write("> ");
};

client.Disconnected += (_, args) =>
{
    Console.WriteLine();
    Console.WriteLine($"[Disconnected from hub: {args.Reason}. Press Enter to exit.]");
};

// --- Send messages ---

Console.WriteLine("Type a message in the format:  recipient-name: message");
Console.WriteLine("Press Enter on an empty line to quit.");
Console.WriteLine();

while (true)
{
    Console.Write("> ");
    string? input = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(input))
    {
        break;
    }

    int separatorIndex = input.IndexOf(':');
    if (separatorIndex < 1)
    {
        Console.WriteLine("Format: recipient-name: message");
        continue;
    }

    string recipientName = input[..separatorIndex].Trim();
    string messageText = input[(separatorIndex + 1)..].Trim();

    if (string.IsNullOrEmpty(messageText))
    {
        Console.WriteLine("Message cannot be empty.");
        continue;
    }

    Guid? recipientId = await client.GetClientIdByNameAsync(recipientName);
    if (recipientId is null)
    {
        Console.WriteLine($"No client named \"{recipientName}\" is connected.");
        continue;
    }

    await client.SendAsync(recipientId.Value, Encoding.UTF8.GetBytes(messageText));
}
