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

client.GroupMessageReceived += (_, args) =>
{
    string text = Encoding.UTF8.GetString(args.Data.Span);
    Console.WriteLine();
    Console.WriteLine($"[Group \"{args.GroupName}\" from {args.SenderId}]: {text}");
    Console.Write("> ");
};

client.Disconnected += (_, args) =>
{
    Console.WriteLine();
    Console.WriteLine($"[Disconnected from hub: {args.Reason}. Press Enter to exit.]");
};

// --- Send messages ---

Console.WriteLine("Commands:");
Console.WriteLine("  <name>: <message>      send a direct message");
Console.WriteLine("  /all <message>         broadcast to every other client");
Console.WriteLine("  /join <group>          join a group");
Console.WriteLine("  /leave <group>         leave a group");
Console.WriteLine("  /g <group> <message>   send a message to a group");
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

    input = input.Trim();

    if (input.StartsWith("/all ", StringComparison.Ordinal))
    {
        string message = input["/all ".Length..].Trim();
        if (message.Length == 0)
        {
            Console.WriteLine("Message cannot be empty.");
            continue;
        }

        await client.BroadcastAsync(Encoding.UTF8.GetBytes(message));
        continue;
    }

    if (input.StartsWith("/join ", StringComparison.Ordinal))
    {
        string group = input["/join ".Length..].Trim();
        if (group.Length == 0)
        {
            Console.WriteLine("Group name cannot be empty.");
            continue;
        }

        await client.JoinGroupAsync(group);
        Console.WriteLine($"Joined \"{group}\". Member of: {string.Join(", ", client.JoinedGroups)}");
        continue;
    }

    if (input.StartsWith("/leave ", StringComparison.Ordinal))
    {
        string group = input["/leave ".Length..].Trim();
        if (group.Length == 0)
        {
            Console.WriteLine("Group name cannot be empty.");
            continue;
        }

        await client.LeaveGroupAsync(group);
        Console.WriteLine($"Left \"{group}\".");
        continue;
    }

    if (input.StartsWith("/g ", StringComparison.Ordinal))
    {
        string rest = input["/g ".Length..].Trim();
        int spaceIndex = rest.IndexOf(' ', StringComparison.Ordinal);
        if (spaceIndex < 1)
        {
            Console.WriteLine("Format: /g <group> <message>");
            continue;
        }

        string group = rest[..spaceIndex];
        string message = rest[(spaceIndex + 1)..].Trim();
        if (message.Length == 0)
        {
            Console.WriteLine("Message cannot be empty.");
            continue;
        }

        await client.SendToGroupAsync(group, Encoding.UTF8.GetBytes(message));
        continue;
    }

    int separatorIndex = input.IndexOf(':', StringComparison.Ordinal);
    if (separatorIndex < 1)
    {
        Console.WriteLine("Unknown input. Use \"<name>: <message>\" or /all, /join, /leave, /g.");
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
