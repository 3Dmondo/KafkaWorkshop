using Chat;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) =>
{
  e.Cancel = true;
  cts.Cancel();
};
Console.WriteLine("press CTRL-C to close.");

var name = "";
while (string.IsNullOrWhiteSpace(name))
{
  Console.Write("Please type your name: ");
  name = Console.ReadLine();
}

using var streamer = new MessageProducer(name);
using var consumer = new MessageConsumer();

Console.WriteLine("You can start typing your messages");

await Task.WhenAll(
  MessageHandler.StreamConsoleInput(cts.Token, streamer),
  MessageHandler.PrintMessagesAsync(cts.Token, consumer));

Console.WriteLine("Terminated");

