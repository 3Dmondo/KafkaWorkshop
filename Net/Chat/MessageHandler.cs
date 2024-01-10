using System.Text;
namespace Chat
{
  internal static class MessageHandler
  {

    public static async Task PrintMessagesAsync(CancellationToken token, MessageConsumer consumer)
    {
      await Task.Run(() => {
        try {
          foreach (var consumerResult in consumer.ConsumeAsync(token)) {
            var consoleMessage = $"{consumerResult.Message.Key}: {consumerResult.Message.Value}";
            Console.CursorLeft = Console.BufferWidth - consoleMessage.Length;
            Console.WriteLine(consoleMessage);
          }
        } catch (OperationCanceledException) { }
      });
    }

    public static async Task StreamConsoleInput(CancellationToken token, MessageProducer messageProducer)
    {
      while (!token.IsCancellationRequested) {
        var message = await ReadLineAsync(token);
        if (!string.IsNullOrEmpty(message))
          messageProducer.SendMessage(message);
      }
    }

    private static async Task<string> ReadLineAsync(CancellationToken token)
    {
      StringBuilder input = new StringBuilder();
      while (!token.IsCancellationRequested) {
        while (Console.KeyAvailable) {
          ConsoleKeyInfo key = Console.ReadKey(true);
          if (key.Key == ConsoleKey.Enter) {
            Console.WriteLine();
            return input.ToString();
          } else {
            input.Append(key.KeyChar);
            Console.Write(key.KeyChar);
          }
        }
        await Task.Delay(10);
      }
      return string.Empty;
    }
  }
}
