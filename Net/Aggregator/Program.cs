using Aggregator;
using static Streamiz.Kafka.Net.KafkaStream;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) => {
  e.Cancel = true;
  cts.Cancel();
};
Console.WriteLine("press CTRL-C to close.");

using var charCounter = new ChatAggregator();
await charCounter.StartAsync(cts.Token);

await charCounter.WaitUntilRunningAsync();

while (charCounter.State == State.RUNNING) {
  foreach (var item in charCounter.StoreValues())
    Console.WriteLine($"Local store char count for {item.Key} is {item.Value}");
  await Task.Delay(10_000);
}

while (charCounter.State != State.NOT_RUNNING)
  await Task.Delay(1000);

Console.WriteLine("Terminated");
