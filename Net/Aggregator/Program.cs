using Aggregator;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) =>
{
  e.Cancel = true;
  cts.Cancel();
};
Console.WriteLine("press CTRL-C to close.");

using var charCounter = new CharCounter();
await charCounter.StartAsync(cts.Token);

while (!cts.IsCancellationRequested)
  await Task.Delay(10);

Console.WriteLine("Terminated");
