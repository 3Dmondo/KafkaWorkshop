using System.Diagnostics;
using Chat;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) => {
  e.Cancel = true;
  cts.Cancel();
};

var st = new Stopwatch();

const int Messages = 10_000;

var ProducerProduceAsync = new MessageProducer();
st = new Stopwatch();
st.Start();
await ProducerProduceAsync.ProduceAsync(Messages);
ProducerProduceAsync.Dispose();
st.Stop();
Console.WriteLine("ProduceAsync " + st.Elapsed.ToString());

var ProducerProduce = new MessageProducer();
st = new Stopwatch();
st.Start();
ProducerProduce.Produce(Messages);
ProducerProduce.Dispose();
st.Stop();
Console.WriteLine("Produce " + st.Elapsed.ToString());



//await Task.WhenAll(
//  Task.Run(() => Producer.SendMessages(10_000)),
//  Task.Run(() => Consumer.ConsumeAll(cts.Token))
//  );