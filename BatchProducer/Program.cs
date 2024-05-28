using System.Diagnostics;
using Chat;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) => {
  e.Cancel = true;
  cts.Cancel();
};

var st = new Stopwatch();

const int Messages = 10_000;

var ProducerProduce = new MessageProducer();
st = new Stopwatch();
st.Start();
ProducerProduce.ProduceWithFlush(Messages);
ProducerProduce.Dispose();
st.Stop();
Console.WriteLine("Produce " + st.Elapsed.ToString());

var ProduceWithTryCatch = new MessageProducer();
st = new Stopwatch();
st.Start();
ProduceWithTryCatch.ProduceWithTryCatch(Messages);
ProduceWithTryCatch.Dispose();
st.Stop();
Console.WriteLine("ProduceAsync " + st.Elapsed.ToString());

//var ProducerProduceAsync = new MessageProducer();
//st = new Stopwatch();
//st.Start();
//await ProducerProduceAsync.ProduceAsync(Messages);
//ProducerProduceAsync.Dispose();
//st.Stop();
//Console.WriteLine("ProduceAsync " + st.Elapsed.ToString());



//await Task.WhenAll(
//  Task.Run(() => Producer.SendMessages(10_000)),
//  Task.Run(() => Consumer.ConsumeAll(cts.Token))
//  );