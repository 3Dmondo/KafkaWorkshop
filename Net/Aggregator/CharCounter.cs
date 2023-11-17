using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Aggregator
{
  internal class CharCounter : IDisposable
  {
    private KafkaStream stream;
    public CharCounter()
    {
      var config = new StreamConfig<StringSerDes, StringSerDes>
      {
        ApplicationId = "aggregator",
        BootstrapServers = Common.Constants.KafkaHost,
        AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
        AllowAutoCreateTopics = true,
        Guarantee = ProcessingGuarantee.EXACTLY_ONCE,
        Logger = LoggerFactory.Create(b => b.ClearProviders()),
      };
      stream = new KafkaStream(BuildTopology(), config);
    }

    async public Task StartAsync(CancellationToken token) =>
      await stream.StartAsync(token);

    private static Topology BuildTopology()
    {
      var builder = new StreamBuilder();
      builder
        .Stream<string, string>(Common.Constants.ChatTopic)
        .Map((k, v) => new KeyValuePair<int, int>(1, v.Length))
        .GroupByKey<Int32SerDes, Int32SerDes>()
        .Aggregate<int, Int32SerDes>(
          () => 0,
          (n0, n1, n2) =>
          {
            var value = n1 + n2;
            Console.WriteLine(value);
            return value;
          });
      return builder.Build();
    }

    public void Dispose()
    {
      stream.Dispose();
    }
  }
}
