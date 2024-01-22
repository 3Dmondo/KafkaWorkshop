using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using static Streamiz.Kafka.Net.KafkaStream;

namespace Aggregator
{
  internal class CharCounter : IDisposable
  {
    private const string StoreName = "countStore1";
    private KafkaStream stream;

    public CharCounter()
    {
      var config = new StreamConfig<StringSerDes, StringSerDes> {
        ApplicationId = "aggregator",
        BootstrapServers = Common.Constants.KafkaHost,
        AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
        AllowAutoCreateTopics = true,
        Guarantee = ProcessingGuarantee.EXACTLY_ONCE,
        Logger = LoggerFactory.Create(b => b.ClearProviders()),

      };
      stream = new KafkaStream(BuildTopologyReduce(), config);
      stream.StateChanged += (oldState, newState) => State = newState;
    }

    public State State { get; private set; }

    public async Task WaitUntilRunningAsync()
    {
      while (State != State.RUNNING)
        await Task.Delay(10);
    }

    async public Task StartAsync(CancellationToken token) =>
      await stream.StartAsync(token);

    private static Topology BuildTopologyAggregate()
    {
      var builder = new StreamBuilder();
      builder
        .Stream<string, string>(Common.Constants.ChatTopic)
        .Map((k, v) => new KeyValuePair<int, string>(1, v))
        .GroupByKey<Int32SerDes, StringSerDes>()
        .Aggregate(
          () => 0,
          (key, value, aggregateValue) => {
            var result = value.Length + aggregateValue;
            return result;
          },
          Materialized<int, int, IKeyValueStore<Bytes, byte[]>>.Create(StoreName))
        .ToStream()
        .Foreach((k, v) => Console.WriteLine($"Char count for {k} is {v}"));
      return builder.Build();
    }

    private static Topology BuildTopologyReduce()
    {
      var builder = new StreamBuilder();
      builder
        .Stream<string, string>(Common.Constants.ChatTopic)
        .Map((k, v) => new KeyValuePair<int, int>(1, v.Length))
        .GroupByKey<Int32SerDes, Int32SerDes>()
        .Reduce(
          (value, aggregateValue) => value + aggregateValue,
          Materialized<int, int, IKeyValueStore<Bytes, byte[]>>.Create(StoreName))
        .ToStream()
        .Foreach((k, v) => Console.WriteLine($"Char count for {k} is {v}"));
      return builder.Build();
    }

    public IEnumerable<KeyValuePair<int, int>> StoreValues()
    {
      try {
        var store = stream.Store(
          StoreQueryParameters.FromNameAndType(
            StoreName, 
            QueryableStoreTypes.KeyValueStore<int, int>()));
        return store.All();
      } catch (Exception e) {
        Console.WriteLine(e.Message);
        Console.WriteLine(e.StackTrace);
        throw;
      }
    }

    public void Dispose()
    {
      stream.Dispose();
    }
  }
}
