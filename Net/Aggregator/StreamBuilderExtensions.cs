using System.IO;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Aggregator;

internal static class StreamBuilderExtensions
{
  public static IKStream<string, string> AddGlobalCharCounter(
    this IKStream<string, string> stream,
    string storeName)
  {
    stream
      .Map((k, v) => new KeyValuePair<int, int>(1, v.Length))
      .GroupByKey<Int32SerDes, Int32SerDes>()
      .Reduce(
        (value, aggregateValue) => value + aggregateValue,
        Materialized<int, int, IKeyValueStore<Bytes, byte[]>>.Create(storeName))
      .ToStream()
      .Foreach((k, v) => Console.WriteLine($"Global char count is {v}"));
    return stream;
  }

  public static IKStream<string, string> AddCharCounter(
    this IKStream<string, string> stream,
    string storeName)
  {
    stream
      .GroupByKey<StringSerDes, StringSerDes>()
      .Aggregate(
        () => 0,
        (key, value, aggregateValue) => {
          var result = value.Length + aggregateValue;
          return result;
        },
        Materialized<string, int, IKeyValueStore<Bytes, byte[]>>
          .Create<StringSerDes, Int32SerDes>(storeName))
      .ToStream()
      .Foreach((k, v) => Console.WriteLine($"Char count for {k} is {v}"));
    return stream;
  }

  public static IKStream<string, string> AddWordCounter(
    this IKStream<string, string> stream,
    string storeName)
  {
    stream
      .FlatMapValues(s => s.Split(' '))
      .GroupByKey<StringSerDes, StringSerDes>()
      .Aggregate(
        () => 0,
        (key, value, aggregateValue) => {
          var result = aggregateValue + 1;
          return result;
        },
        Materialized<string, int, IKeyValueStore<Bytes, byte[]>>
          .Create<StringSerDes, Int32SerDes>(storeName))
      .ToStream()
      .Foreach((k, v) => Console.WriteLine($"Word count for {k} is {v}"));
    return stream;
  }
}
