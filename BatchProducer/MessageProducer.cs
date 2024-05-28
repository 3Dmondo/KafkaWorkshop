using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Chat
{
  internal class MessageProducer : IDisposable
  {
    private readonly IProducer<byte[], byte[]> producer;

    const int maxKBytes = 1024;

    public MessageProducer()
    {
      var producerConfig = new ProducerConfig {
        BootstrapServers = Common.Constants.KafkaHost,
        AllowAutoCreateTopics = true,
        QueueBufferingMaxKbytes = maxKBytes,

      };
      producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();
    }

    public void CreateTopicIfNotExists()
    {
      var adminClientBuilder = new AdminClientBuilder(
        new AdminClientConfig {
          BootstrapServers = Common.Constants.KafkaHost
        });

      using var adminClient = adminClientBuilder.Build();
      var topicNames = adminClient.
        GetMetadata(TimeSpan.FromSeconds(2)).Topics.
        Select(x => x.Topic).ToList();
      if (topicNames.Any(n => n == Common.Constants.ByteTopic))
        adminClient.CreateTopicsAsync(new TopicSpecification[] {
            new TopicSpecification {
              Name = Common.Constants.ByteTopic
            }
        });
    }

    const int KeySize = 10;
    const int MessageSize = 900;

    byte[] Key = new byte[KeySize];
    byte[] Value = new byte[MessageSize];

    Message<byte[], byte[]> RandomMessage()
    {
      Random.Shared.NextBytes(Key);
      Random.Shared.NextBytes(Value);
      return new Message<byte[], byte[]> { Key = Key, Value = Value };
    }

    public void Produce(int count)
    {
      var maxBatch = 1000;
      var intermediateCount = 0;
      while (count-- > 0) {
        intermediateCount++;
        producer.Produce(
          Common.Constants.ByteTopic,
          RandomMessage());
        if (intermediateCount > maxBatch) {
          intermediateCount = 0;
          producer.Flush();
        }
      }
      //producer.Flush();
    }

    public async Task ProduceAsync(int count)
    {
      while (count-- > 0) {
        await producer.ProduceAsync(
          Common.Constants.ByteTopic,
          RandomMessage());//.GetAwaiter().GetResult();
      }
      //producer.Flush();
    }

    public void Dispose()
    {
      producer.Dispose();
    }

  }
}
