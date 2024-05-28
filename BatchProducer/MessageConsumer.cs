using Confluent.Kafka;
using System.Runtime.CompilerServices;

namespace Chat
{
  internal class MessageConsumer : IDisposable
  {
    private IConsumer<byte[], byte[]> consumer;
    private int counter = 0;

    public bool Ready { get; private set; } = false;
    public MessageConsumer()
    {
      var consumerConfig = new ConsumerConfig {
        BootstrapServers = Common.Constants.KafkaHost,
        GroupId = "byte-consumer",
        AllowAutoCreateTopics = true,
        AutoOffsetReset = AutoOffsetReset.Earliest

      };
      consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
        .SetPartitionsAssignedHandler((_, _) => Ready = true)
        .Build();
      consumer.Subscribe(Common.Constants.ByteTopic);
    }

    public void ConsumeAll([EnumeratorCancellation] CancellationToken token)
    {
      while (!token.IsCancellationRequested) {
        var message = Consume(token);
        if (message != null) {
          Console.WriteLine(++counter);
        }
      }
    }

    private ConsumeResult<byte[], byte[]> Consume(CancellationToken token)
    {
      try {
        return consumer.Consume(token);
      } catch (Exception ex) {
        return null;
      }
    }

    public void Dispose()
    {
      consumer.Dispose();
    }
  }
}
