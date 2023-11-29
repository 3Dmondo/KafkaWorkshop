using Confluent.Kafka;
using System.Runtime.CompilerServices;

namespace Chat
{
  internal class MessageConsumer : IDisposable
  {
    private IConsumer<string, string> consumer;
    public MessageConsumer()
    {
      var consumerConfig = new ConsumerConfig
      {
        BootstrapServers = Common.Constants.KafkaHost,
        GroupId = Guid.NewGuid().ToString(),
        AllowAutoCreateTopics = true
      };
      consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
      consumer.Subscribe(Common.Constants.ChatTopic);
    }

    public async IAsyncEnumerable<ConsumeResult<string, string>>
      ConsumeAsync([EnumeratorCancellation] CancellationToken token)
    {
      while (!token.IsCancellationRequested)
      {
        yield return await Task.Factory.StartNew(
          state => consumer.Consume((CancellationToken)state),
          token,
          token);
      }
    }

    public void Dispose()
    {
      consumer.Dispose();
    }
  }
}
