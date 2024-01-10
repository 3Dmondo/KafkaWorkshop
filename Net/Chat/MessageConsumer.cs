using Confluent.Kafka;
using System.Runtime.CompilerServices;

namespace Chat
{
  internal class MessageConsumer : IDisposable
  {
    private IConsumer<string, string> consumer;
    public MessageConsumer()
    {
      var consumerConfig = new ConsumerConfig {
        BootstrapServers = Common.Constants.KafkaHost,
        GroupId = Guid.NewGuid().ToString(),
        AllowAutoCreateTopics = true
      };
      consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
      consumer.Subscribe(Common.Constants.ChatTopic);
    }

    public IEnumerable<ConsumeResult<string, string>>
      ConsumeAsync([EnumeratorCancellation] CancellationToken token)
    {
      while (!token.IsCancellationRequested) {
        var message = Consume(token);
        if (message != null) {
          yield return message;
        }
      }
    }

    private ConsumeResult<string, string> Consume(CancellationToken token)
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
