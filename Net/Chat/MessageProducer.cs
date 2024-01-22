using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Chat
{
  internal class MessageProducer : IDisposable
  {
    private readonly string name;
    private readonly IProducer<string, string> producer;

    public MessageProducer(string name)
    {
      this.name = name;
      var producerConfig = new ProducerConfig {
        BootstrapServers = Common.Constants.KafkaHost,
        AllowAutoCreateTopics = true
      };
      producer = new ProducerBuilder<string, string>(producerConfig).Build();
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
      if (topicNames.Any(n => n == Common.Constants.ChatTopic)) 
        adminClient.CreateTopicsAsync(new TopicSpecification[] {
            new TopicSpecification {
              Name = Common.Constants.ChatTopic 
            } 
        });
    }

    public void SendMessage(string message)
    {
      producer.Produce(
        Common.Constants.ChatTopic,
        new Message<string, string> {
          Key = name,
          Value = message
        });
      producer.Flush();
    }

    public void Dispose()
    {
      producer.Dispose();
    }

  }
}
