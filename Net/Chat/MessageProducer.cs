using Confluent.Kafka;

namespace Chat
{
  internal class MessageProducer : IDisposable
  {
    private readonly string name;
    private readonly IProducer<string, string> producer;

    public MessageProducer(string name)
    {
      this.name = name;
      var producerConfig = new ProducerConfig
      {
        BootstrapServers = Common.Constants.KafkaHost
      };
      producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    public void SendMessage(string message)
    {
      producer.Produce(
        Common.Constants.ChatTopic, 
        new Message<string, string> { 
          Key = name, 
          Value = message });
      producer.Flush();
    }

    public void Dispose()
    {
      producer.Dispose();
    }

  }
}
