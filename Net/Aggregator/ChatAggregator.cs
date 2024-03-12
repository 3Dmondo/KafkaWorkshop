using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using static Streamiz.Kafka.Net.KafkaStream;

namespace Aggregator
{
  internal class ChatAggregator : IDisposable
  {
    private const string GlobalStoreName = "globalCountStore-net";
    private const string CharStoreName = "charCountStore-net";
    private const string WordStoreName = "wordCountStore-net";
    private const string MessageStoreName = "messageCountStore-net";
    private KafkaStream stream;

    public ChatAggregator()
    {
      var config = new StreamConfig<StringSerDes, StringSerDes> {
        ApplicationId = "aggregator-net-2",
        BootstrapServers = Common.Constants.KafkaHost,
        AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
        AllowAutoCreateTopics = true,
        Guarantee = ProcessingGuarantee.EXACTLY_ONCE,
        //Logger = LoggerFactory.Create(b => b.ClearProviders()),
      };

      var streamBuilder = new StreamBuilder();
      streamBuilder.Stream<string, string>(Common.Constants.ChatTopic)
        .AddGlobalCharCounter(GlobalStoreName)
        .AddCharCounter(CharStoreName)
        .AddWordCounter(WordStoreName)
        .AddMessageCounter(MessageStoreName);

      stream = new KafkaStream(streamBuilder.Build(), config);
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

    public IEnumerable<KeyValuePair<string, int>> StoreValues()
    {
      try {
        var store = stream.Store(
          StoreQueryParameters.FromNameAndType(
            CharStoreName,
            QueryableStoreTypes.KeyValueStore<string, int>()));
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
