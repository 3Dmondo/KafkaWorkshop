package com.example.aggregator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Program {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Program.class);

    static void getCount(KafkaStreams kafkaStreams) {
      StoreQueryParameters<ReadOnlyKeyValueStore<Integer, Integer>> params = StoreQueryParameters
        .fromNameAndType("countstore", QueryableStoreTypes.keyValueStore());
      ReadOnlyKeyValueStore<Integer, Integer> localStore = kafkaStreams.store(params);
      KafkaStreams.State state = kafkaStreams.state();
      if ((localStore != null) && (KafkaStreams.State.RUNNING.equals(state))) {
        Integer count = localStore.get(1);
        LOGGER.info("Count is {}", count);
      } else {
        LOGGER.info("Count is not ready");
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      getCount(kafkaStreams);
    }

	public static void main(String[] args) throws InterruptedException, ExecutionException {
        LOGGER.info("press CTRL-C to close.");
        var charCounter = new CharCounter();
        var future = charCounter.startAsync();

        KafkaStreams kafkaStreams = charCounter.getKafkaStreams();
        getCount(kafkaStreams);

        LOGGER.info("Started aggregator");
        future.get();
        /*
        KeyValueIterator<Integer, Integer> iter = localStore.all();
        localStore.range(0, 1);

         */
        LOGGER.info("Terminated");
    }


}
