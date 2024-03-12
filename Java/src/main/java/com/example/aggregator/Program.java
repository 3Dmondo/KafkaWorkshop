package com.example.aggregator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Program {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Program.class);

    private static void runLookupOnLocalStore(KafkaStreams kafkaStreams) {
        if ((kafkaStreams != null) && 
            (kafkaStreams.state() == KafkaStreams.State.RUNNING)) {
                ReadOnlyKeyValueStore<String, Integer> localStore = kafkaStreams
                    .store(StoreQueryParameters
                        .fromNameAndType(ChatAggregator.CHAR_COUNT_STORE, QueryableStoreTypes.keyValueStore()));
                try (KeyValueIterator<String, Integer> iterator = localStore.all()) {
                    LOGGER.info("------------------");
                    LOGGER.info("Char count summary");
                    LOGGER.info("------------------");
                    while (iterator.hasNext()) {
                        KeyValue<String, Integer> record = iterator.next();
                        LOGGER.info("{}: {}", record.key, record.value);
                    }
                }
        }
    }

	public static void main(String[] args) throws InterruptedException, ExecutionException {
        LOGGER.info("press CTRL-C to close.");
        ChatAggregator charCounter = new ChatAggregator();

		ExecutorService executorService = Executors.newFixedThreadPool(2);

		CountDownLatch counterLatch = new CountDownLatch(1);

		Future<?> counterFuture = executorService.submit(() -> {
            charCounter.start();
        });

        Future<?> lookupFuture = executorService.submit(() -> {
            while (counterLatch.getCount() > 0) {
                KafkaStreams kafkaStreams = charCounter.getKafkaStreams();
                runLookupOnLocalStore(kafkaStreams);
                try {
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    LOGGER.error("Found error", e);
                }
            }
        });

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
                LOGGER.info("Shutting down");
                charCounter.close();
                counterLatch.countDown();
                counterFuture.get();
                lookupFuture.get();
			} catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Found error", e);
			}
		}));
    }

}
