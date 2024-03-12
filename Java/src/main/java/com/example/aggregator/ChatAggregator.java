package com.example.aggregator;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.chat.Common;

public class ChatAggregator implements Closeable {

    public static final String GLOABAL_COUNT_STORE = "globalCountStore";

    public static final String CHAR_COUNT_STORE = "charCountStore";

    public static final String MESSAGE_COUNT_STORE = "messageCountStore";

    public static final String WORD_COUNT_STORE = "wordCountStore";

    private static final Logger LOGGER = LoggerFactory.getLogger(ChatAggregator.class);

    private final KafkaStreams kafkaStreams;

    public KafkaStreams getKafkaStreams() {
        return kafkaStreams;
    }

    public ChatAggregator() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KTable<Integer, Integer> globalMessageCount = addGlobalCharCounter(streamsBuilder);

        KTable<String, Integer> charCount = addCharCounter(streamsBuilder);

        //KTable<String, Integer> messageCount = addMessageCounter(streamsBuilder);
        //messageCount.toStream().foreach((k, v) -> LOGGER.info("Message count for {}: {}", k, v));

        //KTable<String, Integer> wordCount = addWordCounter(streamsBuilder);
        //wordCount.toStream().foreach((k, v) -> LOGGER.info("Word count for {}: {}", k, v));

        Topology topology = streamsBuilder.build();
        TopologyDescription topologyDescription = topology.describe();
        LOGGER.info("Initialized Kafka Streams application with topology \n{}", topologyDescription);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregator-java");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Common.KAFKA_HOST);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.kafkaStreams = new KafkaStreams(topology, props);
    }

    public void start() {
        kafkaStreams.start();
    }

    private KTable<Integer, Integer> addGlobalCharCounter(StreamsBuilder streamsBuilder) {
        return streamsBuilder
            .stream(Common.CHAT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .map((k, v) -> new KeyValue<>(1, v))
            .groupByKey(Grouped.with(Serdes.Integer(), Serdes.String()))
            .aggregate(
                () -> 0, 
                (k, msg, agg) -> agg + msg.length(),
                Materialized
                    .<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(GLOABAL_COUNT_STORE)
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(Serdes.Integer()));
    }

    private KTable<String, Integer> addCharCounter(StreamsBuilder streamsBuilder) {
        return streamsBuilder
            .stream(Common.CHAT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                () -> 0, 
                (k, msg, agg) -> agg + msg.length(),
                Materialized
                    .<String, Integer, KeyValueStore<Bytes, byte[]>>as(CHAR_COUNT_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer()));
    }

    private KTable<String, Integer> addMessageCounter(StreamsBuilder streamsBuilder) {
        return streamsBuilder
            .stream(Common.CHAT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                () -> 0, 
                (k, msg, agg) -> agg + 1,
                Materialized
                    .<String, Integer, KeyValueStore<Bytes, byte[]>>as(MESSAGE_COUNT_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer()));
    }

    private KTable<String, Integer> addWordCounter(StreamsBuilder streamsBuilder) {
        return streamsBuilder
            .stream(Common.CHAT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMapValues(v -> Arrays.asList(v.split("\\s+")))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                () -> 0, 
                (k, msg, agg) -> agg + msg.length(),
                Materialized
                    .<String, Integer, KeyValueStore<Bytes, byte[]>>as(WORD_COUNT_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer()));
    }

    private void addAverageWordCharCounter(StreamsBuilder streamsBuilder) {

    }

    @Override
    public void close() {
        kafkaStreams.close();
    }

}
