package com.example.aggregator;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.chat.Common;

public class CharCounter {

    private static final Logger LOGGER = LoggerFactory.getLogger(CharCounter.class);
    
    private final KafkaStreams kafkaStreams;

    public CharCounter() {
        Topology topology = buildTopology();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregator_luca");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Common.KAFKA_HOST);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        this.kafkaStreams = new KafkaStreams(topology, props);
    }

    public CompletableFuture<Void> startAsync() {
        kafkaStreams.start();
        return new CompletableFuture<>();
    }
    
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(Common.CHAT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .map((k, v) -> new KeyValue<>(1, v))
            .groupByKey(Grouped.with(Serdes.Integer(), Serdes.String()))
            .aggregate(
                () -> 0, 
                (k, msg, agg) -> agg + msg.length(),
                Materialized.with(Serdes.Integer(), Serdes.Integer())
            )
            .toStream()
            .foreach((k, v) -> LOGGER.info("Count for {} is {}", k, v));
        return builder.build();
    }

}
