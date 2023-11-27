package com.example.demo;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    private final KafkaConsumer<String, String> consumer;

    public MessageConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Common.KAFKA_HOST);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(Common.CHAT_TOPIC));
    }

    public Stream<ConsumerRecord<String, String>> consume() {
        Iterator<ConsumerRecord<String, String>> iter = this.consumer
            .poll(Duration.ofSeconds(1))
            .iterator();
        return StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED),
          false);
    }

    @Override
    public void close() {
        this.consumer.close();
    }
    
}
