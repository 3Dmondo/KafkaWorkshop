package com.example.demo;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MessageConsumer implements Closeable {

    private final KafkaConsumer<String, String> consumer;

    public MessageConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Common.KAFKA_HOST);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(Common.CHAT_TOPIC));
        this.consumer.close();
    }

    public void consume() {
    }

    @Override
    public void close() throws IOException {
        this.consumer.close();
    }
    
}
