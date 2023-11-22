package com.example.demo;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MessageProducer implements Closeable {

    private final String name;

    private final KafkaProducer<String, String> producer;

    public MessageProducer(String name) {
        this.name = name;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Common.KAFKA_HOST);
        this.producer = new KafkaProducer<>(properties);
    }

    public void sendMessage(String message) {
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(Common.CHAT_TOPIC, this.name, message);
        this.producer.send(record);
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
    }
    
}
