package com.example.chat;

import java.io.Closeable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

    private final String name;

    private final KafkaProducer<String, String> producer;

    public MessageProducer(String name) {
        this.name = name;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Common.KAFKA_HOST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producer = new KafkaProducer<>(properties);
    }

    public void sendMessage(String message) {
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(Common.CHAT_TOPIC, this.name, message);
        LOGGER.debug("Sending message {} to Kafka", message);
        this.producer.send(record, (md, ex) -> {
            if (ex != null) {
                LOGGER.error("Unable to send message");
            } else {
                LOGGER.debug("Correctly sent message");
            }
        });
    }

    @Override
    public void close() {
        this.producer.close();
    }
    
}
