package com.example.utils;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerde {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static class JsonSerializer<T> implements Serializer<T> {

        private static final Logger LOG = LoggerFactory.getLogger(JsonSerializer.class);

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }
            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                LOG.warn("Unable to serialize", e);
                return new byte[0];
            }
        }

    };

    public static class JsonDeserializer<T> implements Deserializer<T> {
        
        private static final Logger LOG = LoggerFactory.getLogger(JsonDeserializer.class);

        public static final String KEY_JSON_CLASS_CONFIG = "key.json.class";

        public static final String VALUE_JSON_CLASS_CONFIG = "value.json.class";

        private Class<T> tClass;

        public JsonDeserializer() {}

        public JsonDeserializer(Class<T> tClass) {
            this.tClass = tClass;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void configure(Map<String, ?> props, boolean isKey) {
            Class<T> fetched;
            if (isKey) {
                fetched = (Class<T>) props.get(KEY_JSON_CLASS_CONFIG);
            } else {
                fetched = (Class<T>) props.get(VALUE_JSON_CLASS_CONFIG);
            }
            if (fetched != null) {
                tClass = fetched;
            }
        }

        @Override
        public T deserialize(String topic, byte[] bytes) {
            if (bytes == null)
                return null;
            try {
                return OBJECT_MAPPER.readValue(bytes, tClass);
            } catch (Exception e) {
                LOG.warn("Unable to deserialize", e);
                return null;
            }
        }
    }

    public static <T> Serde<T> Serde(Class<T> tClass) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(tClass));
    }

    public static <T> Serde<T> Serde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>());
    }
    
}
