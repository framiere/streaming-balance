package com.github.framiere.balance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonSerde<T> implements Serde<T> {
    private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Class clazz;

    public JsonSerde(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public byte[] serialize(String topic, T data) {
                try {
                    return OBJECT_MAPPER.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public T deserialize(String topic, byte[] data) {
                try {
                    return (T) OBJECT_MAPPER.readValue(data, clazz);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() {

            }
        };
    }
}
