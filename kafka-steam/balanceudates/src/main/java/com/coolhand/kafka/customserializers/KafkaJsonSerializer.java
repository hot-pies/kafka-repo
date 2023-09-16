package com.coolhand.kafka.customserializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaJsonSerializer<T> implements Serializer<T> {

        private final ObjectMapper objectMapper=new ObjectMapper();
    @Override
    public void configure(Map<String,?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topics, Object data) {
        try{
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing object to JSON: " + data, e);
        }
    }

    @Override
    public void close() {

    }
}
