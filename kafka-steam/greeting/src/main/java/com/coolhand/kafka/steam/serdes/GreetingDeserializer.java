package com.coolhand.kafka.steam.serdes;

import com.coolhand.kafka.steam.domain.Greeting;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;
@Slf4j
public class GreetingDeserializer implements Deserializer<Greeting> {

    ObjectMapper objectMapper;

    public GreetingDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Greeting deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data,Greeting.class);
        } catch (IOException e) {
            log.error("IOException : {} ",e.getMessage(),e);
            throw new RuntimeException(e);
        }catch (Exception e){
            log.error("Exception : {} ",e.getMessage(),e);
            throw new RuntimeException(e);
        }
    }


}
