package com.coolhand.kafka.steam.greetingspringboot.producer;

import com.coolhand.kafka.steam.greetingspringboot.domain.Greeting;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;

import static com.coolhand.kafka.steam.greetingspringboot.topology.GreetingTopology.GREETING;

@Slf4j
public class GreetingMockData {

    public static void main(String[] args) {

        ObjectMapper objectMapper=new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false);

        englishGreeting(objectMapper);
//        spanishGreeting(objectMapper);
    }

    private static void englishGreeting(ObjectMapper objectMapper){
        var englishGreetings = List.of(
         new Greeting("Hello , Good Morning !", LocalDateTime.now()),
        new Greeting("Hello , Good Evening !", LocalDateTime.now()),
        new Greeting("Hello , Good Night !", LocalDateTime.now())
        );
        englishGreetings
                .forEach(greeting -> {
                    try {
                        var greetingJSON=objectMapper.writeValueAsString(greeting);

                        var recordMetaData=ProducerUtil.publishMessageSync(GREETING,null,greetingJSON);
                        log.info("Published the alphabet message : {} ",recordMetaData);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void spanishGreeting(ObjectMapper objectMapper){
        var spanishGreetings = List.of(
                new Greeting("¡Hola Buenos Dias!", LocalDateTime.now()),
                new Greeting("¡Hola Buenas Tardes!", LocalDateTime.now()),
                new Greeting("¡Hola, Buenas Noches!", LocalDateTime.now())
        );
        spanishGreetings
                .forEach(greeting -> {
                    try {
                        var greetingJSON=objectMapper.writeValueAsString(greeting);
                        var recordMetaData=ProducerUtil.publishMessageSync(GREETING,null,greetingJSON);
                        log.info("Published the alphabet message : {} ",recordMetaData);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
