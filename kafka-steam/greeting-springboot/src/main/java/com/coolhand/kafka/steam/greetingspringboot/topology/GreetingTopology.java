package com.coolhand.kafka.steam.greetingspringboot.topology;

import com.coolhand.kafka.steam.greetingspringboot.domain.Greeting;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GreetingTopology {

    private final ObjectMapper objectMapper;
    public GreetingTopology(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public static String  GREETING="greetings";
    public static String  GREETING_OUTPUT="greetings-output";

    @Autowired
    public void process(StreamsBuilder streamsBuilder){

        var inputStream=streamsBuilder
                .stream(GREETING,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Greeting.class,objectMapper))
                );

        inputStream
                .print(Printed.<String,Greeting>toSysOut().withLabel("input-stream"));
//                .print(Printed.<String,String>toSysOut().withLabel("input-stream"));

        var outputStream=inputStream
                .mapValues((key,value)->
//                        value.toUpperCase());
                        new Greeting(value.message().toUpperCase(),value.timeStamp()));


        outputStream
                .print(Printed.<String ,Greeting>toSysOut().withLabel("output-stream"));

        outputStream
                .to(GREETING_OUTPUT,
                        Produced.with(Serdes.String(),new JsonSerde<>(Greeting.class,objectMapper)));

    }
}
