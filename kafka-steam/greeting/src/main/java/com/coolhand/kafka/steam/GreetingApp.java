package com.coolhand.kafka.steam;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.

@Slf4j
public class GreetingApp {

    public Topology createTopology(){
        StreamsBuilder builder=new StreamsBuilder();

        KStream<String,String> greeting_stream= builder.stream("greetings-input"
//                , Consumed.with(Serdes.String(),Serdes.String())
        );
        greeting_stream.print(Printed.<String,String>toSysOut().withLabel("greeting_stream"));
        greeting_stream.peek((key, value) -> {
           log.info("Before modification key : {}, value : {}",key,value);
        });

        KStream<String,String> greeting_spanish_stream= builder.stream("greetings-spanish-input"
//                , Consumed.with(Serdes.String(),Serdes.String())
        );
        greeting_stream.print(Printed.<String,String>toSysOut().withLabel("greeting_stream"));
        greeting_stream.peek((key, value) -> {
            log.info("Before modification key : {}, value : {}",key,value);
        });

        var merge_greeting=greeting_stream.merge(greeting_spanish_stream);
        merge_greeting.peek((key,value)-> log.info("After topic Merge key :{} , value : {} ",key,value));


//        var modified_greeting = greeting_stream
        var modified_greeting = merge_greeting
//                .mapValues((key,value)->value.toUpperCase())
                .map((key,value)->KeyValue.pair(key.toUpperCase(),value.toUpperCase()))
//                .flatMap((key, value) -> {
//                    var newValues = Arrays.asList(value.split(""));
////                    newValues.forEach(element-> log.info("Split modified greeting : "+element));
//                    return newValues
//                            .stream()
//                            .map(val-> KeyValue.pair(key.toUpperCase(),val.toUpperCase()))
//                            .collect(Collectors.toList());
//                })
                .peek((key, value) -> {
                    log.info("After modifications : key : {}, value : {}",key,value);
                })
                ;

        modified_greeting
                .print(Printed.<String,String>toSysOut().withLabel("modified_greeting:"));

        modified_greeting.to("greetings-output"
//                , Produced.with(Serdes.String(),Serdes.String())
        );

//        KStream<String,String> streamIn = builder.stream("greetings-input");

//        KStream<String,String> streamOut = streamIn
//                .mapValues((key,value)->value.toLowerCase());
//
//        streamOut.to("greetings-output");

        return  builder.build();
    }

    public static void main(String[] args) {

        Properties streamConfig=new Properties();
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.55.11:9092");
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"greeting-application");
        streamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.AT_LEAST_ONCE);
        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // Set the custom properties
        streamConfig.put("key.separator", "-");
        streamConfig.put("parse.key", "true");


        GreetingApp greetingApp=new GreetingApp();
        log.info(" ************ Begin ************ ");
        KafkaStreams kafkaStreams= new KafkaStreams(greetingApp.createTopology(),streamConfig);

        kafkaStreams.cleanUp();
        kafkaStreams.start();

        log.info(" ************ Done ************ ");

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        log.info(" ************ CLOSE ************ ");
    }
}