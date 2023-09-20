package com.coolhand.kafka.steam;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.

@Slf4j
public class GreetingApp {

    public Topology createTopology(){
        StreamsBuilder builder=new StreamsBuilder();

        var greeting_stream= builder.stream("greetings-input", Consumed.with(Serdes.String(),Serdes.String()));
        greeting_stream.print(Printed.toSysOut());

        var modified_greeting= greeting_stream.mapValues((key,value)->value.toUpperCase());
        modified_greeting.print(Printed.toSysOut());

        modified_greeting.to("greetings-output", Produced.with(Serdes.String(),Serdes.String()));

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