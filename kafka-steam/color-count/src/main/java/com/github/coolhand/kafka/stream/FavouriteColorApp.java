package com.github.coolhand.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColorApp {
    public static void main(String[] args) {

        Properties config = new Properties();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.55.11:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"favourite-color-stream");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String,String> colorsource = builder.stream("favourite-color-stream-input");

        KStream<String,String> userandcolors = colorsource
                .filter((key,value) -> value.contains(","))
                .selectKey((user,colours)->colours.split(",")[0].toLowerCase())
                .mapValues((value)->{
                    if(value!=null){
                        String[] elements=value.split(",");
                        if(elements.length >1){
                            return elements[1].toLowerCase();
                        }
                    }
                    System.out.println("Incomplete input");
                    return null;
                })
//              .mapValues((value)->value.split(",")[1].toLowerCase())
                .filter((user,color)->Arrays.asList("red","green","blue").contains(color));
                                        ;

        userandcolors.to("user-key-and-color");

       KTable<String,String> kTable=builder.table("user-key-and-color");

        KTable<String, Long> groupedKTable = kTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count("CountByColor");

        groupedKTable.toStream().to(Serdes.String(),Serdes.Long(),"favourite-color-stream-output");

        KafkaStreams stream=new KafkaStreams(builder,config);
        stream.start();

        System.out.println(stream.toString());

    Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }

}
