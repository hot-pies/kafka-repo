package com.coolhand.kafka.steam.enrich.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class UserEventEnrichStream {
    public static void main(String[] args) {
        String BOOTSTRAP_SERVER="192.168.55.11:9092";
        String USER_TABLE="user-table";
        String USER_PURCHASE="user-purchase";

        Properties streamConfig = new Properties();
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        streamConfig.put(StreamsConfig.AT_LEAST_ONCE,"true");
        streamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.AT_LEAST_ONCE);
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"UserEventEnrichJoin-Application");
        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        streamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KStreamBuilder builder=new KStreamBuilder();

        GlobalKTable<String,String> globalKTable=builder.globalTable(USER_TABLE);
        KStream<String,String> userPurchasestream=builder.stream(USER_PURCHASE);

        KStream<String,String> userPurchaseEnrich=userPurchasestream.join(globalKTable,
                (key,value) -> key,
                (userPurchase,userInfo)->"Purchase :"+userPurchase+"UserInfo : ["+userInfo+"]");

        userPurchaseEnrich.to("user-purchase-enriched-inner-join");


        KStream<String,String> userPurchaseLeftJoin=userPurchasestream.leftJoin(globalKTable,
                (key,value)->key,
                (userPurchase,userInfo)->{
                    if(userInfo!=null){
                        return "Purchase :"+userPurchase+"UserInfo : ["+userInfo+"]";
                    }else{
                        return "Purchase :"+userPurchase+"UserInfo : null";
                    }
                }
        );

        userPurchaseLeftJoin.to("user-purchase-enriched-left-join");

        KafkaStreams streams=new KafkaStreams(builder,streamConfig);
        streams.cleanUp();
        streams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));



    }
}
