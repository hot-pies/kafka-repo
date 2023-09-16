package com.coolhand.kafka.steam;

import com.fasterxml.jackson.databind.JsonNode;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Instant;
import java.util.Properties;

public class BalanceUpdatesApp {
    public static void main(String[] args) {

        String BOOTSTRAP_SERVER="192.168.55.11:9092";
        String INPUT_TOPIC_NAME="bank-balance-updates-v2";
        String OUTPUT_TOPIC_NAME="bank-balance-updates-stream-output-v1";

        Properties streamConfig = new Properties();
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"BalanceUpdatesStream-application");
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        streamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE);
        streamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        final Serializer<JsonNode> jsonNodeSerializer=new JsonSerializer();
        final Deserializer<JsonNode> jsonNodeDeserializer=new JsonDeserializer();

        final Serde<JsonNode> jsonNodeSerde=Serdes.serdeFrom(jsonNodeSerializer,jsonNodeDeserializer);

        KStreamBuilder streamBuilder=new KStreamBuilder();
        KStream<String,JsonNode> stream=streamBuilder.stream(Serdes.String(),jsonNodeSerde,INPUT_TOPIC_NAME);

        ObjectNode initialBalance=JsonNodeFactory.instance.objectNode();
        initialBalance.put("Count",0);
        initialBalance.put("Balance",0);
        initialBalance.put("Date", Instant.ofEpochMilli(0L).toString());

        KTable<String,JsonNode> account=stream
                .groupByKey(Serdes.String(),jsonNodeSerde)
                .aggregate(
                        ()->initialBalance,
                        (key,transaction,balance)-> newBalance(transaction,balance),
                        jsonNodeSerde,
                "bank-balance-agg"
        );


//        KTable<String, Long> account= stream
//                .mapValues((value)->value.toLowerCase())
//                .selectKey((name,amount)->amount)
//                .groupByKey()
//                .count();


        account.toStream().foreach((key,value)->{
            System.out.println("Key: "+key+ ", Value: "+value);
        });
        account.to(Serdes.String(),jsonNodeSerde,OUTPUT_TOPIC_NAME);

        KafkaStreams streams=new KafkaStreams(streamBuilder,streamConfig);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {

        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();

        newBalance.put("Count",balance.get("Count").asInt()+1);
        newBalance.put("Balance",balance.get("Balance").asInt()+transaction.get("amount").asInt());

        Long balanceEpoc=Instant.parse(balance.get("Date").asText()).toEpochMilli();
        Long transactionEpoc=Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant=Instant.ofEpochMilli(Math.max(balanceEpoc,transactionEpoc));
        newBalance.put("Date",newBalanceInstant.toString());

        return  newBalance;
    }
}
