package com.coolhand.kafka.steam.launcher;

import com.coolhand.kafka.steam.topology.KTableTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.coolhand.kafka.steam.topology.KTableTopology.WORDS;

@Slf4j
public class KTableStreamApp {
    public static void main(String[] args) {
        Properties streamConfig=new Properties();
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.55.11:9092");
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"greeting-application");
        streamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.AT_LEAST_ONCE);
        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

//        createTopic(streamConfig, List.of(WORDS));
        KTableTopology topology = new KTableTopology();


        KafkaStreams streams = new KafkaStreams(topology.build(),streamConfig);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        log.info("KTable Streams : ");

        streams.cleanUp();
        streams.start();

    }


    private static void createTopic(Properties config, List<String> greetings){

        AdminClient admin = AdminClient.create(config);
        var partitions=1;
        short replication=1;

        var newTopic = greetings
                .stream()
                .map(topic ->{
                    return new NewTopic(topic,partitions,replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopic);

        try{
            createTopicResult
                    .all().get();
            log.info("Topics are created successfully");
        } catch (ExecutionException e) {
            log.error("ExecutionException creating topics : {} ",e.getMessage(),e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            log.error("InterruptedException creating topics : {} ",e.getMessage(),e);
            throw new RuntimeException(e);
        }catch (Exception e){
            log.error("Exception creating topics : {} ",e.getMessage(),e);
        }
    }
}
