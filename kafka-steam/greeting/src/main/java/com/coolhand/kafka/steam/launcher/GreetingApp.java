package com.coolhand.kafka.steam.launcher;

import com.coolhand.kafka.steam.topology.GreetingTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.coolhand.kafka.steam.topology.GreetingTopology.*;

@Slf4j
public class GreetingApp {

    public static void main(String[] args) {

        Properties streamConfig=new Properties();
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.55.11:9092");
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"greeting-application");
        streamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.AT_LEAST_ONCE);
        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

//        createTopic(streamConfig,List.of(GREETINGS,GREETINGS_UPPERCASE,GREETINGS_SPANISH));

        GreetingTopology greetingApp=new GreetingTopology();
        log.info(" ************ Begin ************ ");
        KafkaStreams kafkaStreams= new KafkaStreams(greetingApp.createTopology(),streamConfig);

        kafkaStreams.cleanUp();
        kafkaStreams.start();

        log.info(" ************ Done ************ ");

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        log.info(" ************ CLOSE ************ ");
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