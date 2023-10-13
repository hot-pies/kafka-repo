package com.coolhand.kafka.steam;

import com.coolhand.kafka.steam.exceptionhandler.StreamDeserialiazationExceptionHandler;

import com.coolhand.kafka.steam.topology.OrderTopology;
import com.coolhand.kafka.steam.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.coolhand.kafka.steam.topology.OrderTopology.*;

@Slf4j
public class OrderApp {
    public static void main(String[] args) {

        Properties streamConfig = new Properties();
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.55.11:9092");
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"order-application");
        streamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.AT_LEAST_ONCE);
        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        streamConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamDeserialiazationExceptionHandler.class);
        streamConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimeStampExtractor.class);


//        Create Topic if not exist
//        createTopics(streamConfig,List.of(OrderTopology.GENERAL_ORDER,OrderTopology.RESTAURANT_ORDER,OrderTopology.ORDERS));
//        createTopics(streamConfig,List.of(GENERAL_ORDER_COUNT,RESTAURANT_ORDER_COUNT));
//        createTopics(streamConfig,List.of(GENERAL_ORDER_REVENUE,RESTAURANT_ORDER_REVENUE));
//        createTopics(streamConfig,List.of(STORE));
//        createTopics(streamConfig,List.of(GENERAL_ORDER_COUNT_WINDOWS,RESTAURANT_ORDER_COUNT_WINDOWS));
//        createTopics(streamConfig,List.of(GENERAL_ORDER_REVENUE_WINDOWS,RESTAURANT_ORDER_REVENUE_WINDOWS));


        OrderTopology orderTopology=new OrderTopology();

        KafkaStreams streams = new KafkaStreams(orderTopology.build(),streamConfig);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    public static void createTopics(Properties config, List<String> orders){

        AdminClient admin= AdminClient.create(config);
        var partitions =2;
        short replication =1;

        var newTopics=orders
                .stream()
                .map(topic->{
                    return new NewTopic(topic,partitions,replication);
                })
                .collect(Collectors.toList());

        var createTopicResult=admin.createTopics(newTopics);

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
        }catch (TopicExistsException e){
            log.error("TopicExistsException while creating topics : {} ",e.getMessage(),e);
        }catch (Exception e){
            log.error("Exception creating topics : {} ",e.getMessage(),e);
        }

    }

}
