package com.coolhand.kafka.steam.launcher;

import com.coolhand.kafka.steam.topology.WindowTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.coolhand.kafka.steam.topology.WindowTopology.WINDOW_WORDS;

@Slf4j
public class WindowsStreamApp {
    public static void main(String[] args) {

        Properties streamConfig=new Properties();
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.55.11:9092");
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"windowed-application");
        streamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.AT_LEAST_ONCE);
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        streamConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"0");

//        createTopics(streamConfig,List.of(WINDOW_WORDS));

        KafkaStreams streams = new KafkaStreams(WindowTopology.build(),streamConfig);
        streams.cleanUp();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    private static void createTopics(Properties config, List<String> aggregate) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication  = 1;

        var newTopics = aggregate
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }
}
