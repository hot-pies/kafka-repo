package com.coolhand.kafka.steam.greetingspringboot.config;

import com.coolhand.kafka.steam.greetingspringboot.topology.GreetingTopology;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import org.springframework.objenesis.ObjenesisHelper;

@Slf4j
@Configuration
public class GreetingsStreamConfiguration {

    @Autowired
    KafkaProperties kafkaProperties;

    @Bean
    public ObjectMapper objectMapper(){
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false);
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        var kafkaStreamProperties=kafkaProperties.buildAdminProperties();

        kafkaStreamProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                RecoveringDeserializationExceptionHandler.class);
        kafkaStreamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-stream-application");
        kafkaStreamProperties.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, recoverer());
        return new KafkaStreamsConfiguration(kafkaStreamProperties);
    }

    private ConsumerRecordRecoverer recoverer() {

        return ((consumerRecord, e) -> {
            log.error("Exception is {} , Failed Record : {}",consumerRecord,e.getMessage(),e);
        });
    }


    @Bean
    public NewTopic GreetingTopic(){
        return TopicBuilder.name(GreetingTopology.GREETING)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic GreetingOutputTopic(){
        return TopicBuilder.name(GreetingTopology.GREETING_OUTPUT)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
