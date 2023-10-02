package com.coolhand.kafka.steam.greetingspringboot.config;

import com.coolhand.kafka.steam.greetingspringboot.topology.GreetingTopology;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;


@Configuration
public class GreetingsStreamConfiguration {

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
