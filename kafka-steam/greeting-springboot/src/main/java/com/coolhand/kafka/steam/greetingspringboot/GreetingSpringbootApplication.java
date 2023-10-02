package com.coolhand.kafka.steam.greetingspringboot;

import com.coolhand.kafka.steam.greetingspringboot.topology.GreetingTopology;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class GreetingSpringbootApplication {

	public static void main(String[] args) {
		SpringApplication.run(GreetingSpringbootApplication.class, args);

	}

}
