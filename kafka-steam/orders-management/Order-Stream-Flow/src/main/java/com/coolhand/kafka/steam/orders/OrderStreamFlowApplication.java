package com.coolhand.kafka.steam.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class OrderStreamFlowApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderStreamFlowApplication.class, args);
	}

}
