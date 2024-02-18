package com.coolhand.kafka.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class KafkaRestApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaRestApplication.class, args);
	}

}
