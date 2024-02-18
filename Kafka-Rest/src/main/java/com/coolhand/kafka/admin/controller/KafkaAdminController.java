package com.coolhand.kafka.admin.controller;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.coolhand.kafka.admin.services.KafkaAdminService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class KafkaAdminController {

	
	@Autowired
	private KafkaAdminService kafkaAdminService;
    
	@GetMapping("/kafka/topics")
	public Set<String> listKafkaTopics() {
		
		Set<String> listKafkaTopics = kafkaAdminService.listKafkaTopics();
		
		return listKafkaTopics;
	}
}
