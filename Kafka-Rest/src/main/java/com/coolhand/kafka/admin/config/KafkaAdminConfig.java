package com.coolhand.kafka.admin.config;


import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class KafkaAdminConfig{
	
	@Autowired
    private KafkaAdmin kafkaAdmin;

    @Bean
    public AdminClient kafkaAdminClient() {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }



}
