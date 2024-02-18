package com.coolhand.kafka.admin.services;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.NoSuchSslBundleException;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.stereotype.Service;

@Service
public class KafkaAdminService {

	@Autowired
	private KafkaProperties kafkaProperties;
	
	public Set<String> listKafkaTopics() {
		try {
		
			Properties props = new Properties();
			props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

			AdminClient adminClient = AdminClient.create(props);
			ListTopicsResult topicsResult = adminClient.listTopics();
			KafkaFuture<Set<String>> topicsFuture = topicsResult.names();

			Set<String> topics = topicsFuture.get();
			System.out.println("Topics: " + topics);

			return topics;
            
        } catch (Exception e) {
            e.printStackTrace();
        }
		return null;
	}
}
