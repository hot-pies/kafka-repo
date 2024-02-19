package com.coolhand.kafka.admin.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
public class KafkaHealthIndicator implements HealthIndicator   {

    private final String bootstrapServers = "192.168.144.100:9093";

    @Override
    public Health health() {
    	System.out.println("Coolhand Health start");
        try (AdminClient adminClient = AdminClient.create(getAdminClientConfig())) {
            DescribeClusterResult describeClusterResult = adminClient.describeCluster();
            KafkaFuture<String> clusterIdFuture = describeClusterResult.clusterId();
            System.out.println("Coolhand Health health");
            // Check Kafka cluster health
            String clusterId = clusterIdFuture.get();
            if (clusterId != null && !clusterId.isEmpty()) {
            	
            	Health build = Health.up().withDetail("clusterId", clusterId).build();
            	System.out.println("++++++++++++++++ "+build);
                return build;
            } else {
                return Health.down().withDetail("message", "Kafka cluster is not healthy").build();
            }
        } catch (Exception e) {
            return Health.down(e).build(); // Kafka health check failed
        }
    }

    private Properties getAdminClientConfig() {
    	System.out.println("Coolhand getAdminClientConfig start");
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return properties;
    }
}
