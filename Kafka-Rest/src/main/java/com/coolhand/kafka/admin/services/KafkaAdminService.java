package com.coolhand.kafka.admin.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;



@Slf4j
@Service
public class KafkaAdminService {

	@Autowired
	private AdminClient adminClient;

	public Set<String> listKafkaTopics() {
		try {

			ListTopicsResult topicsResult = adminClient.listTopics();
			KafkaFuture<Set<String>> topicsFuture = topicsResult.names();
			Set<String> topics = topicsFuture.get();

			log.info("Kafka Topics list : " + topics);
//			OperationMetrics();
			return topics;
            
        } catch (Exception e) {
            e.printStackTrace();
        }
		return null;
	}

	public void OperationMetrics(){
		Map<MetricName, ? extends Metric> metrics = adminClient.metrics();

		log.info("Kafka Admin Client Metrics:");
		for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
			MetricName metricName = entry.getKey();
			Metric metric = entry.getValue();
			System.out.println(metricName.name() + " - " + metric.metricValue());
		}
	}

	public TopicDescription getTopicDescription(String topicName) {
		try {
			log.info("getTopicDescription");
			DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
//			List<TopicPartitionInfo> partitionInfos =describeTopicsResult.all().get().get(topicName).partitions();
			describeTopicsResult.all().get();
			TopicDescription topicDescription = describeTopicsResult.values().get(topicName).get();

			log.info("topicDescription name "+topicDescription.name());
			log.info("topicDescription topicId "+topicDescription.topicId());
			log.info("topicDescription partitions "+topicDescription.partitions().get(0));
			log.info("topicDescription isInternal "+topicDescription.isInternal());

			topicDescription.toString();
			return topicDescription;
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
		}
	}
}
