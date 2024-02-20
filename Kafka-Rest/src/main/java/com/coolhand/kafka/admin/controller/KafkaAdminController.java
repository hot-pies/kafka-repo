package com.coolhand.kafka.admin.controller;

import com.coolhand.kafka.admin.domain.PartitionInfo;
import com.coolhand.kafka.admin.domain.TopicInfo;
import com.coolhand.kafka.admin.services.KafkaAdminService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

	@GetMapping("/kafka/topics/{topicName}")
	public TopicInfo DescribeTopic(@PathVariable String topicName) {
		log.info("getTopicDescription for topic : "+topicName);

		final TopicDescription topicDescription = kafkaAdminService.getTopicDescription(topicName);

		return new TopicInfo(topicDescription);

//		return topicPartitionInfoList;
	}
//	@GetMapping("/kafka/topics/{topicName}")
//	public List<PartitionInfo> DescribeTopic(@PathVariable String topicName) {
//		log.info("getTopicDescription for topic : "+topicName);
//
//		final TopicDescription topicDescription = kafkaAdminService.getTopicDescription(topicName);
//		ArrayList<PartitionInfo> topicPartitionInfoList = new ArrayList<PartitionInfo>();
//
//		for(TopicPartitionInfo topicPartitionInfo :topicDescription.partitions()){
//			final PartitionInfo partitionInfo = new PartitionInfo(topicPartitionInfo);
//			log.info("Coolhand partitionInfo : " +partitionInfo.toString());
//			topicPartitionInfoList.add(partitionInfo);
//		}
//		return topicPartitionInfoList;
//	}


}
