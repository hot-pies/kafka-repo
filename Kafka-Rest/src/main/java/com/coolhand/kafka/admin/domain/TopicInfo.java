package com.coolhand.kafka.admin.domain;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public record TopicInfo(
        String name,
        boolean internal,
        List<PartitionInfo> partitionInfo,
        Set<AclOperation> authorizedOperations
) {

    public TopicInfo(TopicDescription topicDescription){
        this(topicDescription.name(),topicDescription.isInternal(),partitionsInfo(topicDescription.partitions()),topicDescription.authorizedOperations());
    }

    public Set<AclOperation>  authorizedOperations() {
        return authorizedOperations;
    }
    private static List<PartitionInfo> partitionsInfo(List<TopicPartitionInfo> topicPartitions){
        ArrayList<PartitionInfo> topicPartitionInfoList = new ArrayList<PartitionInfo>();
        for(TopicPartitionInfo topicPartitionInfo :topicPartitions){
            final PartitionInfo partitionInfo = new PartitionInfo(topicPartitionInfo);
            topicPartitionInfoList.add(partitionInfo);
        }
        return  topicPartitionInfoList;
    }
}
