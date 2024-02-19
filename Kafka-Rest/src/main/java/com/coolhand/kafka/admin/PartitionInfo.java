package com.coolhand.kafka.admin;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public record PartitionInfo(
        int partition,
        int leader,
        List<Node> replicas,
        List<Node> isr
) {

    // Constructor from TopicPartitionInfo
    public PartitionInfo(TopicPartitionInfo topicPartitionInfo) {
        this(topicPartitionInfo.partition(), topicPartitionInfo.leader().id(), Collections.unmodifiableList(topicPartitionInfo.replicas()), topicPartitionInfo.isr().stream().toList());
    }

    @Override
    public String toString() {
        return "PartitionInfo{" +
                "partition=" + partition +
                ", leader=" + leader +
                ", replicas=" + replicas +
                ", isr=" + isr +
                '}';
    }
}
