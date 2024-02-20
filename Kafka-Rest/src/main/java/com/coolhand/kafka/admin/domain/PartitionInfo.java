package com.coolhand.kafka.admin.domain;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.List;

public record PartitionInfo(
        int partition,
        int leader,
        List<Integer> replicas,
        List<Integer> isr
) {

    // Constructor from TopicPartitionInfo
    public PartitionInfo(TopicPartitionInfo topicPartitionInfo) {
        this(topicPartitionInfo.partition(), topicPartitionInfo.leader().id(), extractIds(topicPartitionInfo.replicas()), extractIds(topicPartitionInfo.isr()));
    }

    private static List<Integer> extractIds(List<org.apache.kafka.common.Node> nodes) {
        List<Integer> ids = new ArrayList<>();
        for (org.apache.kafka.common.Node node : nodes) {
            ids.add(node.id());
        }
        return ids;
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
