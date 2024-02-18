package com.coolhand.kafka.stream;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static com.coolhand.kafka.steam.topology.OrderTopology.ORDERS;

public class OrderTopologyTest {

    TopologyTestDriver topologyTestDriver=null;
    TestInputTopic<String , Order> orderInputTopic =null;

    static String INPUT_TOPIC=ORDERS;

    @BeforeEach
    void setUp() {

    }
}
