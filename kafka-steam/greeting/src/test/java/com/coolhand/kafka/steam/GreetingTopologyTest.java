package com.coolhand.kafka.steam;

import com.coolhand.kafka.steam.domain.Greeting;
import com.coolhand.kafka.steam.serdes.SerdesFactory;
import com.coolhand.kafka.steam.topology.GreetingTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GreetingTopologyTest {
    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, Greeting> inputTopic=null;
    TestOutputTopic<String,Greeting> outputTopic=null;

    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(GreetingTopology.createTopology());

        inputTopic=topologyTestDriver.createInputTopic(GreetingTopology.GREETINGS,
                Serdes.String().serializer(), SerdesFactory.greetingSerdesUsingGenerics().serializer());

        outputTopic=topologyTestDriver.createOutputTopic(GreetingTopology.GREETINGS_UPPERCASE,
                Serdes.String().deserializer(),SerdesFactory.greetingSerdesUsingGenerics().deserializer());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void buildTopology() {
        inputTopic.pipeInput("GM", new Greeting("Good Morning!", LocalDateTime.now()));

        var count =outputTopic.getQueueSize();
        assertEquals(1,count);

        var outputValue=outputTopic.readKeyValue();
        assertEquals("GOOD MORNING!",outputValue.value.message());
        assertNotNull(outputValue.value.timeStamp());
    }

    @Test
    void buildTopology_multipleInput() {

        var greeting1= KeyValue.pair("GM", new Greeting("Good Morning!", LocalDateTime.now()));
        var greeting2= KeyValue.pair("GN", new Greeting("Good Night!", LocalDateTime.now()));
        inputTopic.pipeKeyValueList(List.of(greeting1,greeting2));

        var count =outputTopic.getQueueSize();
        assertEquals(2,count);

        var outputValues = outputTopic.readKeyValuesToList();

        var outputValue1=outputValues.get(0);
        assertEquals("GOOD MORNING!",outputValue1.value.message());
        assertNotNull(outputValue1.value.timeStamp());

        var outputValue2=outputValues.get(1);
        assertEquals("GOOD NIGHT!",outputValue2.value.message());
        assertNotNull(outputValue2.value.timeStamp());
    }

    @Test
    void buildTopology_Error() {
        inputTopic.pipeInput("GM", new Greeting("Transient Error", LocalDateTime.now()));

        var count =outputTopic.getQueueSize();
        assertEquals(0,count);

    }
}
