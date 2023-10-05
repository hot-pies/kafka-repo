package com.coolhand.kafka.steam.enrich;

import com.coolhand.kafka.steam.enrich.producer.UserEventEnrichProducer;
import com.coolhand.kafka.steam.enrich.stream.UserEventEnrichStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.Assert.assertEquals;

public class UserEventEnrichStreamTest {

//    org.apache.kafka.streams.T testDriver;

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer= new StringSerializer();
    ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
    @Before
    public void setUpTopologyTestDriver(){
        Properties streamConfig = new Properties();
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        streamConfig.put(StreamsConfig.AT_LEAST_ONCE,"true");
        streamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.AT_LEAST_ONCE);
        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        streamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        UserEventEnrichStream userEventEnrichStream= new UserEventEnrichStream();
        Topology topology = userEventEnrichStream.createTopology();
        testDriver = new TopologyTestDriver(topology, streamConfig);
    }

    @After
    public void closeTestDriver(){
        System.out.println("XXX CLOSE XXX");
        testDriver.close();
    }

    public void pushNewInputRecord(String value){
        testDriver.pipeInput(recordFactory.create("user-table","Test1", value));
    }
    @Test
    public void testDummy(){
        String dummy="du"+"mmy";
        assertEquals(dummy,"dummy");
    }

    public ProducerRecord<String,String> readOutput(){
        return testDriver.readOutput("ser-purchase-enriched-inner-join",new StringDeserializer(),new StringDeserializer());
    }

    @Test
    public void testUserPurchase(){
        String firstExamle="First=Junit-Test1,Last=Doe,Email=john.doe@gmail.com";
        pushNewInputRecord(firstExamle);
        ProducerRecord<String,String> record=readOutput();
        System.out.println(record.value());

//        ProducerRecord<String, Integer> outputRecord = testDriver.readOutput("output-topic", new StringDeserializer(), new LongDeserializer());
//        OutputVerifier.compareKeyValue(outputRecord, "key", 42L); // throws AssertionError if key or value does not match

//        OutputVerifier.compareKeyValue(readOutput(),firstExamle,"First=Junit-Test1,Last=Doe,Email=john.doe@gmail.com");


    }
}
