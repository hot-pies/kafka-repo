package com.coolhand.kafka.steam;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;
@Slf4j
public class GreetingAppTest {

    static TopologyTestDriver testDriver;

    @BeforeAll
    public static void setupTolology(){

        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"TestGreeting-Application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"TestGreeting:123");

        GreetingApp greetingApp= new GreetingApp();
        Topology tolology=greetingApp.createTopology();
        testDriver=new TopologyTestDriver(tolology,config);

    }

    @AfterAll
    public static void tearDown(){
        testDriver.close();
    }

    public void greetingMsgInput(String message){
        StringSerializer stringSerializer = new StringSerializer();
        TestInputTopic<String,String> inputTopic=testDriver.createInputTopic("greetings-input",stringSerializer,stringSerializer);
        inputTopic.pipeInput(message);
    }

    public TestOutputTopic<String,String> readOutputTopic(){
        StringDeserializer stringDeserializer = new StringDeserializer();
         return testDriver.createOutputTopic("greetings-output",stringDeserializer,stringDeserializer);
    }
    @Test
    public void TestGreatingMesageUpperCase(){
        System.out.println("*********** TestTopology ***********");
        greetingMsgInput("good morning");

        Assertions.assertEquals(readOutputTopic().readKeyValue(),new KeyValue<>(null,"GOOD MORNING"));
    }
}
