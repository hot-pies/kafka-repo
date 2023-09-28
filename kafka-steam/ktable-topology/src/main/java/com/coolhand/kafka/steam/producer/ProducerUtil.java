package com.coolhand.kafka.steam.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerUtil {
    static KafkaProducer<String ,String > producer = new KafkaProducer<>(producerProps());

    private static Properties producerProps() {

        Properties config=new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "92.168.55.11:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return config;
    }

    public static RecordMetadata publishMessageSync(String topicName,String key,String message){

        ProducerRecord<String,String > producerRecord = new ProducerRecord<>(topicName,key,message);
        RecordMetadata recordMetadata = null;

        try {
            recordMetadata= producer.send(producerRecord).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    return recordMetadata;
    }

}
