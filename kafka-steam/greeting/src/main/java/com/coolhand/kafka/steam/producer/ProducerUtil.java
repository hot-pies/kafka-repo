package com.coolhand.kafka.steam.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
@Slf4j
public class ProducerUtil {
    static KafkaProducer<String,String> producer =new KafkaProducer<String,String>(producerProps());

    private static Map<String, Object> producerProps() {
        Map<String,Object> propsmap= new HashMap<>();
        propsmap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.55.11:9092");
        propsmap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsmap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return propsmap;
    }

    public static RecordMetadata publishMessageSync(String topicName, String key, String greetingMessage) {

        ProducerRecord<String,String> producerRecord=new ProducerRecord<>(topicName,key,greetingMessage);
        RecordMetadata recordMetadata=null;

        try {
            log.info("producer Record : {}",producerRecord);
            recordMetadata=producer.send(producerRecord).get();

        } catch (InterruptedException e) {
            log.info("InterruptedException in publishMessageSync : {}",e.getMessage(),e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            log.info("RuntimeException in publishMessageSync : {}",e.getMessage(),e);
            throw new RuntimeException(e);
        }catch (Exception e){
            log.info("Exception in publishMessageSync : {}",e.getMessage(),e);
        }
        return recordMetadata;
    }

}
