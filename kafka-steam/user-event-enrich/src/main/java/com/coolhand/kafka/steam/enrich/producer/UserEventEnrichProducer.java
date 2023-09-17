package com.coolhand.kafka.steam.enrich.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UserEventEnrichProducer {
    static String BOOTSTRAP_SERVER="192.168.55.11:9092";
    static String USER_TOPIC_NAME="user-table";
    static String PURCHASE_TOPIC_NAME="user-purchase";
    public static void main(String[] args) {

        Properties producerConfig=new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        producerConfig.put(ProducerConfig.ACKS_CONFIG,"all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG,3);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
//        producerConfig.put(ProducerConfig.LINGER_MS_CONFIG,"100");

        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String,String>(producerConfig);

        int msgPerSecond=100;
        long delayering=1000/msgPerSecond;

            try {
                kafkaProducer.send(userRecords("VICKY","First=VIVEK,Last=KUMAR,VIVEK.VKGUPTA@GMAIL.COM")).get();
                kafkaProducer.send(purchase("VIVEK","IWATCH-ULTRA ( 1 )")).get();
                Thread.sleep(delayering);

                kafkaProducer.send(userRecords("HEMLATA","First=HEMLATA,Last=GUPTA,HEEMLATA@GMAIL.COM")).get();
                kafkaProducer.send(purchase("HEMLATA","AIRPOD ( 2 )")).get();
                kafkaProducer.send(purchase("AMRISH","MACBOOK ( 2 )")).get();
                Thread.sleep(delayering);

                kafkaProducer.send(userRecords("RAGHAV","First=RAGHAV,Last=GUPTA,RAGHAV@GMAIL.COM")).get();
                kafkaProducer.send(userRecords("AMRISH","First=RAGHU")).get();
                kafkaProducer.send(purchase("RAGHAV","PS2 ( 3 )")).get();
                kafkaProducer.send(purchase("VIVEK","MACBOOK ( 3 )")).get();
                Thread.sleep(delayering);

                kafkaProducer.send(userRecords("AMRISH","NULL")).get();
                kafkaProducer.send(purchase("RAGHAV","PS2 ( 3 )")).get();
                kafkaProducer.send(purchase("VIVEK","MACBOOK ( 3 )")).get();
                Thread.sleep(delayering);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
//        }
    }

    private static ProducerRecord<String,String> userRecords(String key, String value){
        return new ProducerRecord<>(USER_TOPIC_NAME,key,value);
    }

    private static ProducerRecord<String,String> purchase(String key,String value ){
        return new ProducerRecord<>(PURCHASE_TOPIC_NAME,key,value);
    }
}
