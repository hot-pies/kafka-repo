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
        producerConfig.put(ProducerConfig.LINGER_MS_CONFIG,"1");

        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String,String>(producerConfig);

        int msgPerSecond=100;
        long delayering=1000/msgPerSecond;

            try {

                System.out.println("\nExample 1 - new user\n");
                kafkaProducer.send(userRecords("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get();
                kafkaProducer.send(purchase("john", "Apples and Bananas (1)")).get();

                Thread.sleep(10000);

                // 2 - we receive user purchase, but it doesn't exist in Kafka
                System.out.println("\nExample 2 - non existing user\n");
                kafkaProducer.send(purchase("bob", "Kafka Udemy Course (2)")).get();

                Thread.sleep(10000);

                // 3 - we update user "john", and send a new transaction
                System.out.println("\nExample 3 - update to user\n");
                kafkaProducer.send(userRecords("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
                kafkaProducer.send(purchase("john", "Oranges (3)")).get();

                Thread.sleep(10000);

                // 4 - we send a user purchase for stephane, but it exists in Kafka later
                System.out.println("\nExample 4 - non existing user then user\n");
                kafkaProducer.send(purchase("stephane", "Computer (4)")).get();
                kafkaProducer.send(userRecords("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph")).get();
                kafkaProducer.send(purchase("stephane", "Books (4)")).get();
                kafkaProducer.send(userRecords("stephane", null)).get(); // delete for cleanup

                Thread.sleep(10000);

                // 5 - we create a user, but it gets deleted before any purchase comes through
                System.out.println("\nExample 5 - user then delete then data\n");
                kafkaProducer.send(userRecords("alice", "First=Alice")).get();
                kafkaProducer.send(userRecords("alice", null)).get(); // that's the delete record
                kafkaProducer.send(purchase("alice", "Apache Kafka Series (5)")).get();

                Thread.sleep(10000);

                System.out.println("End of demo");
                kafkaProducer.close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
    }

    private static ProducerRecord<String,String> userRecords(String key, String value){
        return new ProducerRecord<>(USER_TOPIC_NAME,key,value);
    }

    private static ProducerRecord<String,String> purchase(String key,String value ){
        return new ProducerRecord<>(PURCHASE_TOPIC_NAME,key,value);
    }
}
