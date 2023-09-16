package com.coolhand.kafka.producer;

import com.coolhand.kafka.customserializers.KafkaJsonSerializer;
import com.coolhand.kafka.customers.CustomerProfile;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BalanceUpdatesProducer {
    public static void main(String[] args) {

        String BOOTSTRAP_SERVER="192.168.55.11:9092";
        String TOPIC_NAME="bank-balance-updates-v2";

        Properties prodcerConfig = new Properties();
        prodcerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        prodcerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        prodcerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        prodcerConfig.put(ProducerConfig.RETRIES_CONFIG,"3");
        prodcerConfig.put(ProducerConfig.ACKS_CONFIG,"all");
        prodcerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        int messagesPerSecond = 100;
        long delayBetweenMessages = 1000 / messagesPerSecond;

        KafkaProducer<String,com.coolhand.kafka.customers.CustomerProfile> producer = new KafkaProducer<String,com.coolhand.kafka.customers.CustomerProfile>(prodcerConfig);
        int i=0;
        while(i<=100){

            CustomerProfile customer=getCustomerProfile();
            ProducerRecord records=new ProducerRecord<String,com.coolhand.kafka.customers.CustomerProfile>(TOPIC_NAME,customer.getName(),customer);
            producer.send(records);
            System.out.println("************************ Record send to topic ************************");
            try {
                Thread.sleep(delayBetweenMessages);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }

        producer.flush();
        producer.close();

    }

    public static CustomerProfile getCustomerProfile(){

        CustomerProfile customerProfile = new CustomerProfile();

        // Generate a random name from a list of names
        String names[]={"VIVEK","HEMA","RAGHAV","AMRISH","VICKY","SAM"};
        String randomName = names[new Random().nextInt(names.length)];

        // Generate a random number between a specified range
        int minNumber = 1;
        int maxNumber = 99;
        int randomNumber = (int) (Math.floor(Math.random() * (maxNumber - minNumber + 1)) + minNumber);

        customerProfile.setName(randomName);
        customerProfile.setAmount(randomNumber);
        customerProfile.setTime(Instant.now().toString());

        return customerProfile;
    }

    public static CustomerProfile getCustomerProfile(String name){

        CustomerProfile customerProfile = new CustomerProfile();

        String randomName = String.valueOf(new Random().nextInt(name.length()));

        // Generate a random number between a specified range
        int minNumber = 1;
        int maxNumber = 99;
//        int randomNumber = (int) (Math.floor(Math.random() * (maxNumber - minNumber + 1)) + minNumber);
        int amount= ThreadLocalRandom.current().nextInt(minNumber,maxNumber);

        customerProfile.setName(name);
        customerProfile.setAmount(amount);
        customerProfile.setTime(Instant.now().toString());


        return customerProfile;
    }

}

