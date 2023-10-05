package com.coolhand.kafka.steam.producer;

import lombok.extern.slf4j.Slf4j;

import static com.coolhand.kafka.steam.producer.ProducerUtil.publishMessageSync;
import static com.coolhand.kafka.steam.topology.WindowTopology.WINDOW_WORDS;
import static java.lang.Thread.sleep;
@Slf4j
public class WindowsMockDataProduer {
    public static void main(String[] args) {

        bulkMockDataProducer();
//        bulkMockDataProducer_SlidingWindows();
    }

    private static void bulkMockDataProducer_SlidingWindows() {
        var key = "A";
        var word = "Apple";
        int count = 0;
        while(count<10){
            var recordMetaData = publishMessageSync(WINDOW_WORDS, key,word);
            log.info("Published the alphabet message : {} ", recordMetaData);
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            count++;
        }
    }

    private static void bulkMockDataProducer() {
        var key = "A";
        var word = "Apple";
        int count = 0;
        while(count<100){
            var recordMetaData = publishMessageSync(WINDOW_WORDS, key,word);
            log.info("Published the alphabet message : {} ", recordMetaData);
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            count++;
        }
    }
}
