package com.coolhand.kafka.steam.producer;

import java.util.Map;

import static com.coolhand.kafka.steam.producer.ProducerUtil.publishMessageSync;
import static com.coolhand.kafka.steam.topology.JoinsOperatorsTopology.ALPHABETS;
import static com.coolhand.kafka.steam.topology.JoinsOperatorsTopology.ALPHABETS_ABBREVATIONS;
import static java.lang.Thread.sleep;

public class JoinsMockDataProducer {
    public static void main(String[] args) {
        var alphabetMap = Map.of(
                "X", "X is the third last letter in English Alphabets.",
                "Y", "Y is the second last letter in English Alphabets."
//                ,
//                "Z", "E is the fifth letter in English Alphabets."
//                ,
//                "X", "A is the First letter in English Alphabets.",
//                "Y", "B is the Second letter in English Alphabets."
        );
//         publishMessages(alphabetMap, ALPHABETS);

//        try {
//            sleep(4000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

        var alphabetAbbrevationMap = Map.of(
                "X", "X-RAY",
                "Y", "Yeld."
                ,"Z", "Zoo."

        );
        publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);

        alphabetAbbrevationMap = Map.of(
                "X", "Xoom",
                "Y", "Yen."

        );
//         publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);
    }

    private static void publishMessages(Map<String, String> alphabetAbbrevationMap, String topicName) {
        alphabetAbbrevationMap
                .forEach((key,value)->{
                    var recordMetaData=publishMessageSync(topicName,key,value);
                });
    }
}
