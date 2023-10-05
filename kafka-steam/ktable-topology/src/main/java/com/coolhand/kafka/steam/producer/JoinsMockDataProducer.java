package com.coolhand.kafka.steam.producer;

import java.util.Map;

import static com.coolhand.kafka.steam.producer.ProducerUtil.publishMessageSync;
import static com.coolhand.kafka.steam.topology.JoinsOperatorsTopology.ALPHABETS_ABBREVATIONS;

public class JoinsMockDataProducer {
    public static void main(String[] args) {
        var alphabetMap = Map.of(
                "A", "A is the first letter in English Alphabets.",
                "B", "B is the second letter in English Alphabets."
                //              ,"E", "E is the fifth letter in English Alphabets."
//                ,
//                "A", "A is the First letter in English Alphabets.",
//                "B", "B is the Second letter in English Alphabets."
        );
        // publishMessages(alphabetMap, ALPHABETS);

        // sleep(6000);

        var alphabetAbbrevationMap = Map.of(
                "A", "Apple",
                "B", "Bus."
                ,"C", "Cat."

        );
        publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);

        alphabetAbbrevationMap = Map.of(
                "A", "Airplane",
                "B", "Baby."

        );
        // publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);
    }

    private static void publishMessages(Map<String, String> alphabetAbbrevationMap, String topicName) {
        alphabetAbbrevationMap
                .forEach((key,value)->{
                    var recordMetaData=publishMessageSync(topicName,key,value);
                });
    }
}
