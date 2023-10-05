package com.coolhand.kafka.steam.topology;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class JoinsOperatorsTopology {
    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        return streamsBuilder.build();
    }

}
