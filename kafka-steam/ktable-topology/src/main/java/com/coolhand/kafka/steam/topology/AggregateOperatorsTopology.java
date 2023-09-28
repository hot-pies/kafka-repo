package com.coolhand.kafka.steam.topology;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class AggregateOperatorsTopology {

    public static String WORDS = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder =new StreamsBuilder();




        return streamsBuilder.build();
    }

}
