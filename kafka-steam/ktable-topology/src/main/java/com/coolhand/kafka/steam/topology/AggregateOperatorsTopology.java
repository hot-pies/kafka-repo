package com.coolhand.kafka.steam.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class AggregateOperatorsTopology {

    public static String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder =new StreamsBuilder();

        var inputStream =streamsBuilder
                .stream(AGGREGATE,
                        Consumed.with(Serdes.String(),Serdes.String()));

        inputStream
                .print(Printed.<String ,String >toSysOut().withLabel(AGGREGATE));


        var groupString =inputStream
//                .groupByKey(Grouped.with(Serdes.String(),Serdes.String()));
                .groupBy((key,value)-> value,
                    Grouped.with(Serdes.String(),Serdes.String()));


        exploreCount(groupString);


        return streamsBuilder.build();
    }

    private static void exploreCount(KGroupedStream<String, String> groupedStream) {

        var countByAlphabet = groupedStream
                .count(Named.as("count-per-alphabet"));

        countByAlphabet
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel("alphabet-char-count"));
    }

}
