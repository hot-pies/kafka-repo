package com.coolhand.kafka.steam.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

public class KTableTopology {

    public static String WORDS="words";

    public static Topology build(){
        StreamsBuilder streamsBuilder= new StreamsBuilder();

        var wordTable = streamsBuilder.table(WORDS,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("words-store")

        );

        wordTable
                .filter((key,value)-> value.length() >2)
                .mapValues((key,value)->value.toUpperCase())
                .toStream()
                .print(Printed.<String , String>toSysOut().withLabel("Words-KTable"));

        return streamsBuilder.build();

    }
}
