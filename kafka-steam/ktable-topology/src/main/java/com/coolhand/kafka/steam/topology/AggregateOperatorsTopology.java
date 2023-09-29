package com.coolhand.kafka.steam.topology;

import com.coolhand.kafka.steam.domain.AlphabetWordAggregate;
import com.coolhand.kafka.steam.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;


@Slf4j
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
                .groupByKey(Grouped.with(Serdes.String(),Serdes.String()));
//                .groupBy((key,value)-> value,
//                    Grouped.with(Serdes.String(),Serdes.String()));


//        exploreCount(groupString);
        exploreReduce(groupString);
//        exploreAggregator(groupString);


        return streamsBuilder.build();
    }

    private static void exploreAggregator(KGroupedStream<String, String> groupedStream) {
        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer=AlphabetWordAggregate::new;
        Aggregator<String,String,AlphabetWordAggregate> aggregator = (key,value,aggregate) -> aggregate.updateNewEvents(key,value);

        var aggregatedStream =groupedStream
                .aggregate(alphabetWordAggregateInitializer,
                        aggregator,
                        Materialized.<String ,AlphabetWordAggregate, KeyValueStore<Bytes,byte[]>>as("aggregated-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.alphabetWordAggregateSerde())
                );

        aggregatedStream
                .toStream()
                .print(Printed.<String ,AlphabetWordAggregate>toSysOut().withLabel("Aggregated-Stream"));
    }

    private static void exploreReduce(KGroupedStream<String, String> groupedStream) {

        var reduceStream=groupedStream
                .reduce(((value1, value2) -> {
                    log.info("value1 : {} , value2 : {} ",value1,value2);
                    return value1.toUpperCase()+"-"+value2.toUpperCase();
                }),
                Materialized.
                        <String ,String,KeyValueStore<Bytes,byte[]>>as("reduce-words")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        reduceStream
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("reduce-words"));
    }

    private static void exploreCount(KGroupedStream<String, String> groupedStream) {

        var countByAlphabet = groupedStream
                .count(Named.as("count-per-alphabet"));

        countByAlphabet
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel("alphabet-char-count"));
    }
}
