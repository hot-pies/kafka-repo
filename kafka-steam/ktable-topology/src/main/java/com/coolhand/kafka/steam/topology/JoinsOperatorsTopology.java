package com.coolhand.kafka.steam.topology;

import com.coolhand.kafka.steam.domain.Alphabet;
import com.coolhand.kafka.steam.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class JoinsOperatorsTopology {
    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

//        joinKStreamWithKTable(streamsBuilder);
//        joinKStreamWithGlobalKTable(streamsBuilder);
//        joinKTableWithKTable(streamsBuilder);
        joinKStreamWithKStream(streamsBuilder);
        return streamsBuilder.build();
    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder) {

        var alphabetsAbbreviation= streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(),Serdes.String())
                );

        alphabetsAbbreviation
                .print(Printed.<String,String>toSysOut().withLabel("alphabets-Abbreviation"));

        var alphabetStream=streamsBuilder
                .stream(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String())
                        );

        alphabetStream
                .print(Printed.<String,String>toSysOut().withLabel("alphabet-Stream"));

        ValueJoiner<String ,String , Alphabet> valueJoiner = Alphabet::new;

        var fiveSecWindow=JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        StreamJoined<String ,String,String > joinedParams =
                StreamJoined.with(Serdes.String(),Serdes.String(), Serdes.String())
                        .withName("alphabet-join")
                        .withStoreName("alphabet-join")
                ;

        var joinSteam = alphabetsAbbreviation
                .outerJoin(alphabetStream,
                        valueJoiner,
                        fiveSecWindow,
                        joinedParams);

        joinSteam
                .print(Printed.<String,Alphabet>toSysOut().withLabel("alphabet_alphabets_Abbreviation"));



//
//
//        var joinedStream=alphabetsAbbreviation
//                .join(alphabetStream,valueJoiner);
//
//        joinedStream
//                .print(Printed.<String,Alphabet>toSysOut().withLabel("alphabetsWithAbbreviation"));
    }

    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {

        var alphabetsAbbreviation= streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(),Serdes.String())
                );

        alphabetsAbbreviation
                .print(Printed.<String,String>toSysOut().withLabel("alphabets-Abbreviation"));

        var alphabetTable=streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()),
                        Materialized.as("alphabet-Store"));

        alphabetTable
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("alphabet-Table"));


        ValueJoiner<String ,String , Alphabet> valueJoiner = Alphabet::new;

        var joinedStream=alphabetsAbbreviation
                .join(alphabetTable,valueJoiner);

        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel("alphabetsWithAbbreviation"));
    }

    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder) {

        var alphabetsAbbreviation= streamsBuilder
                .table(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(),Serdes.String()),
                        Materialized.as("alphabet-Abbreviation-Store")
                );

        alphabetsAbbreviation
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("alphabets-abbreviation"));

        var alphabetTable=streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()),
                        Materialized.as("alphabet-Store"));

        alphabetTable
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("alphabet-Table"));


        ValueJoiner<String ,String , Alphabet> valueJoiner = Alphabet::new;

        var joinedStream=alphabetsAbbreviation
                .join(alphabetTable,valueJoiner);

        joinedStream
                .toStream()
                .print(Printed.<String,Alphabet>toSysOut().withLabel("alphabets-with-Abbreviation"));
    }

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {

        var alphabetsAbbreviation= streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(),Serdes.String())
                );

        alphabetsAbbreviation
                .print(Printed.<String,String>toSysOut().withLabel("alphabets-Abbreviation"));

        var alphabetTable=streamsBuilder
                .globalTable(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()),
                        Materialized.as("alphabet-Store"));

//        alphabetTable
//                .toStream()
//                .print(Printed.<String,String>toSysOut().withLabel("alphabet-Table"));


        ValueJoiner<String ,String , Alphabet> valueJoiner = Alphabet::new;

        KeyValueMapper<String ,String,String> keyValueMapper = (leftkey,rigthkey)-> leftkey;
        var joinedStream=alphabetsAbbreviation
                .join(
                        alphabetTable,
                        keyValueMapper,
                        valueJoiner);

        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel("alphabetsWithAbbreviation"));
    }

}
