package com.coolhand.kafka.steam.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
@Slf4j
public class WindowTopology {
    public static final String WINDOW_WORDS = "windows-words";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var wordStream=streamsBuilder
                .stream(WINDOW_WORDS,
                        Consumed.with(Serdes.String(),Serdes.String()));

//        tumblingWindow(wordStream);
//        hopingWindow(wordStream);

        slidingWindow(wordStream);
        return streamsBuilder.build();
    }

    private static void tumblingWindow(KStream<String ,String> wordStream) {

        Duration windowSize = Duration.ofSeconds(5);
        var timeWindow=TimeWindows.ofSizeWithNoGrace(windowSize);
        var windowedTable=wordStream
                .groupByKey()
                .windowedBy(timeWindow)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull())
                );

        windowedTable
                .toStream()
                .peek((key, value) -> {
                    log.info("tumblingWindow : Key : {} , Value : {} ",key,value);
                    printLocalDateTimes(key,value);

                })
                .print(Printed.<Windowed<String>,Long>toSysOut());
    }

    private static void hopingWindow(KStream<String ,String> wordStream) {

        Duration windowSize = Duration.ofSeconds(5);
        Duration advanceBySize = Duration.ofSeconds(3);

        var timeWindow=TimeWindows.ofSizeWithNoGrace(windowSize)
                .advanceBy(advanceBySize);

        var windowedTable=wordStream
                .groupByKey()
                .windowedBy(timeWindow)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull())
                );

        windowedTable
                .toStream()
                .peek((key, value) -> {
                    log.info("hopingWindow : Key : {} , Value : {} ",key,value);
                    printLocalDateTimes(key,value);

                })
                .print(Printed.<Windowed<String>,Long>toSysOut());
    }

    private static void slidingWindow(KStream<String ,String> wordStream) {

        Duration windowSize = Duration.ofSeconds(5);
        var slidingwindow=SlidingWindows.ofTimeDifferenceWithNoGrace(windowSize);

        var windowedTable=wordStream
                .groupByKey()
                .windowedBy(slidingwindow)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull())
                );

        windowedTable
                .toStream()
                .peek((key, value) -> {
                    log.info("slidingwindow : Key : {} , Value : {} ",key,value);
                    printLocalDateTimes(key,value);

                })
                .print(Printed.<Windowed<String>,Long>toSysOut());
    }
    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();
        log.info("startTime : {} , endTime : {} , cout : {} ",startTime,endTime,value);

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("PST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("PST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }
}
