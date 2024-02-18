package com.coolhand.kafka.steam.topology;

import com.coolhand.kafka.steam.domain.Greeting;
import com.coolhand.kafka.steam.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_SPANISH = "greetings-spanish";
    public static String GREETINGS_UPPERCASE = "greetings_uppercase";


    public static Topology createTopology(){
        StreamsBuilder builder=new StreamsBuilder();

//        KStream<String, String> merge_greeting = getStringGreetingKStream(builder);
        var merge_greeting=getCustomGreetingKStream(builder);

        merge_greeting.peek((key,Greeting)-> log.info("After topic Merge key :{} , value : {} ",key,Greeting));

        var modifiedStream=explorerErrrs(merge_greeting);

        modifiedStream
                .print(Printed.<String,Greeting>toSysOut().withLabel("modifiedStream"));

//        var modified_greeting = greeting_stream
        var modified_greeting = modifiedStream
                .mapValues((key,value)->
                        new Greeting(value.message().toUpperCase(),value.timeStamp()))
//                .map((key,value)->KeyValue.pair(key.toUpperCase(),value.toUpperCase()))
//                .flatMap((key, value) -> {
//                    var newValues = Arrays.asList(value.split(""));
////                    newValues.forEach(element-> log.info("Split modified greeting : "+element));
//                    return newValues
//                            .stream()
//                            .map(val-> KeyValue.pair(key.toUpperCase(),val.toUpperCase()))
//                            .collect(Collectors.toList());
//                })
                .peek((key, value) -> {
                    log.info("After modifications : key : {}, value : {}",key,value);
                })
                ;

        modified_greeting
                .print(Printed.<String,Greeting>toSysOut().withLabel("modified_greeting:"));

        modified_greeting.to(GREETINGS_UPPERCASE
                , Produced.with(Serdes.String(), SerdesFactory.greetingSerdesUsingGenerics())
        );

//        KStream<String,String> streamIn = builder.stream("greetings-input");

//        KStream<String,String> streamOut = streamIn
//                .mapValues((key,value)->value.toLowerCase());
//
//        streamOut.to("greetings-output");

        return  builder.build();
    }

    private static KStream<String,Greeting> explorerErrrs(KStream<String, Greeting> mergeGreeting) {
        return mergeGreeting
                .mapValues((readOnlyKey, value) -> {
                    if(value.message().equals("Transient Error")){
                        try{
                            throw new IllegalStateException(value.message());
                        }catch (Exception e){
                            log.error("Exception in explorerErrors : {} ",e.getMessage());
                            return null;
                        }

                    }
                    return new Greeting(value.message().toUpperCase(),value.timeStamp());
                })
                .filter((key, value) -> key!=null && value !=null);
    }

    private static KStream<String, String> getStringGreetingKStream(StreamsBuilder builder) {
        KStream<String,String> greeting_stream= builder.stream(GREETINGS
//                , Consumed.with(Serdes.String(),Serdes.String())
        );
        greeting_stream.print(Printed.<String,String>toSysOut().withLabel("greeting_stream"));
        greeting_stream.peek((key, value) -> {
            log.info("Before modification key : {}, value : {}",key,value);
        });

        KStream<String,String> greeting_spanish_stream= builder
                .stream(GREETINGS_SPANISH
//                , Consumed.with(Serdes.String(),Serdes.String())
        );
        greeting_stream.print(Printed.<String,String>toSysOut().withLabel("greeting_stream"));
        greeting_stream.peek((key, value) -> {
            log.info("Before modification key : {}, value : {}",key,value);
        });

        var merge_greeting=greeting_stream.merge(greeting_spanish_stream);
        return merge_greeting;
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder builder) {
        var greeting_stream= builder
                .stream(GREETINGS
//                , Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes())
                , Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesUsingGenerics())

        );
        greeting_stream.print(Printed.<String, Greeting>toSysOut().withLabel("greeting_stream"));

        greeting_stream.peek((key, value) -> {
            log.info("Before modification key : {}, value : {}",key,value);
        });

        var greeting_spanish_stream= builder
                .stream(GREETINGS_SPANISH
                , Consumed.with(Serdes.String(),SerdesFactory.greetingSerdesUsingGenerics())
        );
        greeting_stream.print(Printed.<String,Greeting>toSysOut().withLabel("greeting_stream"));
        greeting_stream.peek((key, value) -> {
            log.info("Before modification key : {}, value : {}",key,value);
        });

        var merge_greeting=greeting_stream.merge(greeting_spanish_stream);

        return merge_greeting;
    }

}
