package com.coolhand.kafka.steam.serdes;

import com.coolhand.kafka.steam.domain.Alphabet;
import com.coolhand.kafka.steam.domain.AlphabetWordAggregate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {
    public static Serde<AlphabetWordAggregate> alphabetWordAggregateSerde(){
        JsonSerializer<AlphabetWordAggregate> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<AlphabetWordAggregate> jsonDeserializer = new JsonDeserializer<>(AlphabetWordAggregate.class);
        return Serdes.serdeFrom(jsonSerializer,jsonDeserializer);
    }

    public static Serde<Alphabet> alphabetSerde(){
        JsonSerializer<Alphabet> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Alphabet> jsonDeserializer = new JsonDeserializer<>(Alphabet.class);
        return Serdes.serdeFrom(jsonSerializer,jsonDeserializer);
    }

}
