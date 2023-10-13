package com.coolhand.kafka.steam.producer;

import com.coolhand.kafka.steam.domain.Address;
import com.coolhand.kafka.steam.domain.Store;
import com.coolhand.kafka.steam.topology.OrderTopology;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.coolhand.kafka.steam.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class StoreMockDataProducer {

    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false);

        var address1 = new Address("123 Mathilda Ave", "", "Sunnyvale", "california", "12345");
        var store1 = new Store("store_0001", address1, "12345");
        var store2 = new Store("store_0002", address1, "12345");
        var store3 = new Store("store_0005", address1, "12345");

        var stores= List.of(store1,store2,store3);

        stores.forEach(store->{
            try {
                var storeJSON = objectMapper.writeValueAsString(store);
                var recordMetaData = publishMessageSync(OrderTopology.STORE,store.locationId(),storeJSON);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }







}
