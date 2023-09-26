package com.coolhand.kafka.steam.producer;

import com.coolhand.kafka.steam.domain.Order;
import com.coolhand.kafka.steam.topology.OrderTopology;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

import static com.coolhand.kafka.steam.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class OrderMockDataProducer {

    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false);
    }

    private static void publishFutureRecords(ObjectMapper objectMapper){
        var localDateTime = LocalDateTime.now().plusDays(1);

        var newOrder = buildOrders()
                .stream()
                .map(order ->
                        new Order(order.orderId(),
                                order.locationId(),
                                order.finalAmount(),
                                order.orderType(),
                                order.orderLineItemList(),
                                localDateTime))
                .toList();

        publishOrder(objectMapper,newOrder);
    }

    private static void publishOrder(ObjectMapper objectMapper, List<Order> newOrder) {
        newOrder.forEach(order -> {
            try {
                var orderJSON = objectMapper.writeValueAsString(order);
                var recordMetaData = publishMessageSync(OrderTopology.ORDERS,order.orderId()+"",orderJSON);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static List<Order> buildOrders(){

        return null;
    }
}
