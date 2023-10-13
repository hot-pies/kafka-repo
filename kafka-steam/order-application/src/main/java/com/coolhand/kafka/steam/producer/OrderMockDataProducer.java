package com.coolhand.kafka.steam.producer;

import com.coolhand.kafka.steam.domain.Order;
import com.coolhand.kafka.steam.domain.OrderLineItem;
import com.coolhand.kafka.steam.domain.OrderType;
import com.coolhand.kafka.steam.topology.OrderTopology;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static com.coolhand.kafka.steam.producer.ProducerUtil.publishMessageSync;
import static java.lang.Thread.sleep;

@Slf4j
public class OrderMockDataProducer {

    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false);

//        publishOrder(objectMapper,buildOrders());
        publishBulkOrders(objectMapper);
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

    private static void publishExpiredRecords(ObjectMapper objectMapper){
        var localDateTime=LocalDateTime.now().minusDays(1);

//        var newOrders=buildOrders()

    }
    private static void publishBulkOrders(ObjectMapper objectMapper){
        int count =0;
        while(count <100){
            var orders = buildOrders();
            publishOrder(objectMapper,orders);
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            count++;
        }
    }
    private static void publishOrder(ObjectMapper objectMapper, List<Order> newOrder) {
        newOrder
                .forEach(order -> {
                try {
                    var orderJSON = objectMapper.writeValueAsString(order);
                    var recordMetaData = publishMessageSync(OrderTopology.ORDERS,order.orderId()+"",orderJSON);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    log.error("Exception : {} ", e.getMessage(), e);
                    throw new RuntimeException(e);
                }
        });
    }

    private static List<Order> buildOrders(){

        var orderItems = List.of(
                new OrderLineItem("Banana",2,new BigDecimal(2.00)),
                new OrderLineItem("Apple",2,new BigDecimal(6.00))
        );

        var restaurantItems = List.of(
                new OrderLineItem("Pizza",2,new BigDecimal(12.00)),
                new OrderLineItem("Soda",2,new BigDecimal(2.00))
        );

        var order1 = new Order(
                10001,
                "store_0001",
                new BigDecimal("8.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now());

        var order2 = new Order(
                50001,
                "store_0002",
                new BigDecimal("14.00"),
                OrderType.RESTAURANT,
                restaurantItems,
                LocalDateTime.now()
        );

        var order3 = new Order(
                10002,
                "store_0005",
                new BigDecimal("20.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()
        );

        var order4 = new Order(
                50001,
                "store_0002",
                new BigDecimal("24.00"),
                OrderType.RESTAURANT,
                restaurantItems,
                LocalDateTime.now()
        );

        return List.of(
                order1,
                order2,
                order3,
                order4

                );
    }

    private static List<Order> buildOrdersForGracePeriod(){

        var orderItems = List.of(
                new OrderLineItem("Banana",2,new BigDecimal(2.00)),
                new OrderLineItem("Apple",2,new BigDecimal(6.00))
        );

        var restaurantItems = List.of(
                new OrderLineItem("Pizza",2,new BigDecimal(12.00)),
                new OrderLineItem("Soda",2,new BigDecimal(2.00))
        );

        var order1 = new Order(
                10001,
                "store_0001",
                new BigDecimal("8.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.parse("023-12-06T18:50:21"));

        var order2 = new Order(
                50001,
                "store_0002",
                new BigDecimal("14.00"),
                OrderType.RESTAURANT,
                restaurantItems,
                LocalDateTime.parse("023-12-06T18:50:21")
        );

        var order3 = new Order(
                10002,
                "store_0005",
                new BigDecimal("20.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.parse("023-12-06T18:50:21")
        );

        return List.of(
                order1,
                order2,
                order3
        );
    }
}
