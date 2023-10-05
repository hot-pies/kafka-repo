package com.coolhand.kafka.steam.orders.topology;

import com.coolhand.kafka.stream.orders.domain.Order;
import com.coolhand.kafka.stream.orders.domain.OrderType;
import com.coolhand.kafka.stream.orders.domain.Revenue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;



@Component
@Slf4j
public class OrderManagementTopology {

    public static final String ORDER="order";
    public static final String ORDERS="orders";
    public static final String GENERAL_ORDER="general_orders";
    public static final String GENERAL_ORDER_COUNT="general_orders_count";
    public static final String GENERAL_ORDER_COUNT_WINDOWS="general_orders_count_window";
    public static final String GENERAL_ORDER_REVENUE="general_orders_revenue";

    public static final String GENERAL_ORDER_REVENUE_WINDOWS="general_orders_revenue_window";

    public static final String RESTAURANT_ORDER="restaurant_orders";
    public static final String RESTAURANT_ORDER_COUNT="restaurant_orders_count";
    public static final String RESTAURANT_ORDER_REVENUE="restaurant_orders_revenue";
    public static final String RESTAURANT_ORDER_COUNT_WINDOWS="restaurant_orders_count_window";
    public static final String RESTAURANT_ORDER_REVENUE_WINDOWS="restaurant_orders_revenue_window";


    @Autowired
    public void process(StreamsBuilder streamsBuilder){
        getOrder(streamsBuilder);
//        buildTopology(streamsBuilder);

    }

    public void getOrder(StreamsBuilder streamsBuilder){

        var greeting = streamsBuilder.stream(ORDER);
        greeting.print(Printed.toSysOut().withLabel("GREETING "));
    }

    public static Topology buildTopology(StreamsBuilder streamsBuilder){

        Predicate<String, Order> generalPredicate  = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String,Order> resturantPredicate = (key,order)-> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(),order.finalAmount());

        var ordersStream = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), new JsonSerde<Order>(Order.class))
                );

        ordersStream
                .print(Printed.<String , Order>toSysOut().withLabel("orders :"));

        ordersStream
                .split(Named.as("General-Restaurant-Stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrderStream->{
                            generalOrderStream
                                    .print(Printed.<String,Order>toSysOut().withLabel("generalStream : "));

                            generalOrderStream
                                    .mapValues((ReadOnlyKey,value)-> revenueValueMapper.apply(value))
                                    .to(GENERAL_ORDER,
                                            Produced.with(Serdes.String(),new JsonSerde<Revenue>(Revenue.class)));

                        })
                ).branch(resturantPredicate,
                        Branched.withConsumer(restaurantOrderStream->{
                            restaurantOrderStream
                                    .print(Printed.<String,Order>toSysOut().withLabel("restaurantStream : "));

                            restaurantOrderStream
                                    .mapValues((key,value)-> revenueValueMapper.apply(value))
                                    .to(RESTAURANT_ORDER,Produced.with(Serdes.String(),new JsonSerde<Revenue>(Revenue.class)));

                        })
                );
        return streamsBuilder.build();
    }
}
