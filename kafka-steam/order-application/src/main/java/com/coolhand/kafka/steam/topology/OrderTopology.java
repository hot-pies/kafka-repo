package com.coolhand.kafka.steam.topology;

import com.coolhand.kafka.steam.domain.Order;
import com.coolhand.kafka.steam.domain.OrderType;
import com.coolhand.kafka.steam.domain.Revenue;
import com.coolhand.kafka.steam.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class OrderTopology {

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




    public static Topology buildTopology(){

        Predicate<String,Order> generalPredicate  = (key,order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String,Order> resturantPredicate = (key,order)-> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(),order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var ordersStream = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerdes())
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
                                            Produced.with(Serdes.String(),SerdesFactory.revenueSerdes()));

                })
                ).branch(resturantPredicate,
                        Branched.withConsumer(restaurantOrderStream->{
                            restaurantOrderStream
                                    .print(Printed.<String,Order>toSysOut().withLabel("restaurantStream : "));

                            restaurantOrderStream
                                    .mapValues((key,value)-> revenueValueMapper.apply(value))
                                    .to(RESTAURANT_ORDER,Produced.with(Serdes.String(),SerdesFactory.revenueSerdes()));

                        })
                );


        return streamsBuilder.build();

    }
}
