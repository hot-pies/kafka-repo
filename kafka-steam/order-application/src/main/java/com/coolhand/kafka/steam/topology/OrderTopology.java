package com.coolhand.kafka.steam.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

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


    public static Topology createTopology(){

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        return streamsBuilder.build();

    }
}
