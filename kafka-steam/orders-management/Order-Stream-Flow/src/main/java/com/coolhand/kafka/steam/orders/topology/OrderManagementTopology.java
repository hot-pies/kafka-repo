package com.coolhand.kafka.steam.orders.topology;

import com.coolhand.kafka.steam.orders.util.OrderTimeStampExtractor;
import com.coolhand.kafka.stream.orders.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;


@Component
@Slf4j
public class OrderManagementTopology {

    public static final String ORDER="order";
    public static final String ORDERS="orders";
    public static final String STORE="stores";
    public static final String GENERAL_ORDER="general_orders";
    public static final String GENERAL_ORDER_COUNT="general_orders_count";
    public static final String GENERAL_ORDER_REVENUE="general_orders_revenue";
    public static final String GENERAL_ORDER_COUNT_WINDOWS="general_orders_count_window";
    public static final String GENERAL_ORDER_REVENUE_WINDOWS="general_orders_revenue_window";
    public static final String RESTAURANT_ORDER="restaurant_orders";
    public static final String RESTAURANT_ORDER_COUNT="restaurant_orders_count";
    public static final String RESTAURANT_ORDER_REVENUE="restaurant_orders_revenue";
    public static final String RESTAURANT_ORDER_COUNT_WINDOWS="restaurant_orders_count_window";
    public static final String RESTAURANT_ORDER_REVENUE_WINDOWS="restaurant_orders_revenue_window";


    @Autowired
    public void process(StreamsBuilder streamsBuilder){

        buildTopology(streamsBuilder);
    }

    public static Topology buildTopology(StreamsBuilder streamsBuilder){

        Predicate<String, Order> generalPredicate  = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String,Order> resturantPredicate = (key,order)-> order.orderType().equals(OrderType.RESTAURANT);


        var ordersStream = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key, value) -> value.locationId());

        ordersStream
                .print(Printed.<String , Order>toSysOut().withLabel("orders :"));

        //        KSTREAM - KTABLE
        var storeTable=streamsBuilder
                .table(STORE,
                        Consumed.with(Serdes.String(),new JsonSerde<>(Store.class))
                );

        storeTable
                .toStream()
                .print(Printed.<String , Store>toSysOut().withLabel("orders :"));

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(),order.finalAmount());

        ordersStream
                .split(Named.as("General-Restaurant-Stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrderStream->{
                            generalOrderStream
                                    .print(Printed.<String,Order>toSysOut().withLabel("generalStream : "));

                            generalOrderStream
                                    .mapValues((ReadOnlyKey,value)-> revenueValueMapper.apply(value))
                                    .to(GENERAL_ORDER,
                                            Produced.with(Serdes.String(),new JsonSerde<>(Revenue.class)));

                            aggregateOrderByCount(generalOrderStream, GENERAL_ORDER_COUNT);
                            aggregateOrderCountByTimeWindows(generalOrderStream, GENERAL_ORDER_COUNT_WINDOWS);
                            aggregateOrderByRevenue(generalOrderStream, GENERAL_ORDER_REVENUE, storeTable);
                            aggregateOrderByRevenueWindow(generalOrderStream, GENERAL_ORDER_REVENUE_WINDOWS, storeTable);
                        })
                ).branch(resturantPredicate,
                        Branched.withConsumer(restaurantOrderStream->{
                            restaurantOrderStream
                                    .print(Printed.<String,Order>toSysOut().withLabel("restaurantStream : "));

                            restaurantOrderStream
                                    .mapValues((ReadOnlyKey,value)-> revenueValueMapper.apply(value))
                                    .to(RESTAURANT_ORDER,
                                            Produced.with(Serdes.String(),new JsonSerde<>(Revenue.class)));

                            aggregateOrderByCount(restaurantOrderStream, RESTAURANT_ORDER_COUNT);
                            aggregateOrderCountByTimeWindows(restaurantOrderStream, RESTAURANT_ORDER_COUNT_WINDOWS);
                            aggregateOrderByRevenue(restaurantOrderStream, RESTAURANT_ORDER_REVENUE, storeTable);
                            aggregateOrderByRevenueWindow(restaurantOrderStream, RESTAURANT_ORDER_REVENUE_WINDOWS, storeTable);
                        })
                );
        return streamsBuilder.build();
    }

    private static void aggregateOrderByRevenueWindow
            (KStream<String, Order> orderStream, String storeName, KTable<String, Store> storeTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;
        Aggregator<String,Order,TotalRevenue> aggregator
                = (key,value,aggregate) -> aggregate.updateRunningRevenue(key,value);

        var revenueTable= orderStream
                .map((key,value)->KeyValue.pair(value.locationId(),value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .windowedBy(timeWindows)
                .aggregate(
                        totalRevenueInitializer,
                        aggregator,
                        Materialized.<String,TotalRevenue, WindowStore<Bytes,byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class))
                );

//       KTABLE - KTABLE Join
        ValueJoiner<TotalRevenue,Store, TotalRevenueWithAddress> valueJoiner=TotalRevenueWithAddress::new;
        var joinParam =Joined.with(Serdes.String(),new JsonSerde<>(TotalRevenue.class),new JsonSerde<>(Store.class));

        revenueTable
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(),value))
                .join(storeTable,valueJoiner,joinParam)
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(storeName+"ByStoreName"));

        revenueTable
                .toStream()
                .peek((key, value) -> {
                    log.info("Store Name : {} , Key : {} , Value :{}",storeName,key,value);
                    printLocalDateTimes(key,value);
                })
                .print(Printed.<Windowed<String>, TotalRevenue>toSysOut().withLabel(storeName));
    }


    private static void aggregateOrderCountByTimeWindows(KStream<String, Order> orderStream, String storeName) {
        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        var ordersCountPerStore = orderStream
                .map((key, value) -> KeyValue.pair(value.locationId(),value))
                .groupByKey(Grouped.with(Serdes.String(),new JsonSerde<>(Order.class)))
                .windowedBy(timeWindows)
                .count(Named.as(storeName),Materialized.as(storeName))
                .suppress(Suppressed
                        .untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        ordersCountPerStore
                .toStream()
//                .print(Printed.<String ,Long>toSysOut().withLabel(generalOrderCountWindows)
                .peek((key, value) -> {
                    log.info("Store Name : {} , Key : {} , Value :{}",storeName,key,value);
                    printLocalDateTimes(key,value);
                })
                .print(Printed.<Windowed<String>,Long>toSysOut().withLabel(storeName));

    }

    private static void aggregateOrderByRevenue
            (KStream<String, Order> orderStream, String storeName, KTable<String, Store> storeTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;
        Aggregator<String,Order,TotalRevenue> aggregator
                = (key,value,aggregate) -> aggregate.updateRunningRevenue(key,value);

        var revenueTable= orderStream
                .map((key,value)-> KeyValue.pair(value.locationId(),value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .windowedBy(timeWindows)
                .aggregate(
                        totalRevenueInitializer,
                        aggregator,
                        Materialized.<String,TotalRevenue, WindowStore<Bytes,byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class))
                );

//       KTABLE - KTABLE Join
        ValueJoiner<TotalRevenue,Store,TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var joinParam =Joined.with(Serdes.String(),new JsonSerde<>(TotalRevenue.class), new JsonSerde<Store>(Store.class));

        revenueTable
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(),value))
                .join(storeTable,valueJoiner,joinParam)
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(storeName+"ByStoreName"));

        revenueTable
                .toStream()
                .peek((key, value) -> {
                    log.info("Store Name : {} , Key : {} , Value :{}",storeName,key,value);
                    printLocalDateTimes(key,value);
                })
                .print(Printed.<Windowed<String>, TotalRevenue>toSysOut().withLabel(storeName));
    }

    private static void aggregateOrderByCount(KStream<String, Order> generalOrderStream, String storeName) {
        var ordersCountPerStore = generalOrderStream

//                .map((key, value) -> KeyValue.pair(value.locationId(),value))
                .groupByKey(Grouped.with(Serdes.String(),new JsonSerde<>(Order.class)))
                .count(Named.as(storeName),Materialized.as(storeName));

        ordersCountPerStore
                .toStream()
                .print(Printed.<String ,Long>toSysOut().withLabel(storeName));



    }

    private static void printLocalDateTimes(Windowed<String> key, Object value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();
        log.info("startTime : {} , endTime : {} , cout : {} ",startTime,endTime,value);

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("PST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("PST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }
}
