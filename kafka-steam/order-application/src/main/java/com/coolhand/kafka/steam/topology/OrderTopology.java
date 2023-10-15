package com.coolhand.kafka.steam.topology;

import com.coolhand.kafka.steam.domain.*;
import com.coolhand.kafka.steam.serdes.SerdesFactory;
import com.coolhand.kafka.steam.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
public class OrderTopology {

    public static final String ORDERS="orders";
    public static final String GENERAL_ORDER="general_orders";
    public static final String GENERAL_ORDER_COUNT="general_orders_count";
    public static final String STORE="stores";
    public static final String GENERAL_ORDER_COUNT_WINDOWS="general_orders_count_window";
    public static final String GENERAL_ORDER_REVENUE="general_orders_revenue";

    public static final String GENERAL_ORDER_REVENUE_WINDOWS="general_orders_revenue_window";

    public static final String RESTAURANT_ORDER="restaurant_orders";
    public static final String RESTAURANT_ORDER_COUNT="restaurant_orders_count";
    public static final String RESTAURANT_ORDER_REVENUE="restaurant_orders_revenue";
    public static final String RESTAURANT_ORDER_COUNT_WINDOWS="restaurant_orders_count_window";
    public static final String RESTAURANT_ORDER_REVENUE_WINDOWS="restaurant_orders_revenue_window";

    public static Topology build(){

        Predicate<String,Order> generalPredicate  = (key,order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String,Order> resturantPredicate = (key,order)-> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(),order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var ordersStream = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerdes())
                        .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key,value)->value.locationId());

//        KSTREAM - KTABLE
        var storeTable=streamsBuilder
                .table(STORE,
                Consumed.with(Serdes.String(),SerdesFactory.storeSerdes())
                );

        ordersStream
                .print(Printed.<String , Order>toSysOut().withLabel("orders :"));

        ordersStream
                .split(Named.as("General-Restaurant-Stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrderStream->{
                            generalOrderStream
                                    .print(Printed.<String,Order>toSysOut().withLabel("generalStream : "));

//                            generalOrderStream
//                                    .mapValues((ReadOnlyKey,value)-> revenueValueMapper.apply(value))
//                                    .to(GENERAL_ORDER,
//                                            Produced.with(Serdes.String(),SerdesFactory.revenueSerdes()));
//                            aggregateOrderByCount(generalOrderStream,GENERAL_ORDER_COUNT);
//                            aggregateOrderByRevenue(generalOrderStream,GENERAL_ORDER_REVENUE,storeTable);
//                            aggregateOrderCountByTimeWindows(generalOrderStream,GENERAL_ORDER_COUNT_WINDOWS,storeTable);
                            aggregateOrderByRevenueWindow(generalOrderStream,GENERAL_ORDER_REVENUE_WINDOWS,storeTable);




                })
                ).branch(resturantPredicate,
                        Branched.withConsumer(restaurantOrderStream->{
                            restaurantOrderStream
                                    .print(Printed.<String,Order>toSysOut().withLabel("restaurantStream : "));

//                            restaurantOrderStream
//                                    .mapValues((key,value)-> revenueValueMapper.apply(value))
//                                    .to(RESTAURANT_ORDER,Produced.with(Serdes.String(),SerdesFactory.revenueSerdes()));
//                            aggregateOrderByCount(restaurantOrderStream,RESTAURANT_ORDER_COUNT);
//                            aggregateOrderByRevenue(restaurantOrderStream,RESTAURANT_ORDER_REVENUE,storeTable);
//                            aggregateOrderCountByTimeWindows(restaurantOrderStream,RESTAURANT_ORDER_COUNT_WINDOWS,storeTable);
                            aggregateOrderByRevenueWindow(restaurantOrderStream,RESTAURANT_ORDER_REVENUE_WINDOWS,storeTable);

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
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindows)
                .aggregate(
                        totalRevenueInitializer,
                        aggregator,
                        Materialized.<String,TotalRevenue, WindowStore<Bytes,byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())
                );

//       KTABLE - KTABLE Join
        ValueJoiner<TotalRevenue,Store, TotalRevenueWithAddress> valueJoiner=TotalRevenueWithAddress::new;
        var joinParam =Joined.with(Serdes.String(),SerdesFactory.totalRevenueSerdes(),SerdesFactory.storeSerdes());

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

    private static void aggregateOrderCountByTimeWindows
            (KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storeTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        var ordersCountPerStore = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(),value))
                .groupByKey(Grouped.with(Serdes.String(),SerdesFactory.orderSerdes()))
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

    private static void aggregateOrderByRevenue(KStream<String, Order> orderStream, String storeName,KTable<String,Store> storeTable) {
        Initializer<TotalRevenue> orderInitializer = TotalRevenue::new;
        Aggregator<String,Order,TotalRevenue> aggregator
                = (key,value,aggregate) -> aggregate.updateRunningRevenue(key,value);

       var revenueTable= orderStream
                .map((key,value)->KeyValue.pair(value.locationId(),value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .aggregate(
                        orderInitializer,
                        aggregator,
                        Materialized.<String,TotalRevenue, KeyValueStore<Bytes,byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())
                );

//       KTABLE - KTABLE Join
        ValueJoiner<TotalRevenue,Store, TotalRevenueWithAddress> valueJoiner=TotalRevenueWithAddress::new;

        var totalReviewWIthStore=revenueTable
                .join(storeTable,valueJoiner);

        totalReviewWIthStore
                .toStream()
                        .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(storeName+"ByStoreName"));


        revenueTable
                .toStream()
                .print(Printed.<String, TotalRevenue>toSysOut().withLabel(storeName));

    }

    private static void aggregateOrderByCount(KStream<String, Order> generalOrderStream, String storeName) {
        var ordersCountPerStore = generalOrderStream

//                .map((key, value) -> KeyValue.pair(value.locationId(),value))
                .groupByKey(Grouped.with(Serdes.String(),SerdesFactory.orderSerdes()))
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
