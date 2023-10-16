package com.coolhand.kafka.steam.orders.service;

import com.coolhand.kafka.stream.orders.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.coolhand.kafka.steam.orders.service.OrderService.mapOrderType;
import static com.coolhand.kafka.steam.orders.topology.OrderManagementTopology.*;
import static com.coolhand.kafka.steam.orders.topology.OrderManagementTopology.RESTAURANT_ORDER_COUNT;

@Service
@Slf4j
public class OrdersWindowService {

    private OrderStoreService orderStoreService;

    public OrdersWindowService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrderCountWindowsByTime(String orderType) {

        var countWindowStore=getCountWindowStore(orderType);
        var orderTypeEnum=mapOrderType(orderType);
        var countWindowIterator=countWindowStore.all();

        return mapToOrderCountPerStoreByWindowsDTO(orderTypeEnum, countWindowIterator);
    }

    private static List<OrdersCountPerStoreByWindowsDTO> mapToOrderCountPerStoreByWindowsDTO(OrderType orderTypeEnum, KeyValueIterator<Windowed<String>, Long> countWindowIterator) {
        var spliterator = Spliterators.spliteratorUnknownSize(countWindowIterator,0);

        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrdersCountPerStoreByWindowsDTO(
                                keyValue.key.key(),
                                keyValue.value,
                                orderTypeEnum,
                                LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                                LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                        )
                )
                .collect(Collectors.toList());
    }

    private ReadOnlyWindowStore<String,Long> getCountWindowStore(String orderType){
        return switch (orderType){
            case GENERAL_ORDER -> orderStoreService.orderWindowsCountStore(GENERAL_ORDER_COUNT_WINDOWS);
            case RESTAURANT_ORDER -> orderStoreService.orderWindowsCountStore(RESTAURANT_ORDER_COUNT_WINDOWS);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrderCountByWindows() {

        var generalOrderCountByWindows=getOrderCountWindowsByTime(GENERAL_ORDER);
        var restaurantOrderCountByWindows=getOrderCountWindowsByTime(RESTAURANT_ORDER);

        return Stream.of(generalOrderCountByWindows,restaurantOrderCountByWindows)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrderCountByWindows(LocalDateTime fromTime, LocalDateTime toTime) {
        var fromTimeInstant=fromTime.toInstant(ZoneOffset.UTC);
        var toTimeInstant = toTime.toInstant(ZoneOffset.UTC);
        var generalOrderCountByWindows= getCountWindowStore(GENERAL_ORDER)
                .fetchAll(fromTimeInstant,toTimeInstant);

        var restaurantOrderCountByWindows= getCountWindowStore(RESTAURANT_ORDER)
                .fetchAll(fromTimeInstant,toTimeInstant);

        var generalOrdersCountPerStoreByWindowsDTO=mapToOrderCountPerStoreByWindowsDTO(OrderType.GENERAL,generalOrderCountByWindows);

        var restaurantOrdersCountPerStoreByWindowsDTO=mapToOrderCountPerStoreByWindowsDTO(OrderType.RESTAURANT,restaurantOrderCountByWindows);

        return Stream.of(generalOrdersCountPerStoreByWindowsDTO,restaurantOrdersCountPerStoreByWindowsDTO)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    public List<OrdersRevenuePerStoreByWindowsDTO> getOrderRevenueWindowsByType(String orderType) {
        var revenueWindowStore=getRevenueWindowStore(orderType);
        var orderTypeEnum=mapOrderType(orderType);
        var revenueWindowIterator=revenueWindowStore.all();

        var spliterator = Spliterators.spliteratorUnknownSize(revenueWindowIterator,0);

        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrdersRevenuePerStoreByWindowsDTO(
                                keyValue.key.key(),
                                keyValue.value,
                                orderTypeEnum,
                                LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                                LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                        )
                )
                .collect(Collectors.toList());


    }

    private ReadOnlyWindowStore<String, TotalRevenue> getRevenueWindowStore(String orderType){
        return switch (orderType){
            case GENERAL_ORDER -> orderStoreService.orderWindowsRenenueStore(GENERAL_ORDER_REVENUE_WINDOWS);
            case RESTAURANT_ORDER -> orderStoreService.orderWindowsRenenueStore(RESTAURANT_ORDER_REVENUE_WINDOWS);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }
}
