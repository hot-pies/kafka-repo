package com.coolhand.kafka.steam.orders.service;

import com.coolhand.kafka.stream.orders.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.coolhand.kafka.steam.orders.topology.OrderManagementTopology.*;
@Slf4j
@Service
public class OrderService {

    private OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrderCountPerStoreDTO> getOrderCount(String orderType) {
        var orderCountStore= getOrderStore(orderType);
        var orders = orderCountStore.all();

        var spliterator =Spliterators.spliteratorUnknownSize(orders,0);

        return StreamSupport.stream(spliterator,false)
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

    }

    private ReadOnlyKeyValueStore<String ,Long> getOrderStore(String orderType) {
       return switch (orderType){
            case GENERAL_ORDER -> orderStoreService.orderCountStore(GENERAL_ORDER_COUNT);
            case RESTAURANT_ORDER -> orderStoreService.orderCountStore(RESTAURANT_ORDER_COUNT);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    public OrderCountPerStoreDTO getOrderCountByLocationID(String orderType, String locationId) {

        var orderCountStore= getOrderStore(orderType);
        var ordersCount = orderCountStore.get(orderType);

        if(ordersCount!=null){
            return new OrderCountPerStoreDTO(locationId,ordersCount);
        }
        return null;
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrderCount() {

        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO>
                mapper =(orderCountPerStoreDTO,orderType) ->new AllOrdersCountPerStoreDTO(
                orderCountPerStoreDTO.locationId(),orderCountPerStoreDTO.orderCount(),orderType);

        var generalOrderCount= getOrderCount(GENERAL_ORDER)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO,OrderType.GENERAL))
                .toList();

        var restaurantOrderCount= getOrderCount(RESTAURANT_ORDER)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO,OrderType.RESTAURANT))
                .toList();

        return Stream.of(generalOrderCount,restaurantOrderCount)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    public List<OrderRevenueDTO> getRevenueByOrderType(String orderType) {

        var revenueStoreByType =getRevenueStore(orderType);

        var revenueIterator = revenueStoreByType.all();
        var spliterator = Spliterators.spliteratorUnknownSize(revenueIterator, 0);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue ->{
                    log.info("cooland keyValue.value : ",keyValue.value);
                    var value=keyValue.value;
                    log.info("cooland locationId : ",value.locationId());
                    log.info("cooland updateRunningRevenue : ",value.runnuingOrderCount());
                    log.info("cooland runningRevenue : ",value.runningRevenue());
                    return new OrderRevenueDTO(keyValue.key, mapOrderType(orderType), value);
                })

                .collect(Collectors.toList());

    }

    public static OrderType mapOrderType(String orderType) {
        return switch (orderType){
            case GENERAL_ORDER -> OrderType.GENERAL;
            case RESTAURANT_ORDER -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    private ReadOnlyKeyValueStore<String , TotalRevenue> getRevenueStore(String orderType) {
        log.info("CoolHand getRevenueStore : ", orderType.toString());
        return switch (orderType){
            case GENERAL_ORDER -> orderStoreService.orderRevenueStore(GENERAL_ORDER_REVENUE);
            case RESTAURANT_ORDER -> orderStoreService.orderRevenueStore(RESTAURANT_ORDER_COUNT);
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    public OrderRevenueDTO getRevenueByLocationId(String orderType, String locationId) {
        var revenueStoreByType =getRevenueStore(orderType);

        var totalRevenue = revenueStoreByType.get(locationId);
        if (totalRevenue != null) {
            return new OrderRevenueDTO(locationId,mapOrderType(orderType), totalRevenue);
        } else {
            return null;
        }
    }
}
