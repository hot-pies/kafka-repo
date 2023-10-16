package com.coolhand.kafka.steam.orders.service;

import com.coolhand.kafka.stream.orders.domain.OrderCountPerStoreDTO;
import com.coolhand.kafka.stream.orders.domain.OrdersCountPerStoreByWindowsDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
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
        var spliterator = Spliterators.spliteratorUnknownSize(countWindowIterator,0);

        return StreamSupport.stream(spliterator,false)
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
}
