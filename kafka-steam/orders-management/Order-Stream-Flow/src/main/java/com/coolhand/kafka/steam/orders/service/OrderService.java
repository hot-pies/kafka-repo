package com.coolhand.kafka.steam.orders.service;

import com.coolhand.kafka.stream.orders.domain.OrderCountPerStoreDTO;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.coolhand.kafka.steam.orders.topology.OrderManagementTopology.*;
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
}
