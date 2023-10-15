package com.coolhand.kafka.steam.orders.service;

import com.coolhand.kafka.stream.orders.domain.OrderCountPerStoreDTO;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.List;

import static com.coolhand.kafka.steam.orders.topology.OrderManagementTopology.*;

public class OrderService {

    private OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrderCountPerStoreDTO> getOrderCount(String orderType) {
        var orderCountStore= getOrderStore(orderType);

        return null;
    }

    private ReadOnlyKeyValueStore<String ,Long> getOrderStore(String orderType) {
        switch (orderType){
            case GENERAL_ORDER -> orderStoreService.orderCountStore(GENERAL_ORDER_COUNT);
            case RESTAURANT_ORDER -> orderStoreService.orderCountStore(RESTAURANT_ORDER_COUNT);
            default -> throw new IllegalStateException("Not a valid option");
        }
        return null;
    }
}
