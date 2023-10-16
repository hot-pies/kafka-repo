package com.coolhand.kafka.steam.orders.controller;

import com.coolhand.kafka.steam.orders.service.OrdersWindowService;
import com.coolhand.kafka.stream.orders.domain.OrdersCountPerStoreByWindowsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/orders")
public class OrdersWindowController {

    @Autowired
    private final OrdersWindowService ordersWindowService;

    public OrdersWindowController(OrdersWindowService ordersWindowService) {
        this.ordersWindowService = ordersWindowService;
    }

    @GetMapping("/windows/count/{order_type}")
    public List<OrdersCountPerStoreByWindowsDTO> orderCount(
            @PathVariable("order_type") String orderType
    ){

        return ordersWindowService.getOrderCountWindowsByTime(orderType);
    }
}
