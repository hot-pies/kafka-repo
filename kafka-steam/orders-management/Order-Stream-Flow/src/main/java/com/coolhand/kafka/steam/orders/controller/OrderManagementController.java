package com.coolhand.kafka.steam.orders.controller;

import com.coolhand.kafka.steam.orders.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/orders")
@Slf4j
public class OrderManagementController {

    private OrderService orderService;

    public OrderManagementController(OrderService orderService) {
        this.orderService = orderService;
    }

    public void getOrder(){

    }
}
