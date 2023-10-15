package com.coolhand.kafka.steam.orders.controller;

import com.coolhand.kafka.steam.orders.service.OrderService;
import com.coolhand.kafka.stream.orders.domain.OrderCountPerStoreDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static com.coolhand.kafka.steam.orders.topology.OrderManagementTopology.GENERAL_ORDER;

@RestController
@RequestMapping("/v1/orders")
@Slf4j
public class OrderManagementController {

    @Autowired
    private OrderService orderService;
    @GetMapping("cout/{order_type}")
    public List<OrderCountPerStoreDTO> orderCount(@PathVariable("order_type") String orderType){

        orderService.getOrderCount(orderType);
        return null;
    }



}
