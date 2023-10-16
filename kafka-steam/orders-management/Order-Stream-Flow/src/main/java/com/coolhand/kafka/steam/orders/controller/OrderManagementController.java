package com.coolhand.kafka.steam.orders.controller;

import com.coolhand.kafka.steam.orders.service.OrderService;
import com.coolhand.kafka.stream.orders.domain.AllOrdersCountPerStoreDTO;
import com.coolhand.kafka.stream.orders.domain.OrderCountPerStoreDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.coolhand.kafka.steam.orders.topology.OrderManagementTopology.GENERAL_ORDER;

@RestController
@RequestMapping("/v1/orders")
@Slf4j
public class OrderManagementController {

    @Autowired
    private OrderService orderService;

    public OrderManagementController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> orderCount(
            @PathVariable("order_type") String orderType,
            @RequestParam(value="location_id",required = false) String locationId){

        if (StringUtils.hasLength(locationId)) {
            return ResponseEntity.ok(orderService.getOrderCountByLocationID(orderType,locationId));
        }

        return ResponseEntity.ok(orderService.getOrderCount(orderType));
    }

    @GetMapping("count")
    public List<AllOrdersCountPerStoreDTO> allOrderCount(){
        return orderService.getAllOrderCount();
    }

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> revenueByOrderType(
            @PathVariable("order_type") String orderType,
            @RequestParam(value = "location_id", required = false) String locationId
    ) {
        if (StringUtils.hasLength(locationId)) {
            return ResponseEntity.ok(orderService.getRevenueByLocationId(orderType, locationId));
        } else {

            return ResponseEntity.ok(orderService.getRevenueByOrderType(orderType));

        }
    }
}
