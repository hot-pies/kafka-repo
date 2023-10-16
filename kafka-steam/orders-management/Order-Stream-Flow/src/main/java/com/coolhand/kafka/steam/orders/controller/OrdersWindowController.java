package com.coolhand.kafka.steam.orders.controller;

import com.coolhand.kafka.steam.orders.service.OrdersWindowService;
import com.coolhand.kafka.stream.orders.domain.AllOrdersCountPerStoreDTO;
import com.coolhand.kafka.stream.orders.domain.OrdersCountPerStoreByWindowsDTO;
import com.coolhand.kafka.stream.orders.domain.OrdersRevenuePerStoreByWindowsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
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

    @GetMapping("/windows/count")
    public List<OrdersCountPerStoreByWindowsDTO> allOrderCount(
        @RequestParam(value="from_time",required =false)
                @DateTimeFormat(iso=DateTimeFormat.ISO.DATE_TIME)
        LocalDateTime fromTime,
        @RequestParam(value = "toT_time",required = false)
        @DateTimeFormat(iso=DateTimeFormat.ISO.DATE_TIME)
                LocalDateTime toTime
    ){
        if(fromTime !=null && toTime !=null){
            ordersWindowService.getAllOrderCountByWindows(fromTime,toTime);
        }
        return ordersWindowService.getAllOrderCountByWindows();
    }

    @GetMapping("/windows/revenue/{order_type}")
    public List<OrdersRevenuePerStoreByWindowsDTO> orderReenue(
            @PathVariable("order_type") String orderType
    ){

        return ordersWindowService.getOrderRevenueWindowsByType(orderType);
    }
}
