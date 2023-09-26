package com.coolhand.kafka.steam.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;


    public record Order(Integer orderId,
                        String locationId,
                        BigDecimal finalAmount,
                        OrderType orderType,
                        List<OrderLineItem> orderLineItemList,
                        LocalDateTime localDateTime
                        ){}

