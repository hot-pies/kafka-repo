package com.coolhand.kafka.stream.orders.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record Orders(Integer orderId,
                     String locationId,
                     BigDecimal finalAmount,
                     OrderType orderType,
                     List<OrderLineItem> orderLineItems,
                     LocalDateTime orderedDateTime) {
}
