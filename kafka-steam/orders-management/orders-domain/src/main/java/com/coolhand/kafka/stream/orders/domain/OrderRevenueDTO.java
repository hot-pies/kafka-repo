package com.coolhand.kafka.stream.orders.domain;

public record OrderRevenueDTO(String locationId,
                              OrderType orderType,
                              TotalRevenue totalRevenue) {
}
