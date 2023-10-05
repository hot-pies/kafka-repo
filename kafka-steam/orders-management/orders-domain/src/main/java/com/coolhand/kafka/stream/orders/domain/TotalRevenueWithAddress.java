package com.coolhand.kafka.stream.orders.domain;

public record TotalRevenueWithAddress(TotalRevenue totalRevenue,
                                      Store store) {
}
