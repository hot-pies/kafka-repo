package com.coolhand.kafka.stream.orders.domain;

public record AllOrdersCountPerStoreDTO(String locationId,
                                        Long orderCount,
                                        OrderType orderType) {
}
