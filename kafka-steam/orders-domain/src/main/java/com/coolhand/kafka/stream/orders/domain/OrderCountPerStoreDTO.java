package com.coolhand.kafka.stream.orders.domain;

public record OrderCountPerStoreDTO(String locationId,
                                    Long orderCount) {
}
