package com.coolhand.kafka.stream.orders.domain;

public record Store(String locationId,
                    Address address,
                    String contactNum) {
}
