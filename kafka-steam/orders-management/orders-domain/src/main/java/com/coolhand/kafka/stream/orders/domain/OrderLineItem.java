package com.coolhand.kafka.stream.orders.domain;

import java.math.BigDecimal;

public record OrderLineItem(String item,
                            Integer count,
                            BigDecimal amount) {
}
