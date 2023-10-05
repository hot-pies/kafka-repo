package com.coolhand.kafka.steam.domain;

import java.math.BigDecimal;

public record OrderLineItem(String item,
                            Integer count,
                            BigDecimal amount) {
}
