package com.coolhand.kafka.steam.domain;

import java.util.Set;

public record AlphabetWordAggregate(
        String key,
        Set<String > valueList,
        int runningCount
) {
}
