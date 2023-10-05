package com.coolhand.kafka.steam.domain;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
@Slf4j
public record AlphabetWordAggregate(
        String key,
        Set<String > valueList,
        int runningCount
) {
    public AlphabetWordAggregate() {
        this("",new HashSet<>(),0);
    }

    public AlphabetWordAggregate updateNewEvents(String key, String value) {
        log.info("New Record key : {}, Value", key,value);
        var newRunningCount = this.runningCount+1;
        valueList.add(value);
        var alphabetWordAggregate = new AlphabetWordAggregate(key, valueList, newRunningCount);
        log.info("Update Record key & Value : {}, Value",alphabetWordAggregate);
        return alphabetWordAggregate;
    }
}
