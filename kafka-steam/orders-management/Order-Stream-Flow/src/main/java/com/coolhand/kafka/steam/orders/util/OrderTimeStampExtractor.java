package com.coolhand.kafka.steam.orders.util;

import com.coolhand.kafka.stream.orders.domain.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class OrderTimeStampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {

        var order = (Order) record.value();

        if (order!=null && order.orderedDateTime()!=null){
            var timeStamp =order.orderedDateTime();
            log.info("timeStamp in extractor {} ", timeStamp);

            return convertToInstantTimeStampPST(timeStamp);
        }
        return partitionTime;
    }

    //Hello we are getting
    private long convertToInstantTimeStampPST(LocalDateTime timeStamp) {
        return timeStamp.toInstant(ZoneOffset.ofHours(-7)).toEpochMilli();
    }

    private long convertToInstantTimeStampUTC(LocalDateTime timeStamp) {
        return timeStamp.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
