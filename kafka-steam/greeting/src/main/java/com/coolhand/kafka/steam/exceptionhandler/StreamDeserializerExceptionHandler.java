package com.coolhand.kafka.steam.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

@Slf4j
public class StreamDeserializerExceptionHandler implements DeserializationExceptionHandler {

    int errorCount=0;
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Exception : {} , Kafka streaming records is {}  ",exception.getMessage(),record,exception);
        log.info("errorCount : ", errorCount);
        if(errorCount<2){
            errorCount++;
            return DeserializationHandlerResponse.CONTINUE;
        }
        return null;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
