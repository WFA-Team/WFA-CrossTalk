package com.wfa.crosstalk.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class Subscription<T> implements MessageProcessor {
    private final Class<T> targetType;
    private final Consumer<T> messageHandler;
    private static final Logger log = LoggerFactory.getLogger(Subscription.class);

    public Subscription(Class<T> targetType, Consumer<T> messageHandler) {
        this.targetType = targetType;
        this.messageHandler = messageHandler;
    }

    @Override
    public void process(String message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            T obj = objectMapper.readValue(message, targetType);
            messageHandler.accept(obj);
        } catch (Exception e) {
            log.error("Error processing message", e);
        }
    }
}
