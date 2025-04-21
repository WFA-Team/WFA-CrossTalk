package com.wfa.crosstalk.core.pubsub;

import java.util.function.Consumer;

public interface Subscriber {
    @Deprecated
    <T> void subscribe(String topic, Class<T> targetType, Consumer<T> messageHandler);
    
    void subscribe(String topic, MessageHandler handler);
}
