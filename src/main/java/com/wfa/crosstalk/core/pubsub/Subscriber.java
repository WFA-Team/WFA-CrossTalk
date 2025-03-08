package com.wfa.crosstalk.core.pubsub;

import java.util.function.Consumer;

public interface Subscriber {
    <T> void subscribe(String topic, Class<T> targetType, Consumer<T> messageHandler);
}
