package com.wfa.crosstalk.core.pubsub;

public interface Publisher {
    void publish(String topic, Object message);
}
