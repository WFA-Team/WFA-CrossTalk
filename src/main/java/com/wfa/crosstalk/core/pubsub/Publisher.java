package com.wfa.crosstalk.core.pubsub;

import com.wfa.middleware.utils.DataType;
import com.wfa.middleware.utils.Pair;

import java.util.Map;

public interface Publisher {
    @Deprecated
    void publish(String topic, Object message);

    void publish(Map<String, Pair<Object, DataType>> recordData);
}
