package com.wfa.crosstalk.core.pubsub;

import java.util.Map;

import com.wfa.middleware.utils.DataType;
import com.wfa.middleware.utils.Pair;

public interface MessageHandler {
	void HandleMessage(Map<String, Pair<Object, DataType>> message);
}
