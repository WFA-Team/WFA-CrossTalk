package com.wfa.crosstalk.core.pubsub;

import java.util.List;

public interface TopicManager {
    void createTopic(String topicName, int partitions, short replicationFactor);
    List<String> listTopics();
}