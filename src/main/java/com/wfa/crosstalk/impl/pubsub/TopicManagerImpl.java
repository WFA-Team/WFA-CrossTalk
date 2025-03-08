package com.wfa.crosstalk.impl.pubsub;

import com.wfa.crosstalk.core.pubsub.TopicManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Service
public class TopicManagerImpl implements TopicManager {

    private static final Logger logger = LoggerFactory.getLogger(TopicManagerImpl.class);

    private final AdminClient adminClient;
    private final int adminTimeout;

    public TopicManagerImpl(@Value("${com.wfa.crosstalk.pubsub.bootstrapServers}") String bootstrapServers, @Value("${com.wfa.crosstalk.pubsub.adminTimeout:10}") int adminTimeout) {
        logger.info("Creating topic manager with bootstrapServers: {}", bootstrapServers);
        this.adminClient = createAdminClient(bootstrapServers);
        this.adminTimeout = adminTimeout;
    }

    private AdminClient createAdminClient(String bootstrapServers) {
        var properties = new java.util.Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        return AdminClient.create(properties);
    }

    @Override
    public void createTopic(String topicName, int partitions, short replicationFactor) {
        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
        try {
            adminClient.createTopics(List.of(newTopic)).all().get(adminTimeout, TimeUnit.SECONDS);
            logger.info("Topic {} created successfully.", topicName);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                logger.info("Topic {} already exists.", topicName);
            } else {
                logger.error("Error creating topic {}: {}", topicName, e.getMessage(), e);
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while creating topic {}: {}", topicName, e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.TimeoutException e) {
            logger.error("Timeout while creating topic {}: {}", topicName, e.getMessage(), e);
        }
    }

    @Override
    public List<String> listTopics() {
        try {
            ListTopicsResult topics = adminClient.listTopics();
            return new ArrayList<>(topics.names().get(adminTimeout, TimeUnit.SECONDS));
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            logger.error("Error listing topics: {}", e.getMessage(), e);
            return List.of();
        } catch (InterruptedException e) {
            logger.error("Interrupted while listing topics: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            return List.of();
        }
    }

    @PreDestroy
    public void close() {
        if (adminClient != null) {
            adminClient.close();
            logger.info("AdminClient closed.");
        }
    }
}