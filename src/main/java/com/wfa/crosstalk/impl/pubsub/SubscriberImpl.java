package com.wfa.crosstalk.impl.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wfa.crosstalk.core.pubsub.Subscriber;
import com.wfa.crosstalk.util.MessageProcessor;
import com.wfa.crosstalk.util.Subscription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Service
public class SubscriberImpl implements Subscriber {

    private static final Logger log = LoggerFactory.getLogger(SubscriberImpl.class);

    private final KafkaConsumer<String, String> consumer;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<String, MessageProcessor> topicToProcessor = new ConcurrentHashMap<>();

    private volatile boolean running = true;
    private boolean started = false;

    public SubscriberImpl(@Value("${com.wfa.crosstalk.pubsub.kafkaServer}") String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        log.info("Creating Kafka consumer with bootstrapServers: {}, groupId: {}", bootstrapServers, "");
        this.consumer = new KafkaConsumer<>(properties);
        this.executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    public <T> void subscribe(String topic, Class<T> targetType, Consumer<T> messageHandler) {
        /*  TODO: Update the subscribe logic according to the updated Publisher
        MessageProcessor processor = new Subscription<>(targetType, messageHandler);
        topicToProcessor.put(topic, processor);
        if (!started) {
            started = true;
            executorService.submit(this::pollLoop);
        }
         */
    }

    private void pollLoop() {
        while (running) {
            try {
                consumer.subscribe(topicToProcessor.keySet());
                var records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    var processor = topicToProcessor.get(record.topic());
                    if (processor != null) {
                        processor.process(record.value());
                    }
                }
            } catch (WakeupException e) {
                if (!running) {
                    break;
                }
            } catch (Exception e) {
                log.error("Error in polling loop", e);
            }
        }
        consumer.close();
    }

    private void stop() {
        running = false;
        consumer.wakeup();
    }

    @PreDestroy
    public void destroy() {
        stop();
        executorService.shutdown();
    }
}