package com.wfa.crosstalk.impl.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wfa.crosstalk.core.pubsub.Publisher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.Properties;

@Service
public class PublisherImpl implements Publisher {

    private static final Logger logger = LoggerFactory.getLogger(PublisherImpl.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public PublisherImpl(@Value("${com.wfa.crosstalk.pubsub.bootstrapServers}") String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        logger.info("Creating Kafka producer with bootstrapServers: {}", bootstrapServers);
        this.producer = new KafkaProducer<>(properties);    // TODO: Investigate and fix
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void publish(String topic, Object message) {
        try {
            String messageJson = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageJson);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error while publishing message to Kafka: {}", exception.getMessage(), exception);
                } else {
                    logger.debug("Message successfully sent to topic {} with offset {}", topic, metadata.offset());
                }
            });
        } catch (Exception e) {
            logger.error("Error serializing message: {}", e.getMessage(), e);
        }
    }

    @PreDestroy
    public void close() {
        logger.info("Closing Kafka producer");
        producer.close();
    }
}