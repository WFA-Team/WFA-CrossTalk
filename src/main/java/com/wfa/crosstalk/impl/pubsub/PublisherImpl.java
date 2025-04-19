package com.wfa.crosstalk.impl.pubsub;

import com.wfa.crosstalk.core.model.Constants;
import com.wfa.crosstalk.core.pubsub.Publisher;
import com.wfa.middleware.utils.DataType;
import com.wfa.middleware.utils.Pair;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;

import java.util.*;

@Service
public class PublisherImpl implements Publisher {

    private static final Logger logger = LoggerFactory.getLogger(PublisherImpl.class);

    private final KafkaProducer<String, GenericRecord> producer;

    private final CachedSchemaRegistryClient schemaRegistryClient;

    public PublisherImpl(@Value("${com.wfa.crosstalk.pubsub.kafkaServer}") String kafkaServer, @Value("$}") String schemaRegistryUrl) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", schemaRegistryUrl);

        logger.info("Creating Kafka producer with kafkaServer: {}", kafkaServer);
        this.producer = new KafkaProducer<>(properties);

        logger.info("Creating Cached Schema Registry Client with schemaRegistryUrl: {}", schemaRegistryUrl);
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }

    @Override
    public void publish(Map<String, Pair<Object, DataType>> recordData) {
        String topic = (String) recordData.get(Constants.TOPIC).e1;

        if (topic == null) {
            throw new IllegalArgumentException(Constants.TOPIC + " key must be present in the map");
        }
        recordData.remove(Constants.TOPIC);

        Schema schema = null;
        try {
            schema = getOrCreateSchema(topic, recordData);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        GenericRecord avroRecord = new GenericData.Record(schema);
        for (Map.Entry<String, Pair<Object, DataType>> entry : recordData.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue().e1;
            avroRecord.put(fieldName, value);
        }

        // Produce record
        // TODO: What should be the key of kafka record? For now, using UUID.
        ProducerRecord<String, GenericRecord> record =
                new ProducerRecord<>(topic, UUID.randomUUID().toString(), avroRecord);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending record: " + exception);
            }
        });
    }

    private Schema getOrCreateSchema(String topic, Map<String, Pair<Object, DataType>> recordData) throws Exception {
        try {
            List<Schema.Field> fields = new ArrayList<>();
            for (Map.Entry<String, Pair<Object, DataType>> entry : recordData.entrySet()) {
                Schema fieldSchema = getAvroField(entry.getValue().e2);

                // nullableSchema is required to make the updated schema backwards compatible if a new field is added.
                // Ensures old records have a null value for the new field.
                Schema nullableSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), fieldSchema));

                // Using Schema.Field.NULL_VALUE as using null was not allowing schema updates to be backwards compatible
                fields.add(new Schema.Field(entry.getKey(), nullableSchema, null, Schema.Field.NULL_VALUE));
            }

            Schema schema = Schema.createRecord(topic + "_value", null, null, false);
            schema.setFields(fields);

            AvroSchema avroSchema = new AvroSchema(schema);

            logger.info("Created AvroSchema: " + avroSchema.toString());

            if (schemaRegistryClient.getAllSubjects().contains(topic + "_value")) {
                logger.info("Found existing registered schema for topic: " + topic + "_value");
                boolean isCompatible = schemaRegistryClient.testCompatibility(topic + "_value", avroSchema);
                logger.info("Is current schema compatible with existing schema: " + isCompatible);
                if (!isCompatible) {
                    throw new RuntimeException("New schema is not compatible with existing schema for topic: " + topic);
                }
            }

            // Register schema
            try {
                schemaRegistryClient.register(topic + "_value", avroSchema);
            } catch (Exception e) {
                throw new RuntimeException("Failed to register schema", e);
            }

            return schema;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create or retrieve schema for topic: " + topic, e);
        }
    }

    private Schema getAvroField(DataType dataType) {
        return switch (dataType) {
            case STRING -> Schema.create(Schema.Type.STRING);
            case INTEGER -> Schema.create(Schema.Type.INT);
            case REAL -> Schema.create(Schema.Type.DOUBLE);
            case DATE ->
//                TODO: Check how to handle Date datatype in Avro
                    Schema.create(Schema.Type.INT);
            default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
        };
    }

    @Override
    public void publish(String topic, Object message) {
        /*  TODO: Fix the below code to publish topic and generic message
        try {
            String messageJson = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "placeholder_key", messageJson);
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
        */
    }

    @PreDestroy
    public void close() {
        logger.info("Closing Kafka producer");
        producer.close();
    }
}