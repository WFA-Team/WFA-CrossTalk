package com.wfa.crosstalk.impl.pubsub;

import com.wfa.crosstalk.core.model.HelperMethods;
import com.wfa.crosstalk.core.pubsub.MessageHandler;
import com.wfa.crosstalk.core.pubsub.Subscriber;
import com.wfa.middleware.taskexecutor.api.ATaskElement;
import com.wfa.middleware.taskexecutor.api.ITaskElement;
import com.wfa.middleware.taskexecutor.api.ITaskExecutorEngine;
import com.wfa.middleware.utils.DataType;
import com.wfa.middleware.utils.JoinVoid;
import com.wfa.middleware.utils.Pair;
import com.wfa.middleware.utils.PlayType;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.avro.generic.GenericRecord;

@Service
public class SubscriberImpl implements Subscriber {

    private static final Logger log = LoggerFactory.getLogger(SubscriberImpl.class);

    private final KafkaConsumer<String, GenericRecord> consumer;
    private final ITaskExecutorEngine taskEngine;
    //private final ConcurrentHashMap<String, MessageProcessor> topicToProcessor = new ConcurrentHashMap<>();
    private Map<String, List<MessageHandler>> subscriptions = new ConcurrentHashMap<>();

    private volatile boolean running = true;
    private volatile boolean started = false;
    private final CachedSchemaRegistryClient schemaRegistryClient;

    @Autowired
    public SubscriberImpl(@Value("${com.wfa.crosstalk.pubsub.kafkaServer}") String bootstrapServers, 
    		@Value("${com.wfa.crosstalk.pubsub.schemaRegistryUrl}") String schemaRegistryUrl,
    		@Value("${com.wfa.crosstalk.pubsub.consumerGroup: not_set}") String consumerGroup,
    		ITaskExecutorEngine taskEngine) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        if(consumerGroup != "not_set")
        	properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("schema.registry.url", schemaRegistryUrl);

        log.info("Creating Kafka consumer with bootstrapServers: {}, groupId: {}", bootstrapServers, "");
        this.consumer = new KafkaConsumer<>(properties);
        this.taskEngine = taskEngine;
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }
    
	@Override
	public void subscribe(String topic, MessageHandler handler) {
		if (!subscriptions.containsKey(topic))
			subscriptions.put(topic, new CopyOnWriteArrayList<MessageHandler>());
		
		subscriptions.get(topic).add(handler);
		if (!started) {
			started = true;
			consumer.subscribe(Arrays.asList(topic));
			taskEngine.schedule(getPollingTask());
			if (taskEngine.getState() != PlayType.STARTED)
				taskEngine.startEngine();
		}
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
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                	for (MessageHandler handler : subscriptions.get(record.topic())) {
                		Map<String, Pair<Object, DataType>> message = getMessageFromRecord(record);
                		handler.HandleMessage(message);
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
    
    private ITaskElement<JoinVoid> getPollingTask() {
    	return new ATaskElement<JoinVoid>(taskEngine) {
			@Override
			public void preexecute() { /* do Nothing */ }
			
			@Override
			public void execute() {
				pollLoop();
				setResult(JoinVoid.JoinVoidInstance);
				succeed = true;
			}    		
    	};
    }
    
    private Map<String, Pair<Object, DataType>> getMessageFromRecord(ConsumerRecord<String, GenericRecord> record) {
    	Map<String, Pair<Object, DataType>> message = new HashMap<>();
    	GenericRecord avroRecord = record.value();
		try {
			String serializedSchema = this.schemaRegistryClient.getLatestSchemaMetadata(HelperMethods.getSchemaName(record.topic())).
					getSchema();
	    	Schema.Parser parser = new Schema.Parser();
	    	Schema schema = parser.parse(serializedSchema);
	    	for(Field field: schema.getFields()) {
	    		String fieldName = field.name();
	    		Object value = avroRecord.get(fieldName);
	    		DataType type = ApacheToApplicationDataType(field.schema());
	    		message.put(fieldName, new Pair<Object, DataType>(value, type));
	    	}
	    	
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}
		

    	return message;
    }
    
    private DataType ApacheToApplicationDataType(Schema apacheField) {
    	Type apacheFieldType = apacheField.getType();
    	switch (apacheFieldType) {
    	case Type.UNION:
    		List<Schema> underlyingSchemas = apacheField.getTypes();
    		for (Schema schema: underlyingSchemas) {
    			if (!schema.getType().equals(Type.NULL))
    				return ApacheToApplicationDataType(schema);
    		}
    	case Type.INT:
    		return DataType.INTEGER;
    	case Type.DOUBLE:
    		return DataType.REAL;
    	default:
    		return DataType.STRING;
    	}
    }

    private void stop() {
        running = false;
        consumer.wakeup();
    }

    @PreDestroy
    public void destroy() {
        stop();
    }
}