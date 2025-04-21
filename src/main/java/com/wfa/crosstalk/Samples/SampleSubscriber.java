package com.wfa.crosstalk.Samples;

import java.util.Map;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.wfa.crosstalk.core.pubsub.MessageHandler;
import com.wfa.crosstalk.core.pubsub.Subscriber;
import com.wfa.middleware.utils.DataType;
import com.wfa.middleware.utils.Pair;
import java.util.Properties;

@SpringBootApplication(scanBasePackages = {"com.wfa"})
public class SampleSubscriber {
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("com.wfa.crosstalk.pubsub.kafkaServer", "192.168.56.3:9092");
		properties.setProperty("com.wfa.crosstalk.pubsub.schemaRegistryUrl", "http://192.168.56.3:9094");
		properties.setProperty("com.wfa.crosstalk.pubsub.consumerGroup","SampleConsumerGroup");
		
		ConfigurableApplicationContext ctx = new SpringApplicationBuilder(SampleSubscriber.class).properties(properties).run(args);
		Subscriber sub = ctx.getBean(Subscriber.class);
		sub.subscribe("Sample", new MessageHandler() {

			@Override
			public void HandleMessage(Map<String, Pair<Object, DataType>> message) {
				return;
			}
			
		});
	}
}
