package com.wfa.crosstalk.Samples;

import java.util.Properties;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(scanBasePackages = {"com.wfa"})
public class SamplePublisher {
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("com.wfa.crosstalk.pubsub.kafkaServer", "192.168.56.3:9092");
		properties.setProperty("com.wfa.crosstalk.pubsub.schemaRegistryUrl", "http://192.168.56.3:9094");
		
		ConfigurableApplicationContext ctx = new SpringApplicationBuilder(SamplePublisher.class).properties(properties).run(args);
	}
}
