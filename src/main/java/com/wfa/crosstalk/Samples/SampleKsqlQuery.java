package com.wfa.crosstalk.Samples;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import com.wfa.crosstalk.ksql.api.QueryManager;
import io.confluent.ksql.api.client.Row;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

@SpringBootApplication(scanBasePackages = {"com.wfa"})
public class SampleKsqlQuery {
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("com.wfa.crosstalk.pubsub.ksqlServer", "192.168.56.3:9095");
		properties.setProperty("com.wfa.crosstalk.pubsub.kafkaServer", "192.168.56.3:9092");
		properties.setProperty("com.wfa.crosstalk.pubsub.schemaRegistryUrl", "http://192.168.56.3:9094");
		properties.setProperty("com.wfa.crosstalk.pubsub.consumerGroup","SampleConsumerGroup");
		
		ConfigurableApplicationContext ctx = new SpringApplicationBuilder(SampleKsqlQuery.class).properties(properties).run(args);
		QueryManager queryManager = ctx.getBean(QueryManager.class);
		queryManager.createStream("SampleStream", "Sample", new HashSet<String>(Arrays.asList("key1", "key2", "key3")))
			.thenAccept(result-> {queryManager.executeRawQueryRT("SELECT * FROM SampleStream EMIT CHANGES;").thenAccept(streamedQueryResult -> {
	            // Subscribe to the results
	            streamedQueryResult.subscribe(new Subscriber<Row>() {
	                private Subscription subscription;

	                @Override
	                public void onSubscribe(Subscription s) {
	                    this.subscription = s;
	                    s.request(Long.MAX_VALUE); // Request all data (or use backpressure if needed)
	                }

	                @Override
	                public void onNext(Row row) {
	                    System.out.println("Received row: " + row.values());
	                }

	                @Override
	                public void onError(Throwable t) {
	                    t.printStackTrace();
	                }

	                @Override
	                public void onComplete() {
	                    System.out.println("Stream completed");
	                }
	            });
	        })
	        .exceptionally(e -> {
	            // Handle exceptions
	            e.printStackTrace();
	            return null;
	        });}).exceptionally(e -> {
	            // Handle exceptions
	            e.printStackTrace();
	            return null;});
		
	}
}
