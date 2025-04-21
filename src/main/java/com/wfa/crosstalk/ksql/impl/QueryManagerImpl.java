package com.wfa.crosstalk.ksql.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import com.wfa.crosstalk.core.model.HelperMethods;
import com.wfa.crosstalk.ksql.api.QueryManager;

import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

@Component
public class QueryManagerImpl implements QueryManager {
	
	private final String ksqlServer;
	private final CachedSchemaRegistryClient schemaRegistryClient;
	private final Client sqlClient;
	private static final String COLON = ":";
	
	@Autowired
	public QueryManagerImpl(@Value("${com.wfa.crosstalk.pubsub.ksqlServer: not_set}") String ksqlServer,
			@Value("${com.wfa.crosstalk.pubsub.schemaRegistryUrl}")String schemaRegistryUrl) {
		if (!ksqlServer.equals("not_set")) {
			this.ksqlServer = ksqlServer;
			this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
			ClientOptions options = ClientOptions.create().setHost(this.ksqlServer.substring(0, this.ksqlServer.indexOf(COLON)));
			options.setPort(Integer.parseInt(this.ksqlServer.substring(this.ksqlServer.indexOf(COLON) + 1).strip()));
			this.sqlClient = Client.create(options);
		} else {
			this.ksqlServer = "";
			this.schemaRegistryClient = null;
			this.sqlClient = null;
		}
	}
	
	@Override
	public CompletableFuture<ExecuteStatementResult> createStream(String streamName, String topic, Set<String> FieldNames) {
		if (StringUtils.isBlank(this.ksqlServer))
			throw new IllegalStateException("Bean is not configured properly");
		
		try {
			String query = "CREATE STREAM IF NOT EXISTS " + streamName + " ( ";
			String serializedSchema = this.schemaRegistryClient.getLatestSchemaMetadata(HelperMethods.getSchemaName(topic)).
					getSchema();
			Schema schema = new Schema.Parser().parse(serializedSchema);
			Set<String> schemaFields = new HashSet<String>(schema.getFields().stream().map(Field::name).toList());
			Set<String> invalidFieldNames = new HashSet<String>(FieldNames); 
			invalidFieldNames.removeAll(schemaFields);
			
			if (invalidFieldNames.size() != 0)
				throw new UnsupportedOperationException("Invalid Fields Requested " + invalidFieldNames.toString());
			
			List<Field> requestedFields = schema.getFields().stream().filter(field -> FieldNames.contains(field.name())).toList();
			
			for (int i = 0; i < requestedFields.size() - 1 ; i++) {
				query = query + requestedFields.get(i).name() + " " + HelperMethods.ApacheDataTypeToKsqlDataType(schema) + ", ";
			}
			
			query = query + requestedFields.get(requestedFields.size() - 1).name() + " " + HelperMethods.ApacheDataTypeToKsqlDataType(schema) + " ";
			query = query + ") WITH ( kafka_topic=" + "'" + topic + "'"+", value_format='AVRO' );";
			
			return sqlClient.executeStatement(query);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	@Override
	public BatchedQueryResult executeRawQuery(String rawQuery) {
		Map<String, Object> props = new HashMap<>();
		props.put("auto.offset.reset", "earliest");
		return sqlClient.executeQuery(rawQuery, props);
	}
	
	@Override
	public CompletableFuture<StreamedQueryResult> executeRawQueryRT(String rawQuery) {
		Map<String, Object> props = new HashMap<>();
		props.put("auto.offset.reset", "earliest");
		return sqlClient.streamQuery(rawQuery, props);
	}
}
