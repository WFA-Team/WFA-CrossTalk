package com.wfa.crosstalk.ksql.api;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.StreamedQueryResult;

public interface QueryManager {
	public CompletableFuture<ExecuteStatementResult> createStream(String streamName, String topic, Set<String> FieldNames);
	BatchedQueryResult executeRawQuery(String rawQuery);
	CompletableFuture<StreamedQueryResult> executeRawQueryRT(String rawQuery);
}
