/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch.rest;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Elasticsearch 6.x sink that requests multiple {@link DocWriteRequest DocWriteRequests}
 * against a cluster for each incoming element.
 *
 * <p>The sink internally uses a {@link RestHighLevelClient} to communicate with an Elasticsearch cluster.
 * The sink will fail if no cluster can be connected to using the provided transport addresses passed to the constructor.
 *
 * <p>The {@link Map} passed to the constructor is used to create the {@code TransportClient}. The config keys can be found
 * in the <a href="https://www.elastic.io">Elasticsearch documentation</a>. An important setting is {@code cluster.name},
 * which should be set to the name of the cluster that the sink should emit to.
 *
 * <p>Internally, the sink will use a {@link BulkProcessor} to send {@link DocWriteRequest DocWriteRequests}.
 * This will buffer elements before sending a request to the cluster. The behaviour of the
 * {@code BulkProcessor} can be configured using these config keys:
 * <ul>
 *   <li> {@code bulk.flush.max.actions}: Maximum amount of elements to buffer
 *   <li> {@code bulk.flush.max.size.mb}: Maximum amount of data (in megabytes) to buffer
 *   <li> {@code bulk.flush.interval.ms}: Interval at which to flush data regardless of the other two
 *   settings in milliseconds
 * </ul>
 *
 * <p>Note that the Elasticsearch 5.x and later versions convert {@link DocWriteRequest DocWriteRequest} to
 * {@link DocWriteRequest DocWriteRequest} in {@link BulkProcessorIndexer}.
 *
 * <p>You also have to provide an {@link ElasticsearchSinkFunction}. This is used to create multiple
 * {@link DocWriteRequest DocWriteRequests} for each incoming element. See the class level documentation of
 * {@link ElasticsearchSinkFunction} for an example.
 *
 * @param <T> Type of the elements handled by this sink
 */
public class ElasticsearchSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

	/**
	 * User-provided HTTP Host.
	 */
	private final List<HttpHost> httpHosts;


	private final Integer bulkProcessorFlushMaxActions;
	private final Integer bulkProcessorFlushMaxSizeMb;
	private final Integer bulkProcessorFlushIntervalMillis;
	private final ElasticsearchSinkBase.BulkFlushBackoffPolicy bulkProcessorFlushBackoffPolicy;

	// ------------------------------------------------------------------------
	//  User-facing API and configuration
	// ------------------------------------------------------------------------

	/** The user specified config map that we forward to Elasticsearch when we create the {@link RestHighLevelClient}. */
	private final Map<String, String> userConfig;

	/** The function that is used to construct multiple {@link DocWriteRequest DocWriteRequests} from each incoming element. */
	private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

	/** User-provided handler for failed {@link DocWriteRequest DocWriteRequests}. */
	private final DocWriteRequestFailureHandler failureHandler;

	/** If true, the producer will wait until all outstanding action requests have been sent to Elasticsearch. */
	private boolean flushOnCheckpoint = true;

	/** Provided to the user via the {@link ElasticsearchSinkFunction} to add {@link DocWriteRequest DocWriteRequests}. */
	private transient RequestIndexer requestIndexer;

	// ------------------------------------------------------------------------
	//  Internals for the Flink Elasticsearch Sink
	// ------------------------------------------------------------------------

	/**
	 * Number of pending action requests not yet acknowledged by Elasticsearch.
	 * This value is maintained only if {@link ElasticsearchSinkBase#flushOnCheckpoint} is {@code true}.
	 *
	 * <p>This is incremented whenever the user adds (or re-adds through the {@link DocWriteRequestFailureHandler}) requests
	 * to the {@link RequestIndexer}. It is decremented for each completed request of a bulk request, in
	 * {@link BulkProcessor.Listener#afterBulk(long, BulkRequest, BulkResponse)} and
	 * {@link BulkProcessor.Listener#afterBulk(long, BulkRequest, Throwable)}.
	 */
	private AtomicLong numPendingRequests = new AtomicLong(0);

	/** Elasticsearch client created using the call bridge. */
	private transient RestHighLevelClient client;

	/** Bulk processor to buffer and send requests to Elasticsearch, created using the client. */
	private transient BulkProcessor bulkProcessor;

	/**
	 * This is set from inside the {@link BulkProcessor.Listener} if a {@link Throwable} was thrown in callbacks and
	 * the user considered it should fail the sink via the
	 * {@link DocWriteRequestFailureHandler#onFailure(DocWriteRequest, Throwable, int, RequestIndexer)}
	 * method.
	 *
	 * <p>Errors will be checked and rethrown before processing each input element, and when the sink is closed.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link RestHighLevelClient}.
	 *
	 * @param elasticsearchSinkFunction This is used to generate multiple {@link DocWriteRequest} from the incoming element
	 * @param httpHosts The list of {@HttpHost} to which the {@link RestHighLevelClient} connects to.
	 */
	public ElasticsearchSink(Map<String, String> userConfig, List<HttpHost> httpHosts, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {

		this(userConfig, httpHosts, elasticsearchSinkFunction, new NoOpFailureHandler());
	}

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link RestHighLevelClient}.
	 *
	 * @param elasticsearchSinkFunction This is used to generate multiple {@link DocWriteRequest} from the incoming element
	 * @param failureHandler This is used to handle failed {@link DocWriteRequest}
	 * @param httpHosts The list of {@HttpHost} to which the {@link RestHighLevelClient} connects to.
	 */
	public ElasticsearchSink(
		Map<String, String> userConfig,
		List<HttpHost> httpHosts,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
		DocWriteRequestFailureHandler failureHandler) {

		Preconditions.checkArgument(httpHosts != null && !httpHosts.isEmpty());

		this.httpHosts = httpHosts;
		this.elasticsearchSinkFunction = checkNotNull(elasticsearchSinkFunction);
		this.failureHandler = checkNotNull(failureHandler);

		// we eagerly check if the user-provided sink function and failure handler is serializable;
		// otherwise, if they aren't serializable, users will merely get a non-informative error message
		// "ElasticsearchSinkBase is not serializable"

		checkArgument(InstantiationUtil.isSerializable(elasticsearchSinkFunction),
			"The implementation of the provided ElasticsearchSinkFunction is not serializable. " +
				"The object probably contains or references non-serializable fields.");

		checkArgument(InstantiationUtil.isSerializable(failureHandler),
			"The implementation of the provided DocWriteRequestFailureHandler is not serializable. " +
				"The object probably contains or references non-serializable fields.");

		// extract and remove bulk processor related configuration from the user-provided config,
		// so that the resulting user config only contains configuration related to the Elasticsearch client.

		checkNotNull(userConfig);

		ParameterTool params = ParameterTool.fromMap(userConfig);

		if (params.has(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
			bulkProcessorFlushMaxActions = params.getInt(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
			userConfig.remove(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
		} else {
			bulkProcessorFlushMaxActions = null;
		}

		if (params.has(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
			bulkProcessorFlushMaxSizeMb = params.getInt(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
			userConfig.remove(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
		} else {
			bulkProcessorFlushMaxSizeMb = null;
		}

		if (params.has(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
			bulkProcessorFlushIntervalMillis = params.getInt(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
			userConfig.remove(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
		} else {
			bulkProcessorFlushIntervalMillis = null;
		}

		boolean bulkProcessorFlushBackoffEnable = params.getBoolean(ElasticsearchSinkBase
			.CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, true);
		userConfig.remove(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE);

		if (bulkProcessorFlushBackoffEnable) {
			this.bulkProcessorFlushBackoffPolicy = new ElasticsearchSinkBase.BulkFlushBackoffPolicy();

			if (params.has(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)) {
				bulkProcessorFlushBackoffPolicy.setBackoffType(ElasticsearchSinkBase.FlushBackoffType.valueOf(params.get
					(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)));
				userConfig.remove(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE);
			}

			if (params.has(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES)) {
				bulkProcessorFlushBackoffPolicy.setMaxRetryCount(params.getInt(ElasticsearchSinkBase
					.CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES));
				userConfig.remove(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES);
			}

			if (params.has(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY)) {
				bulkProcessorFlushBackoffPolicy.setDelayMillis(params.getLong(ElasticsearchSinkBase
					.CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY));
				userConfig.remove(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY);
			}

		} else {
			bulkProcessorFlushBackoffPolicy = null;
		}

		this.userConfig = userConfig;
	}

	/**
	 * Creates an Elasticsearch {@link RestHighLevelClient}.
	 *
	 * @return The created client.
	 */
	public RestHighLevelClient createClient() {
		RestHighLevelClient rhlClient =
			new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));

		LOG.info("Created Elasticsearch RestHighLevelClient connected to {}", httpHosts.toString());

		return rhlClient;
	}

	private Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
		if (!bulkItemResponse.isFailed()) {
			return null;
		} else {
			return bulkItemResponse.getFailure().getCause();
		}
	}

	private void configureBulkProcessorBackoff(
		BulkProcessor.Builder builder,
		@Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {

		BackoffPolicy backoffPolicy;
		if (flushBackoffPolicy != null) {
			switch (flushBackoffPolicy.getBackoffType()) {
				case CONSTANT:
					backoffPolicy = BackoffPolicy.constantBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
					break;
				case EXPONENTIAL:
				default:
					backoffPolicy = BackoffPolicy.exponentialBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
			}
		} else {
			backoffPolicy = BackoffPolicy.noBackoff();
		}

		builder.setBackoffPolicy(backoffPolicy);
	}

	public RequestIndexer createRequestIndex(
		BulkProcessor bulkProcessor,
		boolean flushOnCheckpoint,
		AtomicLong numPendingRequests) {
		return new BulkProcessorIndexer(bulkProcessor, flushOnCheckpoint, numPendingRequests);
	}

	/**
	 * Disable flushing on checkpoint. When disabled, the sink will not wait for all
	 * pending action requests to be acknowledged by Elasticsearch on checkpoints.
	 *
	 * <p>NOTE: If flushing on checkpoint is disabled, the Flink Elasticsearch Sink does NOT
	 * provide any strong guarantees for at-least-once delivery of action requests.
	 */
	public void disableFlushOnCheckpoint() {
		this.flushOnCheckpoint = false;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		client = createClient();
		bulkProcessor = buildBulkProcessor(new BulkProcessorListener());
		requestIndexer = createRequestIndex(bulkProcessor, flushOnCheckpoint, numPendingRequests);
	}

	@Override
	public void invoke(T value) throws Exception {
		// if bulk processor callbacks have previously reported an error, we rethrow the error and fail the sink
		checkErrorAndRethrow();

		elasticsearchSinkFunction.process(value, getRuntimeContext(), requestIndexer);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// no initialization needed
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		checkErrorAndRethrow();

		if (flushOnCheckpoint) {
			do {
				bulkProcessor.flush();
				checkErrorAndRethrow();
			} while (numPendingRequests.get() != 0);
		}
	}

	@Override
	public void close() throws Exception {
		if (bulkProcessor != null) {
			bulkProcessor.close();
			bulkProcessor = null;
		}

		if (client != null) {
			client.close();
			client = null;
		}

		// make sure any errors from callbacks are rethrown
		checkErrorAndRethrow();
	}

	/**
	 * Build the {@link BulkProcessor}.
	 *
	 * <p>Note: this is exposed for testing purposes.
	 */
	@VisibleForTesting
	protected BulkProcessor buildBulkProcessor(BulkProcessor.Listener listener) {
		checkNotNull(listener);

		BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(client::bulkAsync, listener);

		// This makes flush() blocking
		bulkProcessorBuilder.setConcurrentRequests(0);

		if (bulkProcessorFlushMaxActions != null) {
			bulkProcessorBuilder.setBulkActions(bulkProcessorFlushMaxActions);
		}

		if (bulkProcessorFlushMaxSizeMb != null) {
			bulkProcessorBuilder.setBulkSize(new ByteSizeValue(bulkProcessorFlushMaxSizeMb, ByteSizeUnit.MB));
		}

		if (bulkProcessorFlushIntervalMillis != null) {
			bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(bulkProcessorFlushIntervalMillis));
		}

		// if backoff retrying is disabled, bulkProcessorFlushBackoffPolicy will be null
		configureBulkProcessorBackoff(bulkProcessorBuilder, bulkProcessorFlushBackoffPolicy);

		return bulkProcessorBuilder.build();
	}

	private void checkErrorAndRethrow() {
		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occurred in ElasticsearchSink.", cause);
		}
	}

	private class BulkProcessorListener implements BulkProcessor.Listener {
		@Override
		public void beforeBulk(long executionId, BulkRequest request) { }

		@Override
		public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			if (response.hasFailures()) {
				BulkItemResponse itemResponse;
				Throwable failure;
				RestStatus restStatus;

				try {
					for (int i = 0; i < response.getItems().length; i++) {
						itemResponse = response.getItems()[i];
						failure = extractFailureCauseFromBulkItemResponse(itemResponse);
						if (failure != null) {
							LOG.error("Failed Elasticsearch item request: {}", itemResponse.getFailureMessage(), failure);

							restStatus = itemResponse.getFailure().getStatus();
							if (restStatus == null) {
								failureHandler.onFailure(request.requests().get(i), failure, -1, requestIndexer);
							} else {
								failureHandler.onFailure(request.requests().get(i), failure, restStatus.getStatus(), requestIndexer);
							}
						}
					}
				} catch (Throwable t) {
					// fail the sink and skip the rest of the items
					// if the failure handler decides to throw an exception
					failureThrowable.compareAndSet(null, t);
				}
			}

			if (flushOnCheckpoint) {
				numPendingRequests.getAndAdd(-request.numberOfActions());
			}
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
			LOG.error("Failed Elasticsearch bulk request: {}", failure.getMessage(), failure.getCause());

			try {
				for (DocWriteRequest docWriteRequest : request.requests()) {
					failureHandler.onFailure(docWriteRequest, failure, -1, requestIndexer);
				}
			} catch (Throwable t) {
				// fail the sink and skip the rest of the items
				// if the failure handler decides to throw an exception
				failureThrowable.compareAndSet(null, t);
			}

			if (flushOnCheckpoint) {
				numPendingRequests.getAndAdd(-request.numberOfActions());
			}
		}
	}

	@VisibleForTesting
	long getNumPendingRequests() {
		if (flushOnCheckpoint) {
			return numPendingRequests.get();
		} else {
			throw new UnsupportedOperationException(
				"The number of pending requests is not maintained when flushing on checkpoint is disabled.");
		}
	}
}
