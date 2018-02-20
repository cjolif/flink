package org.apache.flink.streaming.connectors.elasticsearch.rest;

import org.apache.flink.api.common.functions.RuntimeContext;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;

import java.util.Arrays;

class WrapBaseSinkFunction<T> implements ElasticsearchSinkFunction<T> {
	private org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction elasticsearchSinkFunction;

	WrapBaseSinkFunction(org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction elasticSearchSinkFunction) {
		this.elasticsearchSinkFunction = elasticSearchSinkFunction;
	}

	public void process(T element, RuntimeContext ctx, RequestIndexer indexer) {
		this.elasticsearchSinkFunction.process(element, ctx, new WrapRequestIndexer(indexer));
	}

	private class WrapRequestIndexer implements org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer {
		private RequestIndexer indexer;

		WrapRequestIndexer(RequestIndexer indexer) {
			this.indexer = indexer;
		}

		public void add(ActionRequest... actionRequests) {
			indexer.add(Arrays.copyOf(actionRequests, actionRequests.length, DocWriteRequest[].class));
		}
	}
}
