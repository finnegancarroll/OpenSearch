/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.CardinalityAggregation;
import org.opensearch.protobufs.MissingAggregation;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import io.grpc.ManagedChannel;

import java.util.List;

/**
 * Integration tests for the SearchService gRPC service.
 */
public class SearchServiceIT extends GrpcTransportBaseIT {

    /**
     * Tests the search operation via gRPC.
     */
    public void testSearchServiceSearch() throws Exception {
        // Create a test index
        String indexName = "test-search-index";
        createTestIndex(indexName);

        // Add a document to the index
        indexTestDocument(indexName, "1", DEFAULT_DOCUMENT_SOURCE);

        // Create a gRPC client
        try (NettyGrpcClient client = createGrpcClient()) {
            // Create a SearchService stub
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);

            // Create a search request
            SearchRequestBody requestBody = SearchRequestBody.newBuilder().setFrom(0).setSize(10).build();

            SearchRequest searchRequest = SearchRequest.newBuilder()
                .addIndex(indexName)
                .setRequestBody(requestBody)
                .setQ("field1:value1")
                .build();

            // Execute the search request
            SearchResponse searchResponse = searchStub.search(searchRequest);

            // Verify the response
            assertNotNull("Search response should not be null", searchResponse);
            assertTrue("Search response should have hits", searchResponse.getHits().getTotal().getTotalHits().getValue() > 0);
            assertEquals("Search response should have one hit", 1, searchResponse.getHits().getHitsCount());
            assertEquals("Hit should have correct ID", "1", searchResponse.getHits().getHits(0).getXId());
        }
    }

    public void testBasicMissingAgg() throws Exception {
        String indexName = "test-index";
        createTestIndex(indexName);

        List<String> testDocs = List.of(
            "{\"species\":\"turtle\",\"age\":142}",
            "{\"species\":\"cat\",\"age\":12}",
            "{\"species\":\"dog\",\"age\":5}",
            "{\"species\":\"dog\"}"
        );
        for (String testDoc : testDocs) {
            client().prepareIndex(indexName).setSource(testDoc, XContentType.JSON).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();

        MissingAggregation.Builder missingAgg = MissingAggregation.newBuilder()
            .setField("age");

        AggregationContainer aggCont = AggregationContainer.newBuilder()
            .setMissing(missingAgg)
            .build();

        SearchRequestBody requestBody = SearchRequestBody.newBuilder()
            .putAggregations("missing_age", aggCont)
            .build();

        SearchRequest searchRequest = SearchRequest.newBuilder()
            .addIndex(indexName)
            .setRequestBody(requestBody)
            .build();

        SearchResponse searchResponse;
        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);
            searchResponse = searchStub.search(searchRequest);
        }

        assertNotNull("Search response should not be null", searchResponse);
        assertEquals("Search response should return all documents as hits", 4, searchResponse.getHits().getHitsCount());
        assertEquals("Missing doc count should be 1", 1, searchResponse.getAggregationsMap().get("missing_age").getMissing().getDocCount());
    }

    public void testBasicCardinalityAgg() throws Exception {
        String indexName = "new-test-index";
        String mapping = """
            {
              "properties": {
                "species": {
                  "type": "keyword"
                },
                "age": {
                  "type": "integer"
                }
              }
            }""";
        createIndex(indexName, Settings.EMPTY, mapping);
        ensureGreen(indexName);

        List<String> testDocs = List.of(
            "{\"species\":\"turtle\",\"age\":142}",
            "{\"species\":\"cat\",\"age\":12}",
            "{\"species\":\"dog\",\"age\":5}",
            "{\"species\":\"dog\"}"
        );
        for (String testDoc : testDocs) {
            client().prepareIndex(indexName).setSource(testDoc, XContentType.JSON).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();

        CardinalityAggregation.Builder cardinalityAgg = CardinalityAggregation.newBuilder()
            .setField("species");

        AggregationContainer aggCont = AggregationContainer.newBuilder()
            .setCardinality(cardinalityAgg)
            .build();

        SearchRequestBody requestBody = SearchRequestBody.newBuilder()
            .putAggregations("species_cardinality", aggCont)
            .build();

        SearchRequest searchRequest = SearchRequest.newBuilder()
            .addIndex(indexName)
            .setRequestBody(requestBody)
            .build();

        SearchResponse searchResponse;
        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);
            searchResponse = searchStub.search(searchRequest);
        }

        assertNotNull("Search response should not be null", searchResponse);
        assertEquals("Search response should return all documents as hits", 4, searchResponse.getHits().getHitsCount());
        assertEquals("Cardinality of species field should be 3", 3, searchResponse.getAggregationsMap().get("species_cardinality").getCardinality().getValue());
    }

    public void testMultiAggRequest() throws Exception {
        String indexName = "new-test-index";
        String mapping = """
            {
              "properties": {
                "http-method": {
                  "type": "keyword"
                },
                "response-code": {
                  "type": "keyword"
                },
                "uri": {
                  "type": "keyword"
                }
              }
            }""";
        createIndex(indexName, Settings.EMPTY, mapping);
        ensureGreen(indexName);

        List<String> testDocs = List.of(
            "{\"http-method\":\"del\",\"response-code\":\"200\",\"uri\":142}",
            "{\"http-method\":\"put\",\"response-code\":\"200\",\"uri\":142}",
            "{\"http-method\":\"get\",\"response-code\":\"200\",\"uri\":142}",
            "{\"http-method\":\"get\",\"response-code\":\"401\"}"
        );
        for (String testDoc : testDocs) {
            client().prepareIndex(indexName).setSource(testDoc, XContentType.JSON).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();

        CardinalityAggregation.Builder methodCardAgg = CardinalityAggregation.newBuilder()
            .setField("http-method");
        AggregationContainer methodCardAggCont = AggregationContainer.newBuilder()
            .setCardinality(methodCardAgg)
            .build();

        CardinalityAggregation.Builder respCardAgg = CardinalityAggregation.newBuilder()
            .setField("response-code");
        AggregationContainer respCardAggCont = AggregationContainer.newBuilder()
            .setCardinality(respCardAgg)
            .build();

        MissingAggregation.Builder uriMissingAgg = MissingAggregation.newBuilder()
            .setField("uri");
        AggregationContainer uriMissingAggCont = AggregationContainer.newBuilder()
            .setMissing(uriMissingAgg)
            .build();

        SearchRequestBody requestBody = SearchRequestBody.newBuilder()
            .putAggregations("http-method-card", methodCardAggCont)
            .putAggregations("http-response-card", respCardAggCont)
            .putAggregations("no-uri", uriMissingAggCont)
            .build();

        SearchRequest searchRequest = SearchRequest.newBuilder()
            .addIndex(indexName)
            .setRequestBody(requestBody)
            .build();

        SearchResponse searchResponse;
        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);
            searchResponse = searchStub.search(searchRequest);
        }

        assertNotNull("Search response should not be null", searchResponse);
        assertEquals("Search response should return all documents as hits", 4, searchResponse.getHits().getHitsCount());
        assertEquals("Http method cardinality should be 3", 3, searchResponse.getAggregationsMap().get("http-method-card").getCardinality().getValue());
        assertEquals("Http response code cardinality should be 2", 2, searchResponse.getAggregationsMap().get("http-response-card").getCardinality().getValue());
        assertEquals("Documents missing uri should be 1", 1, searchResponse.getAggregationsMap().get("no-uri").getMissing().getDocCount());
    }
}
