/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.MissingAggregation;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SearchResponse;
import org.opensearch.protobufs.services.SearchServiceGrpc;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import io.grpc.ManagedChannel;

import java.util.ArrayList;
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
                .setSearchRequestBody(requestBody)
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

    public void testMissingAgg() throws Exception {
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
            .setQ("field1:value1")
            .build();

        try (NettyGrpcClient client = createGrpcClient()) {
            ManagedChannel channel = client.getChannel();
            SearchServiceGrpc.SearchServiceBlockingStub searchStub = SearchServiceGrpc.newBlockingStub(channel);
            SearchResponse searchResponse = searchStub.search(searchRequest);
        }

        assertNotNull("Search response should not be null", searchResponse);
        assertTrue("Search response should have hits", searchResponse.getHits().getTotal().getTotalHits().getValue() > 0);
        assertEquals("Search response should have one hit", 1, searchResponse.getHits().getHitsCount());
        assertEquals("Hit should have correct ID", "1", searchResponse.getHits().getHits(0).getXId());
    }
}
