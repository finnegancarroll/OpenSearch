/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.Map;

/**
 * Integration test verifying that analytics engine queries increment the standard
 * {@code query_total} counter in the {@code GET /_nodes/stats} search stats section
 * via the {@code SearchStatsContributor} extension point.
 */
public class SearchStatsContributorIT extends AnalyticsRestTestCase {

    private static final String INDEX = "search_stats_contributor_test";

    public void testQueryTotalIncrementsAfterAnalyticsQueries() throws Exception {
        createIndex();
        indexData();

        // Get baseline query_total
        long baselineQueryTotal = getQueryTotal();

        // Execute analytics queries
        executePPL("source = " + INDEX + " | stats avg(score) by name");
        executePPL("source = " + INDEX + " | where score > 80");
        executePPL("source = " + INDEX + " | fields name, score");

        // Verify query_total incremented
        long afterQueryTotal = getQueryTotal();
        long increment = afterQueryTotal - baselineQueryTotal;
        assertTrue(
            "query_total must increment by at least 1 after analytics queries, got increment=" + increment,
            increment >= 1
        );
    }

    @SuppressWarnings("unchecked")
    private long getQueryTotal() throws Exception {
        Request request = new Request("GET", "/_nodes/stats/indices/search");
        Response response = client().performRequest(request);
        Map<String, Object> body = entityAsMap(response);

        Map<String, Object> nodes = (Map<String, Object>) body.get("nodes");
        // Sum query_total across all nodes
        long total = 0;
        for (Object nodeValue : nodes.values()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodeValue;
            Map<String, Object> indices = (Map<String, Object>) nodeStats.get("indices");
            if (indices == null) continue;
            Map<String, Object> search = (Map<String, Object>) indices.get("search");
            if (search == null) continue;
            Number queryTotal = (Number) search.get("query_total");
            if (queryTotal != null) {
                total += queryTotal.longValue();
            }
        }
        return total;
    }

    private void executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        assertEquals("PPL query must succeed", 200, response.getStatusLine().getStatusCode());
    }

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("{"
            + "\"settings\": {"
            + "  \"index.number_of_shards\": 2,"
            + "  \"index.number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": {\"type\": \"keyword\"},"
            + "    \"score\": {\"type\": \"double\"}"
            + "  }"
            + "}"
            + "}");
        client().performRequest(create);
    }

    private void indexData() throws Exception {
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"name\":\"alice\",\"score\":95.5}\n"
                + "{\"index\":{}}\n{\"name\":\"bob\",\"score\":87.3}\n"
                + "{\"index\":{}}\n{\"name\":\"carol\",\"score\":91.0}\n"
        );
        client().performRequest(bulk);

        // Flush to commit parquet files
        Request flush = new Request("POST", "/" + INDEX + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);

        // Wait for index health
        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }
}
