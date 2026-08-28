/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.List;
import java.util.Map;

/**
 * EXPERIMENT (not a real regression test): force a shard to contain a MIX of parquet files —
 * some where {@code tags} is physically scalar and some where it is physically LIST — by using
 * separate flush cycles so each batch becomes its own segment/parquet file.
 *
 * <p>Scenario:
 * <ol>
 *   <li>Create a 1-shard composite index. {@code tags} declared {@code multi_value: true}.</li>
 *   <li>Batch A: several SINGLE-value docs → flush → segment/parquet file #1.</li>
 *   <li>Batch B: one MULTI-value doc → flush → segment/parquet file #2.</li>
 *   <li>Query across both (no force-merge) and observe whether reads succeed.</li>
 * </ol>
 *
 * <p>preserveIndicesUponCompletion/preserveClusterUponCompletion are overridden true so the
 * on-disk parquet files survive for manual inspection after the test.
 */
public class MixedFormatExperimentIT extends AnalyticsRestTestCase {

    private static final String INDEX = "mixed_format_exp";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    private static String compositeSettings() {
        return "\"number_of_shards\": 1,"
            + "\"number_of_replicas\": 0,"
            + "\"index.pluggable.dataformat.enabled\": true,"
            + "\"index.pluggable.dataformat\": \"composite\","
            + "\"index.composite.primary_data_format\": \"parquet\","
            + "\"index.composite.secondary_data_formats\": [\"lucene\"]";
    }

    @SuppressWarnings("unchecked")
    public void testMixedScalarAndListFilesInOneShard() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String mapping = "{"
            + "\"settings\": {" + compositeSettings() + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"id\":   { \"type\": \"keyword\" },"
            + "    \"tags\": { \"type\": \"keyword\", \"multi_value\": true }"
            + "  }"
            + "}"
            + "}";
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(mapping);
        assertOkAndParse(client().performRequest(create), "create " + INDEX);

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        // ---- Batch A: single-value docs only, then flush → segment #1 ----
        String batchA = "{\"index\":{}}\n{\"id\":\"a1\",\"tags\":\"red\"}\n"
            + "{\"index\":{}}\n{\"id\":\"a2\",\"tags\":\"green\"}\n"
            + "{\"index\":{}}\n{\"id\":\"a3\",\"tags\":\"blue\"}\n";
        Request bulkA = new Request("POST", "/" + INDEX + "/_bulk");
        bulkA.setJsonEntity(batchA);
        bulkA.addParameter("refresh", "true");
        Map<String, Object> respA = assertOkAndParse(client().performRequest(bulkA), "_bulk A");
        assertEquals("batch A no errors: " + respA, Boolean.FALSE, respA.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));

        // ---- Batch B: one multi-value doc, then flush → segment #2 ----
        String batchB = "{\"index\":{}}\n{\"id\":\"b1\",\"tags\":[\"alpha\",\"beta\"]}\n";
        Request bulkB = new Request("POST", "/" + INDEX + "/_bulk");
        bulkB.setJsonEntity(batchB);
        bulkB.addParameter("refresh", "true");
        Map<String, Object> respB = assertOkAndParse(client().performRequest(bulkB), "_bulk B");
        assertEquals("batch B no errors: " + respB, Boolean.FALSE, respB.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));

        // ---- Observe: how many segments/files, and can we read across both shapes? ----
        Request segs = new Request("GET", "/" + INDEX + "/_segments");
        logger.info("SEGMENTS: {}", EntityUtilsToString(client().performRequest(segs)));

        // Full scan (ListingTable path) across both files.
        Map<String, Object> all = executePpl("source = " + INDEX + " | fields id, tags");
        List<String> cols = extractColumnNames(all);
        List<List<Object>> rows = (List<List<Object>>) all.get("datarows");
        logger.info("SCAN COLUMNS: {}", cols);
        for (List<Object> row : rows) {
            logger.info("SCAN ROW: {}", row);
        }
        assertEquals("all four docs must come back across both files", 4, rows.size());

        // Indexed path (where predicate) touching the scalar-file docs.
        Map<String, Object> scalarQuery = executePpl("source = " + INDEX + " | where id = 'a1' | fields id, tags");
        List<List<Object>> scalarRows = (List<List<Object>>) scalarQuery.get("datarows");
        logger.info("SCALAR-FILE INDEXED QUERY ROWS: {}", scalarRows);
        assertEquals(1, scalarRows.size());

        // Indexed path touching the list-file doc.
        Map<String, Object> listQuery = executePpl("source = " + INDEX + " | where id = 'b1' | fields id, tags");
        List<List<Object>> listRows = (List<List<Object>>) listQuery.get("datarows");
        logger.info("LIST-FILE INDEXED QUERY ROWS: {}", listRows);
        assertEquals(1, listRows.size());
    }

    private String EntityUtilsToString(org.opensearch.client.Response r) throws Exception {
        return new String(r.getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
}
