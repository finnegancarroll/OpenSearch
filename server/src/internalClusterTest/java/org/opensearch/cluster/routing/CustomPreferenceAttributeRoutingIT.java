/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CustomPreferenceAttributeRoutingIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    /**
     * For regression in custom string query preference with awareness attributes enabled.
     * We expect preference will consistently route to the same shard replica. However, when awareness attributes
     * are configured this does not hold.
     */
    public void testCustomPreferenceShardIdCombination() {
        // Configure cluster with awareness attributes
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "rack")
            .put("cluster.routing.allocation.awareness.force.rack.values", "rack1,rack2")
            .put("cluster.routing.use_adaptive_replica_selection", false)
            .put("cluster.search.ignore_awareness_attributes", false)
            .build();

        // Start cluster
        internalCluster().startClusterManagerOnlyNode(commonSettings);
        internalCluster().startDataOnlyNodes(2,
            Settings.builder().put(commonSettings).put("node.attr.rack", "rack1").build());
        internalCluster().startDataOnlyNodes(2,
            Settings.builder().put(commonSettings).put("node.attr.rack", "rack2").build());

        ensureStableCluster(5);
        ensureGreen();

        // Create index with specific shard configuration
        assertAcked(prepareCreate("test_index")
            .setSettings(Settings.builder()
                .put("index.number_of_shards", 6)
                .put("index.number_of_replicas", 1)
                .build())
        );

        ensureGreen("test_index");

        // Index test documents
        for (int i = 0; i < 30; i++) {
            client().prepareIndex("test_index")
                .setId(String.valueOf(i))
                .setSource("field", "value" + i)
                .get();
        }
        refresh("test_index");

        /*
        Execute the same match all query with custom string preference.
        For each search and each shard in the response we record the node on which the shard was located.
        Given the custom string preference, we expect each shard or each search should report the exact same node id.
        Otherwise, the custom string pref is not producing consistent shard routing.
         */
        Map<String, Set<String>> shardToNodes = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            SearchResponse response = client().prepareSearch("test_index")
                .setQuery(matchAllQuery())
                .setPreference("test_preference_123")
                .setSize(30)
                .get();
            for (int j = 0; j < response.getHits().getHits().length; j++) {
                String shardId = response.getHits().getAt(j).getShard().getShardId().toString();
                String nodeId = response.getHits().getAt(j).getShard().getNodeId();
                shardToNodes.computeIfAbsent(shardId, k -> new HashSet<>()).add(nodeId);
            }
        }

        /*
        If more than one node was responsible for serving a request for a given shard,
        then there was a regression in the custom preference string.
         */
        logger.info("--> shard to node mappings: {}", shardToNodes);
        for (Map.Entry<String, Set<String>> entry : shardToNodes.entrySet()) {
            assertThat("Shard " + entry.getKey() + " should consistently route to the same node",
                entry.getValue().size(), equalTo(1));
        }
    }
}
