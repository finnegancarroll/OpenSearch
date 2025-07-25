/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.state;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndexClosedException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.opensearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CloseIndexIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final int MAX_DOCS = 25_000;

    public CloseIndexIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    // This is to reuse CloseIndexIT in RemoteCloseIndexIT .
    // Concurrent search deosn't make a difference in these tests.
    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Collections.singletonList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() }
        );
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(
                IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                new ByteSizeValue(randomIntBetween(1, 4096), ByteSizeUnit.KB)
            )
            .build();
    }

    public void testCloseMissingIndex() {
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareClose("test").get());
        assertThat(e.getMessage(), is("no such index [test]"));
    }

    public void testCloseOneMissingIndex() {
        createIndex("test1");
        final IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> client().admin().indices().prepareClose("test1", "test2").get()
        );
        assertThat(e.getMessage(), is("no such index [test2]"));
    }

    public void testCloseOneMissingIndexIgnoreMissing() throws Exception {
        createIndex("test1");
        assertBusy(() -> assertAcked(client().admin().indices().prepareClose("test1", "test2").setIndicesOptions(lenientExpandOpen())));
        assertIndexIsClosed("test1");
    }

    public void testCloseNoIndex() {
        final ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().admin().indices().prepareClose().get()
        );
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testCloseNullIndex() {
        final ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().admin().indices().prepareClose((String[]) null).get()
        );
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testCloseIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int nbDocs = randomIntBetween(0, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                .collect(toList())
        );

        assertBusy(() -> closeIndices(indexName));
        assertIndexIsClosed(indexName);

        assertAcked(client().admin().indices().prepareOpen(indexName));
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), nbDocs);
    }

    public void testCloseAlreadyClosedIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        if (randomBoolean()) {
            indexRandom(
                randomBoolean(),
                false,
                randomBoolean(),
                IntStream.range(0, randomIntBetween(1, 10))
                    .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                    .collect(toList())
            );
        }
        // First close should be fully acked
        assertBusy(() -> closeIndices(indexName));
        assertIndexIsClosed(indexName);

        // Second close should be acked too
        final ActiveShardCount activeShardCount = randomFrom(ActiveShardCount.NONE, ActiveShardCount.DEFAULT, ActiveShardCount.ALL);
        assertBusy(() -> {
            CloseIndexResponse response = client().admin().indices().prepareClose(indexName).setWaitForActiveShards(activeShardCount).get();
            assertAcked(response);
            assertTrue(response.getIndices().isEmpty());
        });
        assertIndexIsClosed(indexName);
    }

    public void testCloseUnassignedIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(indexName).setWaitForActiveShards(ActiveShardCount.NONE)
                .setSettings(Settings.builder().put("index.routing.allocation.include._name", "nothing").build())
        );

        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metadata().indices().get(indexName).getState(), is(IndexMetadata.State.OPEN));
        assertThat(clusterState.routingTable().allShards().stream().allMatch(ShardRouting::unassigned), is(true));

        assertBusy(() -> closeIndices(client().admin().indices().prepareClose(indexName).setWaitForActiveShards(ActiveShardCount.NONE)));
        assertIndexIsClosed(indexName);
    }

    public void testConcurrentClose() throws InterruptedException {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int nbDocs = randomIntBetween(10, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                .collect(toList())
        );
        ensureYellowAndNoInitializingShards(indexName);

        final CountDownLatch startClosing = new CountDownLatch(1);
        final Thread[] threads = new Thread[randomIntBetween(2, 5)];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    startClosing.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                try {
                    client().admin().indices().prepareClose(indexName).get();
                } catch (final Exception e) {
                    assertException(e, indexName);
                }
            });
            threads[i].start();
        }

        startClosing.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertIndexIsClosed(indexName);
    }

    public void testCloseWhileIndexingDocuments() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        int nbDocs = 0;
        try (BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client(), MAX_DOCS)) {
            indexer.setFailureAssertion(t -> assertException(t, indexName));

            waitForDocs(randomIntBetween(10, 50), indexer);
            assertBusy(() -> closeIndices(indexName));
            indexer.stopAndAwaitStopped();
            nbDocs += indexer.totalIndexedDocs();
        }

        assertIndexIsClosed(indexName);
        assertAcked(client().admin().indices().prepareOpen(indexName));
        assertHitCount(client().prepareSearch(indexName).setSize(0).setTrackTotalHitsUpTo(TRACK_TOTAL_HITS_ACCURATE).get(), nbDocs);
    }

    public void testCloseWhileDeletingIndices() throws Exception {
        final String[] indices = new String[randomIntBetween(3, 10)];
        for (int i = 0; i < indices.length; i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName);
            if (randomBoolean()) {
                indexRandom(
                    randomBoolean(),
                    false,
                    randomBoolean(),
                    IntStream.range(0, 10)
                        .mapToObj(n -> client().prepareIndex(indexName).setId(String.valueOf(n)).setSource("num", n))
                        .collect(toList())
                );
            }
            indices[i] = indexName;
        }
        assertThat(client().admin().cluster().prepareState().get().getState().metadata().indices().size(), equalTo(indices.length));

        final List<Thread> threads = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);

        for (final String indexToDelete : indices) {
            threads.add(new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                try {
                    assertAcked(client().admin().indices().prepareDelete(indexToDelete).setTimeout("60s"));
                } catch (final Exception e) {
                    assertException(e, indexToDelete);
                }
            }));
        }
        for (final String indexToClose : indices) {
            threads.add(new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                try {
                    client().admin().indices().prepareClose(indexToClose).setTimeout("60s").get();
                } catch (final Exception e) {
                    assertException(e, indexToClose);
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testConcurrentClosesAndOpens() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client(), MAX_DOCS);
        indexer.setFailureAssertion(e -> {});
        waitForDocs(1, indexer);

        final CountDownLatch latch = new CountDownLatch(1);
        final Runnable waitForLatch = () -> {
            try {
                latch.await();
            } catch (final InterruptedException e) {
                throw new AssertionError(e);
            }
        };

        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            threads.add(new Thread(() -> {
                try {
                    waitForLatch.run();
                    client().admin().indices().prepareClose(indexName).get();
                } catch (final Exception e) {
                    throw new AssertionError(e);
                }
            }));
        }
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            threads.add(new Thread(() -> {
                try {
                    waitForLatch.run();
                    assertAcked(client().admin().indices().prepareOpen(indexName).get());
                } catch (final Exception e) {
                    throw new AssertionError(e);
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        indexer.stopAndAwaitStopped();

        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        if (clusterState.metadata().indices().get(indexName).getState() == IndexMetadata.State.CLOSE) {
            assertIndexIsClosed(indexName);
            assertAcked(client().admin().indices().prepareOpen(indexName));
        }
        refresh(indexName);
        assertIndexIsOpened(indexName);
        waitForReplication(indexName);
        assertHitCount(
            client().prepareSearch(indexName).setSize(0).setTrackTotalHitsUpTo(TRACK_TOTAL_HITS_ACCURATE).get(),
            indexer.totalIndexedDocs()
        );
    }

    public void testCloseIndexWaitForActiveShards() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0) // no replicas to avoid recoveries that could fail the index closing
                .build()
        );

        final int nbDocs = randomIntBetween(0, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                .collect(toList())
        );
        ensureGreen(indexName);

        final CloseIndexResponse closeIndexResponse = client().admin()
            .indices()
            .prepareClose(indexName)
            .setWaitForActiveShards(ActiveShardCount.DEFAULT)
            .get();
        assertThat(client().admin().cluster().prepareHealth(indexName).get().getStatus(), is(ClusterHealthStatus.GREEN));
        assertTrue(closeIndexResponse.isAcknowledged());
        assertTrue(closeIndexResponse.isShardsAcknowledged());
        assertThat(closeIndexResponse.getIndices().get(0), notNullValue());
        assertThat(closeIndexResponse.getIndices().get(0).hasFailures(), is(false));
        assertThat(closeIndexResponse.getIndices().get(0).getIndex().getName(), equalTo(indexName));
        assertIndexIsClosed(indexName);
    }

    public void testNoopPeerRecoveriesWhenIndexClosed() throws Exception {
        final String indexName = "noop-peer-recovery-test";
        int numberOfReplicas = between(1, 2);
        internalCluster().ensureAtLeastNumDataNodes(numberOfReplicas + between(1, 2));
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                .put("index.routing.rebalance.enable", "none")
                .build()
        );
        int iterations = between(1, 3);
        for (int iter = 0; iter < iterations; iter++) {
            indexRandom(
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                IntStream.range(0, randomIntBetween(0, 50))
                    .mapToObj(n -> client().prepareIndex(indexName).setSource("num", n))
                    .collect(toList())
            );
            ensureGreen(indexName);

            // Closing an index should execute noop peer recovery
            assertAcked(client().admin().indices().prepareClose(indexName).get());
            assertIndexIsClosed(indexName);
            ensureGreen(indexName);
            assertNoFileBasedRecovery(indexName);
            internalCluster().assertSameDocIdsOnShards();

            // Open a closed index should execute noop recovery
            assertAcked(client().admin().indices().prepareOpen(indexName).get());
            assertIndexIsOpened(indexName);
            ensureGreen(indexName);
            assertNoFileBasedRecovery(indexName);
            internalCluster().assertSameDocIdsOnShards();
        }
    }

    /**
     * Ensures that if a replica of a closed index does not have the same content as the primary, then a file-based recovery will occur.
     */
    public void testRecoverExistingReplica() throws Exception {
        final String indexName = "test-recover-existing-replica";
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<String> dataNodes = randomSubsetOf(
            2,
            Sets.newHashSet(clusterService().state().nodes().getDataNodes().values().iterator())
                .stream()
                .map(DiscoveryNode::getName)
                .collect(Collectors.toSet())
        );
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name", String.join(",", dataNodes))
                .build()
        );
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 50))
                .mapToObj(n -> client().prepareIndex(indexName).setSource("num", n))
                .collect(toList())
        );
        ensureGreen(indexName);
        client().admin().indices().prepareFlush(indexName).get();
        // index more documents while one shard copy is offline
        internalCluster().restartNode(dataNodes.get(1), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                Client client = client(dataNodes.get(0));
                int moreDocs = randomIntBetween(1, 50);
                for (int i = 0; i < moreDocs; i++) {
                    client.prepareIndex(indexName).setSource("num", i).get();
                }
                assertAcked(client.admin().indices().prepareClose(indexName));
                return super.onNodeStopped(nodeName);
            }
        });
        assertIndexIsClosed(indexName);
        ensureGreen(indexName);
        internalCluster().assertSameDocIdsOnShards();
        for (RecoveryState recovery : client().admin().indices().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), not(empty()));
            }
        }
    }

    /**
     * Test for <a href="https://github.com/elastic/elasticsearch/issues/47276">Elasticsearch issue #47276</a> which checks that the persisted metadata on a data node does not
     * become inconsistent when using replicated closed indices.
     */
    public void testRelocatedClosedIndexIssue() throws Exception {
        final String indexName = "closed-index";
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        // allocate shard to first data node
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", dataNodes.get(0))
                .build()
        );
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 50))
                .mapToObj(n -> client().prepareIndex(indexName).setSource("num", n))
                .collect(toList())
        );
        assertAcked(client().admin().indices().prepareClose(indexName).setWaitForActiveShards(ActiveShardCount.ALL));
        // move single shard to second node
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.include._name", dataNodes.get(1)))
            .get();
        ensureGreen(indexName);
        internalCluster().fullRestart();
        ensureGreen(indexName);
        assertIndexIsClosed(indexName);
    }

    public void testResyncPropagatePrimaryTerm() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        final String indexName = "closed_indices_promotion";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build()
        );
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 50))
                .mapToObj(n -> client().prepareIndex(indexName).setSource("num", n))
                .collect(toList())
        );
        ensureGreen(indexName);
        assertAcked(client().admin().indices().prepareClose(indexName));
        assertIndexIsClosed(indexName);
        ensureGreen(indexName);
        String nodeWithPrimary = clusterService().state()
            .nodes()
            .get(clusterService().state().routingTable().index(indexName).shard(0).primaryShard().currentNodeId())
            .getName();
        internalCluster().restartNode(nodeWithPrimary, new InternalTestCluster.RestartCallback());
        ensureGreen(indexName);
        long primaryTerm = clusterService().state().metadata().index(indexName).primaryTerm(0);
        for (String nodeName : internalCluster().nodesInclude(indexName)) {
            IndexShard shard = internalCluster().getInstance(IndicesService.class, nodeName)
                .indexService(resolveIndex(indexName))
                .getShard(0);
            assertThat(shard.routingEntry().toString(), shard.getOperationPrimaryTerm(), equalTo(primaryTerm));
        }
    }

    private static void closeIndices(final String... indices) {
        closeIndices(client().admin().indices().prepareClose(indices));
    }

    private static void closeIndices(final CloseIndexRequestBuilder requestBuilder) {
        final CloseIndexResponse response = requestBuilder.get();
        assertThat(response.isAcknowledged(), is(true));
        assertThat(response.isShardsAcknowledged(), is(true));

        final String[] indices = requestBuilder.request().indices();
        if (indices != null) {
            assertThat(response.getIndices().size(), equalTo(indices.length));
            for (String index : indices) {
                CloseIndexResponse.IndexResult indexResult = response.getIndices()
                    .stream()
                    .filter(result -> index.equals(result.getIndex().getName()))
                    .findFirst()
                    .get();
                assertThat(indexResult, notNullValue());
                assertThat(indexResult.hasFailures(), is(false));
                assertThat(indexResult.getException(), nullValue());
                assertThat(indexResult.getShards(), notNullValue());
                Arrays.stream(indexResult.getShards()).forEach(shardResult -> {
                    assertThat(shardResult.hasFailures(), is(false));
                    assertThat(shardResult.getFailures(), notNullValue());
                    assertThat(shardResult.getFailures().length, equalTo(0));
                });
            }
        } else {
            assertThat(response.getIndices().size(), equalTo(0));
        }
    }

    static void assertIndexIsClosed(final String... indices) {
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (String index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().indices().get(index);
            assertThat(indexMetadata.getState(), is(IndexMetadata.State.CLOSE));
            final Settings indexSettings = indexMetadata.getSettings();
            assertThat(indexSettings.hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(true));
            assertThat(indexSettings.getAsBoolean(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), false), is(true));
            assertThat(clusterState.routingTable().index(index), notNullValue());
            assertThat(clusterState.blocks().hasIndexBlock(index, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
            assertThat(
                "Index " + index + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
                clusterState.blocks()
                    .indices()
                    .getOrDefault(index, emptySet())
                    .stream()
                    .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)
                    .count(),
                equalTo(1L)
            );
        }
    }

    static void assertIndexIsOpened(final String... indices) {
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (String index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().indices().get(index);
            assertThat(indexMetadata.getState(), is(IndexMetadata.State.OPEN));
            assertThat(indexMetadata.getSettings().hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(false));
            assertThat(clusterState.routingTable().index(index), notNullValue());
            assertThat(clusterState.blocks().hasIndexBlock(index, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(false));
        }
    }

    static void assertException(final Throwable throwable, final String indexName) {
        final Throwable t = ExceptionsHelper.unwrapCause(throwable);
        if (t instanceof ClusterBlockException) {
            ClusterBlockException clusterBlockException = (ClusterBlockException) t;
            assertThat(clusterBlockException.blocks(), hasSize(1));
            assertTrue(clusterBlockException.blocks().stream().allMatch(b -> b.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID));
        } else if (t instanceof IndexClosedException) {
            IndexClosedException indexClosedException = (IndexClosedException) t;
            assertThat(indexClosedException.getIndex(), notNullValue());
            assertThat(indexClosedException.getIndex().getName(), equalTo(indexName));
        } else if (t instanceof IndexNotFoundException) {
            IndexNotFoundException indexNotFoundException = (IndexNotFoundException) t;
            assertThat(indexNotFoundException.getIndex(), notNullValue());
            assertThat(indexNotFoundException.getIndex().getName(), equalTo(indexName));
        } else {
            fail("Unexpected exception: " + t);
        }
    }

    void assertNoFileBasedRecovery(String indexName) {
        for (RecoveryState recovery : client().admin().indices().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), empty());
            }
        }
    }
}
