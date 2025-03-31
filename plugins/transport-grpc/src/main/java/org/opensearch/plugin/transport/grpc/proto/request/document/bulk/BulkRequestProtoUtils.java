/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.plugin.transport.grpc.proto.request.common.FetchSourceContextProtoUtils;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.WaitForActiveShards;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.document.RestBulkAction;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.client.Requests;

import java.io.IOException;

/**
 * Handler for bulk requests in gRPC.
 */
public class BulkRequestProtoUtils {
//    protected final Settings settings;

    protected BulkRequestProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Prepare the request for execution.
     * Similar to {@link RestBulkAction#prepareRequest(RestRequest, NodeClient)} ()}
     * Please ensure to keep both implementations consistent.
     *
     * @param request the request to execute
     * @return a future of the bulk action that was     executed
     * @throws IOException if an I/O exception occurred parsing the request and preparing for execution
     */
    public static org.opensearch.action.bulk.BulkRequest prepareRequest(BulkRequest request) throws IOException {
        org.opensearch.action.bulk.BulkRequest bulkRequest = Requests.bulkRequest();

        String defaultIndex = request.hasIndex() ? request.getIndex() : null;
        String defaultRouting = request.hasRouting() ? request.getRouting() : null;
        FetchSourceContext defaultFetchSourceContext = FetchSourceContextProtoUtils.parseFromProtoRequest(request);
        String defaultPipeline = request.hasPipeline() ? request.getPipeline() : null;

        bulkRequest = getActiveShardCount(bulkRequest, request); // TODO why is 'bulkRequest' updated by this method

        Boolean defaultRequireAlias = request.hasRequireAlias() ? request.getRequireAlias() : null;

        if (request.hasTimeout()){
            bulkRequest.timeout(request.getTimeout());
        } else {
            bulkRequest.timeout(BulkShardRequest.DEFAULT_TIMEOUT);
        }

        bulkRequest.setRefreshPolicy(getRefreshPolicy(request));

        // Note: batch_size is deprecated in OS 3.x. Add batch_size parameter when backporting to OS 2.x
        /*
        if (request.hasBatchSize()){
            logger.info("The batch size option in bulk API is deprecated and will be removed in 3.0.");
        }
        bulkRequest.batchSize(request.hasBatchSize() ? request.getBatchSize() : Integer.MAX_VALUE);
        */

        bulkRequest.add(BulkRequestParserProtoUtils.getDocWriteRequests(request, defaultIndex, defaultRouting, defaultFetchSourceContext, defaultPipeline, defaultRequireAlias));

        return bulkRequest;
    }

    protected static String getRefreshPolicy(org.opensearch.protobufs.BulkRequest request) {
        if (!request.hasRefresh()){
            return null;
        }
        switch (request.getRefresh()) {
            case REFRESH_TRUE:
                return WriteRequest.RefreshPolicy.IMMEDIATE.getValue();
            case REFRESH_WAIT_FOR:
                return WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue();
            case REFRESH_FALSE:
            case REFRESH_UNSPECIFIED:
            default:
                return WriteRequest.RefreshPolicy.NONE.getValue();
        }
    }

    protected static org.opensearch.action.bulk.BulkRequest getActiveShardCount(org.opensearch.action.bulk.BulkRequest bulkRequest, BulkRequest request){
        if(!request.hasWaitForActiveShards()){
            return bulkRequest;
        }
        WaitForActiveShards waitForActiveShards = request.getWaitForActiveShards();
        switch (waitForActiveShards.getWaitForActiveShardsCase()){
            case WaitForActiveShards.WaitForActiveShardsCase.WAIT_FOR_ACTIVE_SHARD_OPTIONS:
                switch (waitForActiveShards.getWaitForActiveShardOptions()) {
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED:
                        throw new UnsupportedOperationException("No mapping for WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED");
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_ALL:
                       bulkRequest.waitForActiveShards(ActiveShardCount.ALL);
                       break;
                    default:
                        bulkRequest.waitForActiveShards(ActiveShardCount.DEFAULT);
                        break;
                }
                break;
            case WaitForActiveShards.WaitForActiveShardsCase.INT32_VALUE:
                bulkRequest.waitForActiveShards(waitForActiveShards.getInt32Value());
                break;
            default:
                throw new UnsupportedOperationException("No mapping for WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED");
        }
        return bulkRequest;
    }
}
