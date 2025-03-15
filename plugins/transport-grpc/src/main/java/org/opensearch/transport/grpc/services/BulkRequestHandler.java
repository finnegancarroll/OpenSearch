/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.VersionType;
import org.opensearch.protobuf.BulkRequestBody;
import org.opensearch.protobuf.CreateOperation;
import org.opensearch.protobuf.DeleteOperation;
import org.opensearch.protobuf.IndexOperation;
import org.opensearch.protobuf.Script;
import org.opensearch.protobuf.UpdateOperation;
import org.opensearch.protobuf.WaitForActiveShards;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.document.RestBulkAction;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.proto.BulkResponseProtoUtils;
import org.opensearch.transport.grpc.proto.FetchSourceContextProtoUtils;

import java.io.IOException;

/**
 * Handler for bulk requests in gRPC.
 */
public class BulkRequestHandler {  // todo extend some common BaseGrpcHandler
    protected static Logger logger = LogManager.getLogger(BulkRequestHandler.class);

    private final NodeClient client;

    /**
     *
     * @param client: Client for executing actions on the local node
     */
    public BulkRequestHandler(NodeClient client) {
        this.client = client;
    }

    public org.opensearch.protobuf.BulkResponse executeRequest(org.opensearch.protobuf.BulkRequest request) throws IOException {
        return BulkResponseProtoUtils.toProto(client.bulk(prepareRequest(request)).actionGet());
    }

    /**
     * Prepare the request for execution.
     *
     * Similar to {@link RestBulkAction#prepareRequest(RestRequest, NodeClient)} ()}
     * Please ensure to keep both implementations consistent.
     *
     * @param request the request to execute
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and preparing for execution
     */
    public org.opensearch.action.bulk.BulkRequest prepareRequest(org.opensearch.protobuf.BulkRequest request) throws IOException {

        org.opensearch.action.bulk.BulkRequest bulkRequest = new org.opensearch.action.bulk.BulkRequest();

        bulkRequest.pipeline(request.getPipeline());

        switch (request.getRefresh()) {
            case REFRESH_TRUE:
                bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                break;
            case REFRESH_WAIT_FOR:
                bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
                break;
            case REFRESH_FALSE:
            case REFRESH_UNSPECIFIED:
            default:
                bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
                break;
        }

        if (request.hasRequireAlias()) {
            bulkRequest.requireAlias(request.getRequireAlias());
        }

        if (request.hasRouting()) {
            bulkRequest.routing(request.getRouting());
        }

        if (request.hasTimeout()) {
            // TODO add validation for regex?
            bulkRequest.timeout(request.getTimeout());
        }

        if (request.hasWaitForActiveShards()) {
            if (request.getWaitForActiveShards()
                .getWaitForActiveShardsCase() == WaitForActiveShards.WaitForActiveShardsCase.WAIT_FOR_ACTIVE_SHARD_OPTIONS) {
                switch (request.getWaitForActiveShards().getWaitForActiveShardOptions()) {
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_INVALID:
                        throw new UnsupportedOperationException("No mapping for WAIT_FOR_ACTIVE_SHARD_OPTIONS_UNSPECIFIED");
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_ALL:
                        bulkRequest.waitForActiveShards(ActiveShardCount.ALL);
                        break;
                    case WAIT_FOR_ACTIVE_SHARD_OPTIONS_INDEX_SETTING:
                        throw new UnsupportedOperationException("WAIT_FOR_ACTIVE_SHARD_OPTIONS_INDEX_SETTING does not map to anything");  // TODO what
                                                                                                                                          // does
                                                                                                                                          // this
                                                                                                                                          // correspond
                                                                                                                                          // to?
                    default:
                        bulkRequest.waitForActiveShards(ActiveShardCount.DEFAULT);
                        break;
                }
            } else if (request.getWaitForActiveShards()
                .getWaitForActiveShardsCase() == WaitForActiveShards.WaitForActiveShardsCase.INT32_VALUE) {
                    bulkRequest.waitForActiveShards(request.getWaitForActiveShards().getInt32Value());
                }
        }

        // todo skipping this field since deprecated
        // [deprecated] optional string type = 6;

        // [required] The request body contains create, delete, index, and update actions and their associated source data
        // repeated BulkRequestBody request_body = 8;
        for (BulkRequestBody bulkRequestBodyEntry : request.getRequestBodyList()) {
            switch (bulkRequestBodyEntry.getOperationContainerCase()) {
                // TODO remove toBytes
                case INDEX:
                    bulkRequest.add(buildIndexRequest(bulkRequestBodyEntry.getIndex(), bulkRequestBodyEntry.getDoc().toByteArray()));
                    break;
                case CREATE:
                    bulkRequest.add(buildCreateRequest(bulkRequestBodyEntry.getCreate(), bulkRequestBodyEntry.getDoc().toByteArray()));
                    break;
                case UPDATE:
                    bulkRequest.add(
                        buildUpdateRequest(
                            bulkRequestBodyEntry.getUpdate(),
                            bulkRequestBodyEntry.getDoc().toByteArray(),
                            bulkRequestBodyEntry
                        )
                    );
                    break;
                case DELETE:
                    bulkRequest.add(buildDeleteRequest(bulkRequestBodyEntry.getDelete()));
                    break;
                case OPERATIONCONTAINER_NOT_SET:
                    break;
            }
        }
        return bulkRequest;
    }

    private static IndexRequest buildCreateRequest(CreateOperation createOperation, byte[] document) {
        IndexRequest indexRequest = new IndexRequest();

        if (!createOperation.hasIndex()) {
            throw new IllegalArgumentException("Create operation must contain index field");
        }
        indexRequest.index(createOperation.getIndex());

        if (createOperation.hasId()) {
            indexRequest.id(createOperation.getId());
        }
        if (createOperation.hasRouting()) {
            indexRequest.routing(createOperation.getRouting());
        }

        // todo set dynamic_templates
        // for (Map<String, String> dynamic_template_entry : indexOperation.getDynamicTemplatesMap()) {
        //
        // }
        if (createOperation.hasPipeline()) {
            indexRequest.setPipeline(createOperation.getPipeline());
        }
        if (createOperation.hasRequireAlias()) {
            indexRequest.setRequireAlias(createOperation.getRequireAlias());
        }

        indexRequest.source(document, MediaTypeRegistry.JSON);
        // indexRequest.source("key1", "value1", "key2", "value2"); // temp for testing
        indexRequest.opType(DocWriteRequest.OpType.CREATE);

        return indexRequest;
    }

    // TODO check default values set correctly for all
    private static IndexRequest buildIndexRequest(IndexOperation indexOperation, byte[] document) {
        IndexRequest indexRequest = new IndexRequest();

        if (!indexOperation.hasIndex()) {
            throw new IllegalArgumentException("Index operation must contain index field");
        }
        indexRequest.index(indexOperation.getIndex());

        if (indexOperation.hasId()) {
            indexRequest.id(indexOperation.getId());
        }
        if (indexOperation.hasRouting()) {
            indexRequest.routing(indexOperation.getRouting());
        }
        if (indexOperation.hasIfPrimaryTerm()) {
            indexRequest.setIfPrimaryTerm(indexOperation.getIfPrimaryTerm());
        }
        if (indexOperation.hasIfSeqNo()) {
            indexRequest.setIfSeqNo(indexOperation.getIfSeqNo());
        }
        if (indexOperation.hasVersion()) {
            indexRequest.version(indexOperation.getVersion());
        }
        switch (indexOperation.getVersionType()) {
            case VERSION_TYPE_EXTERNAL:
                indexRequest.versionType(VersionType.EXTERNAL);
                break;
            case VERSION_TYPE_EXTERNAL_GTE:
                indexRequest.versionType(VersionType.EXTERNAL_GTE);
                break;
            default:
                indexRequest.versionType(VersionType.INTERNAL);
                break;
        }
        // todo set dynamic_templates
        // for (Map<String, String> dynamic_template_entry : indexOperation.getDynamicTemplatesMap()) {
        //
        // }
        if (indexOperation.hasPipeline()) {
            indexRequest.setPipeline(indexOperation.getPipeline());
        }
        if (indexOperation.hasRequireAlias()) {
            indexRequest.setRequireAlias(indexOperation.getRequireAlias());
        }

        indexRequest.source(document, MediaTypeRegistry.JSON);
        indexRequest.opType(DocWriteRequest.OpType.INDEX);

        return indexRequest;
    }

    private static UpdateRequest buildUpdateRequest(UpdateOperation updateOperation, byte[] document, BulkRequestBody bulkRequestBody) {
        UpdateRequest updateRequest = new UpdateRequest();

        // TODO add `has*()` wrappers to correctly set default values
        if (!updateOperation.hasIndex()) {
            throw new IllegalArgumentException("Update operation must contain index field");
        }
        updateRequest.index(updateOperation.getIndex());

        if (!updateOperation.hasId()) {
            throw new IllegalArgumentException("Update operation must contain id field");
        }
        updateRequest.id(updateOperation.getId());

        if (updateOperation.hasRouting()) {
            updateRequest.routing(updateOperation.getRouting());
        }
        if (updateOperation.hasIfPrimaryTerm()) {
            updateRequest.setIfPrimaryTerm(updateOperation.getIfPrimaryTerm());
        }
        if (updateOperation.hasIfSeqNo()) {
            updateRequest.setIfSeqNo(updateOperation.getIfSeqNo());
        }
        if (updateOperation.hasVersion()) {
            updateRequest.version(updateOperation.getVersion());
        }
        if (updateOperation.hasRequireAlias()) {
            updateRequest.setRequireAlias(updateOperation.getRequireAlias());
        }
        if (bulkRequestBody.hasDocAsUpsert()) {
            updateRequest.docAsUpsert(bulkRequestBody.getDocAsUpsert());
        }
        if (bulkRequestBody.hasScript()) {
            Script protoScript = bulkRequestBody.getScript();
            if (bulkRequestBody.getScript().hasInlineScript()) {
                // todo support InlineScript
            } else {
                // todo pass params too
                org.opensearch.script.Script script = new org.opensearch.script.Script(protoScript.getStoredScriptId().getId());
                updateRequest.script(script);
            }
        }

        updateRequest.doc(document, MediaTypeRegistry.JSON);

        if (bulkRequestBody.hasScriptedUpsert()) {
            updateRequest.scriptedUpsert(bulkRequestBody.getScriptedUpsert());
        }

        if (bulkRequestBody.hasSource()) {
            org.opensearch.search.fetch.subphase.FetchSourceContext fetchSourceContext = FetchSourceContextProtoUtils.fromProto(
                bulkRequestBody.getSource()
            );
            updateRequest.fetchSource(fetchSourceContext);
        }

        if (bulkRequestBody.hasUpsert()) {
            updateRequest.upsert(bulkRequestBody.getUpsert(), MediaTypeRegistry.JSON);
        }

        return updateRequest;
    }

    private static DeleteRequest buildDeleteRequest(DeleteOperation deleteOperation) {
        DeleteRequest deleteRequest = new DeleteRequest();

        if (!deleteOperation.hasIndex()) {
            throw new IllegalArgumentException("Delete operation must contain index field");
        }
        deleteRequest.index(deleteOperation.getIndex());

        if (!deleteOperation.hasId()) {
            throw new IllegalArgumentException("Delete operation must contain ID field");
        }
        deleteRequest.id(deleteOperation.getId());

        if (deleteOperation.hasRouting()) {
            deleteRequest.routing(deleteOperation.getRouting());
        }
        if (deleteOperation.hasIfPrimaryTerm()) {
            deleteRequest.setIfPrimaryTerm(deleteOperation.getIfPrimaryTerm());
        }
        if (deleteOperation.hasIfSeqNo()) {
            deleteRequest.setIfSeqNo(deleteOperation.getIfSeqNo());
        }
        if (deleteOperation.hasVersion()) {
            deleteRequest.version(deleteOperation.getVersion());
        }

        switch (deleteOperation.getVersionType()) {
            case VERSION_TYPE_EXTERNAL:
                deleteRequest.versionType(VersionType.EXTERNAL);
                break;
            case VERSION_TYPE_EXTERNAL_GTE:
                deleteRequest.versionType(VersionType.EXTERNAL_GTE);
                break;
            default:
                deleteRequest.versionType(VersionType.INTERNAL);
                break;
        }

        return deleteRequest;
    }
}
