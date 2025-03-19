/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobuf.ErrorCause;
import org.opensearch.protobuf.NullValue;
import org.opensearch.protobuf.ResponseItem;
import org.opensearch.protobuf.ShardFailure;
import org.opensearch.protobuf.ShardStatistics;

/**
 * Utility class for converting DocWriteResponse objects to Protocol Buffers.
 * This class handles the conversion of document write operation responses (index, create, update, delete)
 * to their Protocol Buffer representation.
 */
public class DocWriteResponseProtoUtils {

    private DocWriteResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a DocWriteResponse to its Protocol Buffer representation.
     * This method is equivalent to the {@link DocWriteResponse#innerToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param response The DocWriteResponse to convert
     * @return A ResponseItem.Builder with the DocWriteResponse data
     *
     */
    public static ResponseItem.Builder toProto(DocWriteResponse response) {
        ResponseItem.Builder responseItem = ResponseItem.newBuilder();

        // Set the index name
        responseItem.setIndex(response.getIndex());

        // Handle document ID (can be null in some cases)
        if (response.getId().isEmpty()) {
            responseItem.setId(ResponseItem.Id.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build());
        } else {
            responseItem.setId(ResponseItem.Id.newBuilder().setString(response.getId()).build());
        }

        // Set document version
        responseItem.setVersion(response.getVersion());

        // Set operation result (CREATED, UPDATED, DELETED, NOT_FOUND, NOOP)
        responseItem.setResult(response.getResult().getLowercase());

        // Set forced refresh flag if applicable
        if (response.forcedRefresh()) {
            responseItem.setForcedRefresh(true);
        }
        // Handle shard information
        ShardStatistics shardStatistics = ShardInfoProtoUtils.convertShardInfoProto(response.getShardInfo());
        responseItem.setShards(shardStatistics);

        // Set sequence number and primary term if available
        if (response.getSeqNo() >= 0) {
            responseItem.setSeqNo(response.getSeqNo());
            responseItem.setPrimaryTerm(response.getPrimaryTerm());
        }

        return responseItem;
    }
}
