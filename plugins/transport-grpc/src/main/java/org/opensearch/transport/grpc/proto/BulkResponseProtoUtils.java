/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto;

import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.protobuf.BulkResponseBody;

import java.io.IOException;

/**
 * Utility class for converting BulkResponse objects to Protocol Buffers.
 * This class handles the conversion of bulk operation responses to their
 * Protocol Buffer representation.
 */
public class BulkResponseProtoUtils {

    private BulkResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a BulkResponse to its Protocol Buffer representation.
     * This method is equivalent to the toXContent method in BulkResponse.
     *
     * @param response The BulkResponse to convert
     * @return A Protocol Buffer BulkResponse representation
     * @throws IOException if there's an error during conversion
     */
    public static org.opensearch.protobuf.BulkResponse toProto(BulkResponse response) throws IOException {
        org.opensearch.protobuf.BulkResponse.Builder bulkResponse = org.opensearch.protobuf.BulkResponse.newBuilder();

        // Create the bulk response body
        BulkResponseBody.Builder bulkResponseBody = BulkResponseBody.newBuilder();

        // Set the time taken for the bulk operation (excluding ingest preprocessing)
        bulkResponseBody.setTook(response.getTook().getMillis());

        // Set ingest preprocessing time if available
        if (response.getIngestTookInMillis() != BulkResponse.NO_INGEST_TOOK) {
            bulkResponseBody.setIngestTook(response.getIngestTookInMillis());
        }

        // Set whether any operations failed
        bulkResponseBody.setErrors(response.hasFailures());

        // Add individual item responses for each operation in the bulk request
        for (BulkItemResponse bulkItemResponse : response.getItems()) {
            bulkResponseBody.addItems(BulkItemResponseProtoUtils.toProto(bulkItemResponse));
        }

        // Set the bulk response body and build the final response
        bulkResponse.setBulkResponseBody(bulkResponseBody.build());
        return bulkResponse.build();
    }
}
