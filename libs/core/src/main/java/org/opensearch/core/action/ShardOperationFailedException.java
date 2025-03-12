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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.core.action;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;

import java.util.Objects;

/**
 * An exception indicating that a failure occurred performing an operation on the shard.
 *
 * @opensearch.internal
 */
public abstract class ShardOperationFailedException implements Writeable, ToXContentObject {

    protected String index;
    protected int shardId = -1;
    protected String reason;
    protected RestStatus status;
    protected Throwable cause;

    protected ShardOperationFailedException() {

    }

    protected ShardOperationFailedException(@Nullable String index, int shardId, String reason, RestStatus status, Throwable cause) {
        this.index = index;
        this.shardId = shardId;
        this.reason = Objects.requireNonNull(reason, "reason cannot be null");
        this.status = Objects.requireNonNull(status, "status cannot be null");
        this.cause = Objects.requireNonNull(cause, "cause cannot be null");
    }

    /**
     * The index the operation failed on. Might return {@code null} if it can't be derived.
     */
    @Nullable
    public final String index() {
        return index;
    }

    /**
     * The index the operation failed on. Might return {@code -1} if it can't be derived.
     */
    public final int shardId() {
        return shardId;
    }

    /**
     * The reason of the failure.
     */
    public final String reason() {
        return reason;
    }

    /**
     * The status of the failure.
     */
    public final RestStatus status() {
        return status;
    }

    /**
     * The GRPC status of the failure.
     */
    // public final Status grpcStatus() {
    //     return grpcStatus;
    // }

    /**
     * The cause of this failure
     */
    public final Throwable getCause() {
        return cause;
    }
}
