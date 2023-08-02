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

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.transport.BoundTransportAddress;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * OpenSearch Transport Interface
 *
 * @opensearch.internal
 */
public interface Transport extends LifecycleComponent {

    /**
     * Registers a new request handler
     */
    default <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        getRequestHandlers().registerHandler(reg);
    }

    void setMessageListener(TransportMessageListener listener);

    /**
     * Registers a new request handler
    */
    default <Request extends TransportRequest> void registerProtobufRequestHandler(ProtobufRequestHandlerRegistry<Request> reg) {
        getProtobufRequestHandlers().registerHandler(reg);
    }

    // void setMessageListenerProtobuf(ProtobufTransportMessageListener listener);

    default void setSlowLogThreshold(TimeValue slowLogThreshold) {}

    default boolean isSecure() {
        return false;
    }

    /**
     * The address the transport is bound on.
     */
    BoundTransportAddress boundAddress();

    /**
     * Further profile bound addresses
     * @return <code>null</code> iff profiles are unsupported, otherwise a map with name of profile and its bound transport address
     */
    Map<String, BoundTransportAddress> profileBoundAddresses();

    /**
     * Returns an address from its string representation.
     */
    TransportAddress[] addressesFromString(String address) throws UnknownHostException;

    /**
     * Returns a list of all local addresses for this transport
     */
    List<String> getDefaultSeedAddresses();

    /**
     * Opens a new connection to the given node. When the connection is fully connected, the listener is called.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     */
    void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener);

    TransportStats getStats();

    ResponseHandlers getResponseHandlers();

    RequestHandlers getRequestHandlers();

    ProtobufRequestHandlers getProtobufRequestHandlers();

    /**
     * A unidirectional connection to a {@link DiscoveryNode}
     */
    interface Connection extends Closeable {
        /**
         * The node this connection is associated with
         */
        DiscoveryNode getNode();

        /**
         * Sends the request to the node this connection is associated with
         * @param requestId see {@link ResponseHandlers#add(ResponseContext)} for details
         * @param action the action to execute
         * @param request the request to send
         * @param options request options to apply
         * @throws NodeNotConnectedException if the given node is not connected
         */
        void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException,
            TransportException;

        // /**
        // * Sends the request to the node this connection is associated with
        // * @param requestId see {@link ResponseHandlers#add(ResponseContext)} for details
        // * @param action the action to execute
        // * @param request the request to send
        // * @param options request options to apply
        // * @throws NodeNotConnectedException if the given node is not connected
        // */
        // void sendRequestProtobuf(long requestId, String action, TransportRequest request, TransportRequestOptions options)
        // throws IOException, TransportException;

        /**
         * The listener's {@link ActionListener#onResponse(Object)} method will be called when this
         * connection is closed. No implementations currently throw an exception during close, so
         * {@link ActionListener#onFailure(Exception)} will not be called.
         *
         * @param listener to be called
         */
        void addCloseListener(ActionListener<Void> listener);

        boolean isClosed();

        /**
         * Returns the version of the node this connection was established with.
         */
        default Version getVersion() {
            return getNode().getVersion();
        }

        /**
         * Returns a key that this connection can be cached on. Delegating subclasses must delegate method call to
         * the original connection.
         */
        default Object getCacheKey() {
            return this;
        }

        @Override
        void close();
    }

    // /**
    // * A unidirectional connection to a {@link DiscoveryNode}
    // */
    // interface ProtobufConnection extends Closeable {
    // /**
    // * The node this connection is associated with
    // */
    // DiscoveryNode getNode();

    // /**
    // * Sends the request to the node this connection is associated with
    // * @param requestId see {@link ResponseHandlers#add(ResponseContext)} for details
    // * @param action the action to execute
    // * @param request the request to send
    // * @param options request options to apply
    // * @throws NodeNotConnectedException if the given node is not connected
    // */
    // void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
    // throws IOException, TransportException;

    // /**
    // * The listener's {@link ActionListener#onResponse(Object)} method will be called when this
    // * connection is closed. No implementations currently throw an exception during close, so
    // * {@link ActionListener#onFailure(Exception)} will not be called.
    // *
    // * @param listener to be called
    // */
    // void addCloseListener(ActionListener<Void> listener);

    // boolean isClosed();

    // /**
    // * Returns the version of the node this connection was established with.
    // */
    // default Version getVersion() {
    // return getNode().getVersion();
    // }

    // /**
    // * Returns a key that this connection can be cached on. Delegating subclasses must delegate method call to
    // * the original connection.
    // */
    // default Object getCacheKey() {
    // return this;
    // }

    // @Override
    // void close();
    // }

    /**
     * This class represents a response context that encapsulates the actual response handler, the action and the connection it was
     * executed on.
     */
    final class ResponseContext<T extends TransportResponse> {

        private final TransportResponseHandler<T> handler;

        private final Connection connection;

        private final String action;

        ResponseContext(TransportResponseHandler<T> handler, Connection connection, String action) {
            this.handler = handler;
            this.connection = connection;
            this.action = action;
        }

        public TransportResponseHandler<T> handler() {
            return handler;
        }

        public Connection connection() {
            return this.connection;
        }

        public String action() {
            return this.action;
        }
    }

    // /**
    // * This class represents a response context that encapsulates the actual response handler, the action and the connection it was
    // * executed on.
    // */
    // final class ProtobufResponseContext<T extends TransportResponse> {

    // private final ProtobufTransportResponseHandler<T> handler;

    // private final Connection connection;

    // private final String action;

    // ProtobufResponseContext(ProtobufTransportResponseHandler<T> handler, Connection connection, String action) {
    // this.handler = handler;
    // this.connection = connection;
    // this.action = action;
    // }

    // public ProtobufTransportResponseHandler<T> handler() {
    // return handler;
    // }

    // public Connection connection() {
    // return this.connection;
    // }

    // public String action() {
    // return this.action;
    // }
    // }

    /**
     * This class is a registry that allows
     */
    final class ResponseHandlers {
        private final ConcurrentMapLong<ResponseContext<? extends TransportResponse>> handlers = ConcurrentCollections
            .newConcurrentMapLongWithAggressiveConcurrency();
        private final AtomicLong requestIdGenerator = new AtomicLong();

        /**
         * Returns <code>true</code> if the give request ID has a context associated with it.
         */
        public boolean contains(long requestId) {
            return handlers.containsKey(requestId);
        }

        /**
         * Removes and return the {@link ResponseContext} for the given request ID or returns
         * <code>null</code> if no context is associated with this request ID.
         */
        public ResponseContext<? extends TransportResponse> remove(long requestId) {
            return handlers.remove(requestId);
        }

        /**
         * Adds a new response context and associates it with a new request ID.
         * @return the new request ID
         * @see Connection#sendRequest(long, String, TransportRequest, TransportRequestOptions)
         */
        public long add(ResponseContext<? extends TransportResponse> holder) {
            long requestId = newRequestId();
            ResponseContext<? extends TransportResponse> existing = handlers.put(requestId, holder);
            assert existing == null : "request ID already in use: " + requestId;
            return requestId;
        }

        /**
         * Returns a new request ID to use when sending a message via {@link Connection#sendRequest(long, String,
         * TransportRequest, TransportRequestOptions)}
         */
        long newRequestId() {
            return requestIdGenerator.incrementAndGet();
        }

        /**
         * Removes and returns all {@link ResponseContext} instances that match the predicate
         */
        public List<ResponseContext<? extends TransportResponse>> prune(Predicate<ResponseContext<? extends TransportResponse>> predicate) {
            final List<ResponseContext<? extends TransportResponse>> holders = new ArrayList<>();
            for (Map.Entry<Long, ResponseContext<? extends TransportResponse>> entry : handlers.entrySet()) {
                ResponseContext<? extends TransportResponse> holder = entry.getValue();
                if (predicate.test(holder)) {
                    ResponseContext<? extends TransportResponse> remove = handlers.remove(entry.getKey());
                    if (remove != null) {
                        holders.add(holder);
                    }
                }
            }
            return holders;
        }

        /**
         * called by the {@link Transport} implementation when a response or an exception has been received for a previously
         * sent request (before any processing or deserialization was done). Returns the appropriate response handler or null if not
         * found.
         */
        public TransportResponseHandler<? extends TransportResponse> onResponseReceived(
            final long requestId,
            final TransportMessageListener listener
        ) {
            ResponseContext<? extends TransportResponse> context = handlers.remove(requestId);
            listener.onResponseReceived(requestId, context);
            if (context == null) {
                return null;
            } else {
                return context.handler();
            }
        }
    }

    // /**
    // * This class is a registry that allows
    // */
    // final class ProtobufResponseHandlers {
    // private final ConcurrentMapLong<ProtobufResponseContext<? extends TransportResponse>> handlers = ConcurrentCollections
    // .newConcurrentMapLongWithAggressiveConcurrency();
    // private final AtomicLong requestIdGenerator = new AtomicLong();

    // /**
    // * Returns <code>true</code> if the give request ID has a context associated with it.
    // */
    // public boolean contains(long requestId) {
    // return handlers.containsKey(requestId);
    // }

    // /**
    // * Removes and return the {@link ResponseContext} for the given request ID or returns
    // * <code>null</code> if no context is associated with this request ID.
    // */
    // public ProtobufResponseContext<? extends TransportResponse> remove(long requestId) {
    // return handlers.remove(requestId);
    // }

    // /**
    // * Adds a new response context and associates it with a new request ID.
    // * @return the new request ID
    // * @see Connection#sendRequestProtobuf(long, String, TransportRequest, TransportRequestOptions)
    // */
    // public long add(ProtobufResponseContext<? extends TransportResponse> holder) {
    // long requestId = newRequestId();
    // ProtobufResponseContext<? extends TransportResponse> existing = handlers.put(requestId, holder);
    // assert existing == null : "request ID already in use: " + requestId;
    // return requestId;
    // }

    // /**
    // * Returns a new request ID to use when sending a message via {@link Connection#sendRequestProtobuf(long, String,
    // * TransportRequest, TransportRequestOptions)}
    // */
    // long newRequestId() {
    // return requestIdGenerator.incrementAndGet();
    // }

    // /**
    // * Removes and returns all {@link ResponseContext} instances that match the predicate
    // */
    // public List<ProtobufResponseContext<? extends TransportResponse>> prune(
    // Predicate<ProtobufResponseContext<? extends TransportResponse>> predicate
    // ) {
    // final List<ProtobufResponseContext<? extends TransportResponse>> holders = new ArrayList<>();
    // for (Map.Entry<Long, ProtobufResponseContext<? extends TransportResponse>> entry : handlers.entrySet()) {
    // ProtobufResponseContext<? extends TransportResponse> holder = entry.getValue();
    // if (predicate.test(holder)) {
    // ProtobufResponseContext<? extends TransportResponse> remove = handlers.remove(entry.getKey());
    // if (remove != null) {
    // holders.add(holder);
    // }
    // }
    // }
    // return holders;
    // }

    // /**
    // * called by the {@link Transport} implementation when a response or an exception has been received for a previously
    // * sent request (before any processing or deserialization was done). Returns the appropriate response handler or null if not
    // * found.
    // */
    // public ProtobufTransportResponseHandler<? extends TransportResponse> onResponseReceived(
    // final long requestId,
    // final ProtobufTransportMessageListener listener
    // ) {
    // ProtobufResponseContext<? extends TransportResponse> context = handlers.remove(requestId);
    // listener.onResponseReceived(requestId, context);
    // if (context == null) {
    // return null;
    // } else {
    // return context.handler();
    // }
    // }
    // }

    /**
     * Request handler implementations
     *
     * @opensearch.internal
     */
    final class RequestHandlers {

        private volatile Map<String, RequestHandlerRegistry<? extends TransportRequest>> requestHandlers = Collections.emptyMap();

        private volatile Map<String, ProtobufRequestHandlerRegistry<? extends TransportRequest>> protobufRequestHandlers = Collections
            .emptyMap();

        synchronized <Request extends TransportRequest> void registerHandler(RequestHandlerRegistry<Request> reg) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }

        // TODO: Only visible for testing. Perhaps move StubbableTransport from
        // org.opensearch.test.transport to org.opensearch.transport
        public synchronized <Request extends TransportRequest> void forceRegister(RequestHandlerRegistry<Request> reg) {
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }

        @SuppressWarnings("unchecked")
        public <T extends TransportRequest> RequestHandlerRegistry<T> getHandler(String action) {
            return (RequestHandlerRegistry<T>) requestHandlers.get(action);
        }
    }

    /**
     * Request handler implementations
    *
    * @opensearch.internal
    */
    final class ProtobufRequestHandlers {

        private volatile Map<String, ProtobufRequestHandlerRegistry<? extends TransportRequest>> requestHandlers = Collections.emptyMap();

        synchronized <Request extends TransportRequest> void registerHandler(ProtobufRequestHandlerRegistry<Request> reg) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }

        // TODO: Only visible for testing. Perhaps move StubbableTransport from
        // org.opensearch.test.transport to org.opensearch.transport
        public synchronized <Request extends TransportRequest> void forceRegister(ProtobufRequestHandlerRegistry<Request> reg) {
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }

        @SuppressWarnings("unchecked")
        public <T extends TransportRequest> ProtobufRequestHandlerRegistry<T> getHandler(String action) {
            return (ProtobufRequestHandlerRegistry<T>) requestHandlers.get(action);
        }
    }
}
