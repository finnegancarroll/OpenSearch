/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.interceptor;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Debug interceptor that logs thread identity at each stage of gRPC request processing.
 * Used to verify that interceptCall, parse, and onMessage execute on the same thread.
 */
public class DebugThreadInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        Metadata headers,
        ServerCallHandler<ReqT, RespT> next
    ) {
        String method = call.getMethodDescriptor().getFullMethodName();
        long threadId = Thread.currentThread().getId();
        String threadName = Thread.currentThread().getName();

        // === BREAKPOINT HERE ===
        System.out.println("[STAGE-1 interceptCall] method=" + method + " thread=" + threadName + "(id=" + threadId + ")");

        ServerCall.Listener<ReqT> listener = next.startCall(call, headers);

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            @Override
            public void onMessage(ReqT message) {
                long onMsgThreadId = Thread.currentThread().getId();
                String onMsgThreadName = Thread.currentThread().getName();

                // === BREAKPOINT HERE ===
                System.out.println(
                    "[STAGE-3 onMessage] method="
                        + method
                        + " thread="
                        + onMsgThreadName
                        + "(id="
                        + onMsgThreadId
                        + ") sameAsInterceptCall="
                        + (onMsgThreadId == threadId)
                );

                super.onMessage(message);
            }
        };
    }
}
