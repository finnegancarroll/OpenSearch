/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.interceptor;

import java.io.InputStream;

import io.grpc.MethodDescriptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;

/**
 * Debug wrapper that intercepts marshalling to log thread identity during parse.
 */
public final class DebugHashingMarshaller {

    private DebugHashingMarshaller() {}

    /**
     * Wraps a service definition so parse() logs thread identity.
     */
    public static ServerServiceDefinition wrap(ServerServiceDefinition serviceDef) {
        return ServerInterceptors.useMarshalledMessages(serviceDef, new LoggingMarshaller());
    }

    static final class LoggingMarshaller implements MethodDescriptor.Marshaller<InputStream> {

        @Override
        public InputStream parse(InputStream stream) {
            String threadName = Thread.currentThread().getName();
            if (threadName.contains("[grpc]")) {
                // === BREAKPOINT HERE ===
                System.out.println(
                    "[STAGE-2 marshaller.parse] thread="
                        + threadName
                        + "(id="
                        + Thread.currentThread().getId()
                        + ") — reading bytes off wire"
                );
            }
            return stream;
        }

        @Override
        public InputStream stream(InputStream value) {
            return value;
        }
    }
}
