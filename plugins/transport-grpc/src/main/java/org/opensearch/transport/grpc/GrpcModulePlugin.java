/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc;

import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.auxiliary.AuxTransportSettings.SETTING_AUX_BIND_HOST;
import static org.opensearch.auxiliary.AuxTransportSettings.SETTING_AUX_HOST;
import static org.opensearch.auxiliary.AuxTransportSettings.SETTING_AUX_PUBLISH_HOST;

/**
 * Main class for the gRPC plugin
 */
public final class GrpcModulePlugin extends Plugin implements NetworkPlugin {
    public static final String GRPC_TRANSPORT_NAME = "grpc-transport";

    public static final Setting<Integer> SETTING_GRPC_WORKER_COUNT = new Setting<>(
        "grpc.netty.worker_count",
        (s) -> Integer.toString(OpenSearchExecutors.allocatedProcessors(s)),
        (s) -> Setting.parseInt(s, 1, "grpc.netty.worker_count"),
        Setting.Property.NodeScope
    );

    @Override
    public Map<String, Supplier<LifecycleComponent>> getAuxTransports(
        Settings settings,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        NetworkService networkService,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        if (FeatureFlags.isEnabled(FeatureFlags.GRPC_ENABLE_SETTING) == false) {
            throw new IllegalArgumentException("transport-grpc is experimental and feature flag must be enabled before use");
        }
        return Collections.singletonMap(
            GRPC_TRANSPORT_NAME,
            () -> new Netty4GrpcServerTransport(settings, Collections.emptyList(), networkService)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SETTING_AUX_HOST, SETTING_AUX_PUBLISH_HOST, SETTING_AUX_BIND_HOST, SETTING_GRPC_WORKER_COUNT);
    }
}