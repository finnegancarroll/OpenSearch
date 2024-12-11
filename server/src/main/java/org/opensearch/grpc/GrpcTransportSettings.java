/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.grpc;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.transport.PortsRange;

/**
 * Transport settings for gRPC connections
 *
 * @opensearch.internal
 */
public final class GrpcTransportSettings {

    public static final Setting<PortsRange> SETTING_GRPC_PORT = new Setting<>(
        "grpc.port",
        "9400-9500",
        PortsRange::new,
        Property.NodeScope
    );

    private GrpcTransportSettings() {}
}
