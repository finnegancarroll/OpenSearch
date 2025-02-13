/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import io.grpc.health.v1.HealthCheckResponse;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.util.List;

import io.grpc.BindableService;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;

public class Netty4GrpcServerTransportTests extends OpenSearchTestCase {
    private NetworkService networkService;
    private List<BindableService> services;

    @Before
    public void setup() {
        networkService = new NetworkService(List.of());
        services = List.of();
    }

    public void testGrpcTransportStartStop() {
        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(createSettings(), services, networkService)) {
            transport.start();
            MatcherAssert.assertThat(transport.boundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(transport.boundAddress().publishAddress().address());
            transport.stop();
        }
    }

    public void testGrpcTransportHealthcheck() {
        try (Netty4GrpcServerTransport serverTransport = new Netty4GrpcServerTransport(
            createSettings(),
            services,
            networkService
        )) {
            serverTransport.start();
            final TransportAddress remoteAddress = randomFrom(serverTransport.boundAddress().boundAddresses());
            try(NettyGrpcClient client = new NettyGrpcClient.Builder()
                .setAddress(remoteAddress)
                .build()){
                assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
            }
            serverTransport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Settings createSettings() {
        return Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), getPortRange()).build();
    }
}
