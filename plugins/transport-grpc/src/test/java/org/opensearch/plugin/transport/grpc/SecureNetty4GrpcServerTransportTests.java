/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc;

import io.grpc.BindableService;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.junit.After;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.opensearch.transport.grpc.ssl.SecureNetty4GrpcServerTransport;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

public class SecureNetty4GrpcServerTransportTests extends OpenSearchTestCase {
    private NetworkService networkService;
    private final List<BindableService> services = new ArrayList<>();
    private SecureAuxTransportSettingsProvider settingsProvider;

    private static SecureAuxTransportSettingsProvider getSecureSettingsProvider() {
        return settings -> Optional.of(new SecureAuxTransportSettingsProvider.SecureTransportParameters() {
            @Override
            public boolean dualModeEnabled() {
                return false;
            }

            @Override
            public String sslProvider() {
                return "JDK";
            }

            @Override
            public String clientAuth() {
                return "NONE";
            }

            @Override
            public Iterable<String> protocols() {
                return List.of("TLSv1.3", "TLSv1.2");
            }

            @Override
            public Iterable<String> cipherSuites() {
                /**
                 * Attempt to fetch supported ciphers from default provider.
                 * Else fall back to common defaults.
                 */
                try {
                    SSLContext context = SSLContext.getInstance("TLS");
                    context.init(null, null, null);
                    SSLEngine engine = context.createSSLEngine();
                    return List.of(engine.getSupportedCipherSuites());
                } catch (NoSuchAlgorithmException | KeyManagementException e) {
                    return List.of(
                        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",  // TLSv1.2
                        "TLS_AES_128_GCM_SHA256"                  // TLSv1.3
                    );
                }
            }

            @Override
            public KeyManagerFactory keyManagerFactory() {
                try {
                    final KeyStore keyStore = KeyStore.getInstance("PKCS12");
                    keyStore.load(
                        SecureNetty4GrpcServerTransport.class.getResourceAsStream("/netty4-secure.jks"),
                        "password".toCharArray()
                    );
                    final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                    keyManagerFactory.init(keyStore, "password".toCharArray());
                    return keyManagerFactory;
                } catch (UnrecoverableKeyException |
                         CertificateException |
                         KeyStoreException |
                         IOException |
                         NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public TrustManagerFactory trustManagerFactory() {
                return InsecureTrustManagerFactory.INSTANCE;
            }
        });
    }

    static Settings createSettings() {
        return Settings.builder().put(
                SecureNetty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(),
                getPortRange())
            .build();
    }

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
        settingsProvider = getSecureSettingsProvider();
    }

    @After
    public void shutdown() {
        networkService = null;
    }

    public void testGrpcSecureTransportStartStop() {
        try (SecureNetty4GrpcServerTransport transport = new SecureNetty4GrpcServerTransport(
            createSettings(),
            services,
            networkService,
            settingsProvider
        )) {
            transport.start();
            assertTrue(transport.boundAddress().boundAddresses().length > 0);
            assertNotNull(transport.boundAddress().publishAddress().address());
            transport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testGrpcSecureTransportHealthcheck() {
        try (SecureNetty4GrpcServerTransport transport = new SecureNetty4GrpcServerTransport(
            createSettings(),
            services,
            networkService,
            settingsProvider
        )) {
            transport.start();
            assertTrue(transport.boundAddress().boundAddresses().length > 0);
            assertNotNull(transport.boundAddress().publishAddress().address());
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            try(NettyGrpcClient client = new NettyGrpcClient.Builder()
                .setAddress(remoteAddress)
                .setSecureSettingsProvider(settingsProvider)
                .build()){
                assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
            }
            transport.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
