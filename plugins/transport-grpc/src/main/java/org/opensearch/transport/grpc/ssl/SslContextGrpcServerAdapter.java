/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.ssl;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider.SecureAuxTransportParameters;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.JdkSslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SupportedCipherSuiteFilter;

/**
 * An io.grpc.SslContext which wraps and delegates functionality to an internal io.netty.handler.ssl.JdkSslContext.
 * As this ssl context is provided for the gRPC aux transport it operates in server mode always.
 */
public class SslContextGrpcServerAdapter extends SslContext {
    private final JdkSslContext ctxt;
    private final SecureAuxTransportParameters params;
    static final String[] DEFAULT_SSL_PROTOCOLS = { "TLSv1.3", "TLSv1.2", "TLSv1.1" };

    /**
     * Initializes a new SslContextGrpcServerAdapter from SecureAuxTransportSettingsProvider.
     * @param provider provides SSLContext with additional client auth mode and enabled cipher suites.
     */
    public SslContextGrpcServerAdapter(SecureAuxTransportSettingsProvider provider) throws NoSuchAlgorithmException,
        KeyManagementException {
        SSLContext sslContext = provider.buildSSLContext().orElseThrow(IllegalArgumentException::new);
        this.params = provider.parameters().orElseThrow(IllegalArgumentException::new);
        ctxt = new JdkSslContext(
            sslContext,
            isClient(),
            Arrays.asList(getEnabledCipherSuites()),
            SupportedCipherSuiteFilter.INSTANCE,
            new ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                ApplicationProtocolNames.HTTP_2 // gRPC -> always http2
            ),
            getClientAuthMode(),
            DEFAULT_SSL_PROTOCOLS,
            true
        );
    }

    /**
     * Fetch client auth mode from SecureAuxTransportParameters.
     */
    private ClientAuth getClientAuthMode() {
        if (params.clientAuth().isEmpty()) {
            throw new OpenSearchSecurityException("No client auth provided");
        }
        switch (params.clientAuth().orElseThrow(IllegalArgumentException::new)) {
            case "NONE" -> {
                return ClientAuth.NONE;
            }
            case "OPTIONAL" -> {
                return ClientAuth.OPTIONAL;
            }
            case "REQUIRE" -> {
                return ClientAuth.REQUIRE;
            }
            default -> throw new OpenSearchSecurityException("Invalid client auth provided: " + params.clientAuth());
        }
    }

    /**
     * Fetch enabled cipher suites from SecureAuxTransportParameters.
     * Subset of supported SSLContext cipher suites.
     * @return cipher suites
     */
    private String[] getEnabledCipherSuites() {
        List<String> ciphers = (List<String>) params.cipherSuites();
        return ciphers.toArray(new String[0]);
    }

    /*
      Mirror the io.grpc.netty.shaded.io.netty.handler.ssl API with our delegate.
     */

    /**
     * @return server only context - always false.
     */
    @Override
    public boolean isClient() {
        return false;
    }

    /**
     * Create a new SSLEngine instance to handle TLS for a connection.
     * @param byteBufAllocator netty allocator - unused by delegate.
     * @return new SSLEngine instance.
     */
    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator) {
        return ctxt.newEngine(byteBufAllocator);
    }

    /**
     * Create a new SSLEngine instance to handle TLS for a connection.
     * @param byteBufAllocator netty allocator - unused by delegate.
     * @param s host hint.
     * @param i port hint.
     * @return new SSLEngine instance.
     */
    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator, String s, int i) {
        return ctxt.newEngine(byteBufAllocator, s, i);
    }

    /**
     * @return supported cipher suites.
     */
    @Override
    public List<String> cipherSuites() {
        return ctxt.cipherSuites();
    }

    /**
     * Deprecated.
     * @return HTTP2 requires "h2" be specified in ALPN.
     */
    @Deprecated
    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return ctxt.applicationProtocolNegotiator();
    }

    /**
     * @return session context.
     */
    @Override
    public SSLSessionContext sessionContext() {
        return ctxt.context().getServerSessionContext();
    }
}
