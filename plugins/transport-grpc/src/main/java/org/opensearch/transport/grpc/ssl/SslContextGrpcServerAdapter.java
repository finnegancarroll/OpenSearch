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
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.List;

import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

/**
 * An io.grpc.SslContext which wraps and delegates functionality to an internal javax.net.SSLContext.
 * As this ssl context is provided for aux transports it operates in server mode always.
 */
public class SslContextGrpcServerAdapter extends SslContext {
    private final SSLContext ctxt;
    private final SecureAuxTransportParameters params;

    /**
     * Initializes a new SecureAuxTransportSslContext.
     * @param provider provides SSLContext with additional ALPN params for engine configuration.
     */
    public SslContextGrpcServerAdapter(SecureAuxTransportSettingsProvider provider) throws NoSuchAlgorithmException, KeyManagementException {
        this.ctxt = provider.buildSSLContext().orElseThrow(IllegalArgumentException::new);
        this.params = provider.parameters().orElseThrow(IllegalArgumentException::new);
    }

    /**
     * Apply client auth mode as configured by SecureAuxTransportParameters.
     */
    private void setClientAuthMode(SSLEngine engine) {
        if (params.clientAuth().isEmpty()) {
            throw new OpenSearchSecurityException("No client auth provided");
        }

        switch (params.clientAuth().orElseThrow(IllegalArgumentException::new)) {
            case "NONE" -> engine.setNeedClientAuth(false);
            case "OPTIONAL" -> engine.setWantClientAuth(true);
            case "REQUIRE" -> engine.setNeedClientAuth(true);
            default -> throw new OpenSearchSecurityException("Invalid client auth provided: " + params.clientAuth());
        }
    }

    /**
     * Apply http2 ALPN to engine instance.
     */
    private void setALPN(SSLEngine engine) {
        SSLParameters p = engine.getSSLParameters();
        p.setApplicationProtocols(new String[]{ApplicationProtocolNames.HTTP_2});
        engine.setSSLParameters(p);
    }

    /**
     * Always operate in server mode in this plugin.
     */
    private void setServerMode(SSLEngine engine){
        engine.setUseClientMode(false);
    }

    /**
     * Apply enabled cipher suites as configured by SecureAuxTransportParameters.
     */
    private String[] getEnabledCipherSuites() {
        List<String> ciphers = (List<String>) params.cipherSuites();
        return ciphers.toArray(new String[0]);
    }

    /*
      Mirror the io.grpc.netty.shaded.io.netty.handler.ssl API with our delegate.
     */

    /**
     * Create a new SSLEngine instance to handle TLS for a connection.
     * @param byteBufAllocator netty allocator - unused by delegate.
     * @return new SSLEngine instance.
     */
    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator) {
        SSLEngine engine = ctxt.createSSLEngine();
        setServerMode(engine);
        setALPN(engine);
        setClientAuthMode(engine);
        engine.setEnabledCipherSuites(getEnabledCipherSuites());
        return engine;
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
        SSLEngine engine = ctxt.createSSLEngine(s, i);
        setServerMode(engine);
        setALPN(engine);
        setClientAuthMode(engine);
        engine.setEnabledCipherSuites(getEnabledCipherSuites());
        return engine;
    }

    /**
     * @return server only context - always false.
     */
    @Override
    public boolean isClient() {
        return false;
    }

    /**
     * @return supported cipher suites.
     */
    @Override
    public List<String> cipherSuites() {
        return List.of(getEnabledCipherSuites());
    }

    /**
     * Deprecated.
     * @return HTTP2 requires "h2" be specified in ALPN.
     */
    @Deprecated
    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return () -> List.of(ApplicationProtocolNames.HTTP_2);
    }

    /**
     * @return session context.
     */
    @Override
    public SSLSessionContext sessionContext() {
        return ctxt.getServerSessionContext();
    }
}
