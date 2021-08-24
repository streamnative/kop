/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.security;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.naming.AuthenticationException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseUtils;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServer;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler;
import org.apache.kafka.common.utils.Utils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;

/**
 * The SASL authenticator.
 */
@Slf4j
public class SaslAuthenticator {

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    @Getter
    private static volatile AuthenticationService authenticationService = null;
    private final BrokerService brokerService;
    private final PulsarAdmin admin;
    private final Set<String> allowedMechanisms;
    private final AuthenticateCallbackHandler oauth2CallbackHandler;
    private State state = State.INITIAL_REQUEST;
    private SaslServer saslServer;
    private Session session;
    private boolean enableKafkaSaslAuthenticateHeaders;


    private enum State {
        INITIAL_REQUEST,
        HANDSHAKE_OR_VERSIONS_REQUEST,
        HANDSHAKE_REQUEST,
        AUTHENTICATE,
        COMPLETE
    }

    /**
     * The exception to indicate that the authenticator's state is illegal when processing some requests.
     */
    public static class IllegalStateException extends AuthenticationException {

        public IllegalStateException(String msg, State actualState, State expectedState) {
            super(msg + " actual state: " + actualState + " expected state: " + expectedState);
        }
    }

    /**
     * The exception to indicate that the client provided mechanism is not supported.
     */
    public static class UnsupportedSaslMechanismException extends AuthenticationException {

        public UnsupportedSaslMechanismException(String mechanism) {
            super("SASL mechanism '" + mechanism + "' requested by client is not supported");
        }
    }

    public SaslAuthenticator(PulsarService pulsarService,
                             Set<String> allowedMechanisms,
                             KafkaServiceConfiguration config) throws PulsarServerException {
        this.brokerService = pulsarService.getBrokerService();
        this.admin = pulsarService.getAdminClient();
        this.allowedMechanisms = allowedMechanisms;
        this.oauth2CallbackHandler = allowedMechanisms.contains(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
                ? createOauth2CallbackHandler(config) : null;
        this.enableKafkaSaslAuthenticateHeaders = false;
    }

    public void authenticate(ChannelHandlerContext ctx,
                             ByteBuf requestBuf) throws AuthenticationException {
        checkArgument(requestBuf.readableBytes() > 0);

        if (saslServer != null && saslServer.isComplete()) {
            setState(State.COMPLETE);
            return;
        }

        if (authenticationService == null) {
            authenticationService = brokerService.getAuthenticationService();
        }
        switch (state) {
            case HANDSHAKE_OR_VERSIONS_REQUEST:
            case HANDSHAKE_REQUEST:
                handleKafkaRequest(ctx, requestBuf);
                break;
            case INITIAL_REQUEST:
                if (handleKafkaRequest(ctx, requestBuf)) {
                    break;
                }
                break;
            case AUTHENTICATE:
                handleSaslToken(ctx, requestBuf);
                if (saslServer.isComplete()) {
                    setState(State.COMPLETE);
                }
                break;
            default:
                break;
        }
    }

    public boolean complete() {
        return state == State.COMPLETE;
    }

    public Session session() {
        if (this.saslServer != null && complete()) {
            return this.session;
        }
        return null;
    }


    public void reset() {
        state = State.HANDSHAKE_OR_VERSIONS_REQUEST;
        if (saslServer != null) {
            try {
                saslServer.dispose();
            } catch (SaslException ignored) {
            }
            saslServer = null;
        }
    }

    private void setState(State state) {
        this.state = state;
        if (log.isDebugEnabled()) {
            log.debug("Set SaslAuthenticator's state to {}", state);
        }
    }

    private @NonNull AuthenticateCallbackHandler createOauth2CallbackHandler(
            @NonNull final KafkaServiceConfiguration config) {
        AuthenticateCallbackHandler handler;
        if (config.getKopOauth2AuthenticateCallbackHandler() != null) {
            final String className = config.getKopOauth2AuthenticateCallbackHandler();
            try {
                Class<?> clazz = Class.forName(className);
                handler = (AuthenticateCallbackHandler) clazz.newInstance();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load class " + className + ": " + e.getMessage());
            } catch (IllegalAccessException | InstantiationException e) {
                throw new RuntimeException("Failed to create new instance of " + className + ": " + e.getMessage());
            } catch (ClassCastException e) {
                throw new RuntimeException("Failed to cast " + className + ": " + e.getMessage());
            }
        } else {
            handler = new OAuthBearerUnsecuredValidatorCallbackHandler();
        }

        final Properties props = config.getKopOauth2Properties();
        final Map<String, String> oauth2Configs = new HashMap<>();
        props.forEach((key, value) -> oauth2Configs.put(key.toString(), value.toString()));
        final AppConfigurationEntry appConfigurationEntry = new AppConfigurationEntry(
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                oauth2Configs);
        handler.configure(null, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                Collections.singletonList(appConfigurationEntry));
        return handler;
    }

    private void createSaslServer(final String mechanism) throws AuthenticationException {
        // TODO: support more mechanisms, see https://github.com/streamnative/kop/issues/235
        if (mechanism.equals(PlainSaslServer.PLAIN_MECHANISM)) {
            saslServer = new PlainSaslServer(authenticationService, admin);
        } else if (mechanism.equals(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)) {
            if (this.oauth2CallbackHandler == null) {
                throw new IllegalArgumentException("No OAuth2CallbackHandler found when mechanism is "
                        + OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);
            }
            saslServer = new OAuthBearerSaslServer(oauth2CallbackHandler);
        } else {
            throw new AuthenticationException("KoP doesn't support '" + mechanism + "' mechanism");
        }
    }

    private boolean handleKafkaRequest(ChannelHandlerContext ctx,
                                    ByteBuf requestBuf) throws AuthenticationException {
        boolean isKafkaRequest = false;
        try {
            ByteBuffer nioBuffer = requestBuf.nioBuffer();
            RequestHeader header = RequestHeader.parse(nioBuffer);
            ApiKeys apiKey = header.apiKey();

            short apiVersion = header.apiVersion();
            Struct struct = apiKey.parseRequest(apiVersion, nioBuffer);
            AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);

            // A valid Kafka request header was received. SASL authentication tokens are now expected only
            // following a SaslHandshakeRequest since this is not a GSSAPI client token from a Kafka 0.9.0.x client.
            if (state == State.INITIAL_REQUEST) {
                setState(State.HANDSHAKE_OR_VERSIONS_REQUEST);
            }
            isKafkaRequest = true;

            // Raise an error prior to parsing if the api cannot be handled at this layer. This avoids
            // unnecessary exposure to some of the more complex schema types.
            if (apiKey != ApiKeys.API_VERSIONS && apiKey != ApiKeys.SASL_HANDSHAKE) {
                throw new IllegalSaslStateException(
                        "Unexpected Kafka request of type " + apiKey + " during SASL handshake.");
            }

            if (log.isDebugEnabled()) {
                log.debug("Handling Kafka request header {}, body {}", header, body);
            }

            if (apiKey == ApiKeys.API_VERSIONS) {
                handleApiVersionsRequest(ctx, header, (ApiVersionsRequest) body);
            } else {
                String clientMechanism = handleHandshakeRequest(ctx, header, (SaslHandshakeRequest) body);
                try {
                    createSaslServer(clientMechanism);
                } catch (AuthenticationException e) {
                    sendKafkaResponse(ctx,
                            header,
                            body,
                            null,
                            e);
                    throw e;
                }

                setState(State.AUTHENTICATE);
            }
        } catch (AuthenticationException e) {
            throw e;
        }
        return isKafkaRequest;
    }

    private static void sendKafkaResponse(ChannelHandlerContext ctx,
                                          RequestHeader header,
                                          AbstractRequest request,
                                          AbstractResponse abstractResponse,
                                          Exception e) {
        short version = header.apiVersion();
        ApiKeys apiKey = header.apiKey();
        AbstractResponse backResponse;

        if (e != null && abstractResponse == null) {
            backResponse = request.getErrorResponse(e);
        } else {
            backResponse = abstractResponse;
        }

        if (apiKey == API_VERSIONS){
            if (!ApiKeys.API_VERSIONS.isVersionSupported(version)) {
                version = ApiKeys.API_VERSIONS.oldestVersion();
            }
        }
        ByteBuf response = ResponseUtils.serializeResponse(
                version,
                header.toResponseHeader(),
                backResponse
        );
        ctx.channel().eventLoop().execute(() -> {
            ctx.channel().writeAndFlush(response);
        });
    }

    private static ByteBuf sizePrefixed(ByteBuffer buffer) {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(0, buffer.remaining());
        ByteBuf byteBuf = Unpooled.buffer(sizeBuffer.capacity() + buffer.remaining());
        byteBuf.writeBytes(sizeBuffer);
        byteBuf.writeBytes(buffer);
        return byteBuf;
    }

    private void handleSaslToken(ChannelHandlerContext ctx,
                                 ByteBuf requestBuf) throws AuthenticationException {
        ByteBuffer nioBuffer = requestBuf.nioBuffer();
        if (!enableKafkaSaslAuthenticateHeaders) {
            try {
                byte[] clientToken = new byte[nioBuffer.remaining()];
                nioBuffer.get(clientToken, 0, clientToken.length);
                byte[] response = saslServer.evaluateResponse(clientToken);
                if (response != null) {
                    ByteBuf byteBuf = sizePrefixed(ByteBuffer.wrap(response));
                    ctx.channel().writeAndFlush(byteBuf).addListener(future -> {
                        if (!future.isSuccess()) {
                            log.error("[{}] Failed to write {}", ctx.channel(), future.cause());
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Send sasl response to old Client {} successfully", ctx.channel());
                            }

                            this.session = new Session(
                                    new KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID()),
                                    "old-clientId");
                        }
                    });
                }
            } catch (SaslException e) {
                log.debug("Authenticate failed for oldClient, reason {}", e.getMessage());
                throw new AuthenticationException("Failed during handleSaslToken " + e.getMessage());
            }
        } else {
            RequestHeader header = RequestHeader.parse(nioBuffer);
            ApiKeys apiKey = header.apiKey();
            short version = header.apiVersion();
            Struct struct = apiKey.parseRequest(version, nioBuffer);
            AbstractRequest request = AbstractRequest.parseRequest(apiKey, version, struct);
            if (apiKey != ApiKeys.SASL_AUTHENTICATE) {
                AuthenticationException e = new AuthenticationException(
                        "Unexpected Kafka request of type " + apiKey + " during SASL authentication");
                sendKafkaResponse(ctx,
                        header,
                        request,
                        null,
                        e);
                throw e;
            }
            if (!apiKey.isVersionSupported(version)) {
                throw new AuthenticationException("Version " + version + " is not supported for apiKey " + apiKey);
            }

            SaslAuthenticateRequest saslAuthenticateRequest = (SaslAuthenticateRequest) request;

            try {
                byte[] responseToken =
                        saslServer.evaluateResponse(Utils.toArray(saslAuthenticateRequest.saslAuthBytes()));
                ByteBuffer responseBuf = (responseToken == null) ? EMPTY_BUFFER : ByteBuffer.wrap(responseToken);
                this.session = new Session(
                        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID()),
                        header.clientId());
                sendKafkaResponse(ctx,
                        header,
                        request,
                        new SaslAuthenticateResponse(Errors.NONE, null, responseBuf),
                        null);
                if (log.isDebugEnabled()) {
                    log.debug("Authenticate successfully for client, header {}, request {}, session {}",
                            header, saslAuthenticateRequest, session);
                }
            } catch (SaslException e) {
                sendKafkaResponse(ctx,
                        header,
                        request,
                        new SaslAuthenticateResponse(Errors.SASL_AUTHENTICATION_FAILED, e.getMessage()),
                        null);
                if (log.isDebugEnabled()) {
                    log.debug("Authenticate failed for client, header {}, request {}, reason {}",
                            header, saslAuthenticateRequest, e.getMessage());
                }
            }
        }
    }

    private void handleApiVersionsRequest(ChannelHandlerContext ctx,
                                          RequestHeader header,
                                          ApiVersionsRequest request)
            throws AuthenticationException {
        if (state != State.HANDSHAKE_OR_VERSIONS_REQUEST) {
            throw new IllegalStateException(
                    "Receive ApiVersions request", state, State.HANDSHAKE_OR_VERSIONS_REQUEST);
        }
        if (request.hasUnsupportedRequestVersion()) {
            sendKafkaResponse(ctx,
                    header,
                    request,
                    request.getErrorResponse(0, Errors.UNSUPPORTED_VERSION.exception()),
                    null);
        } else {
            sendKafkaResponse(ctx,
                    header,
                    request,
                    ApiVersionsResponse.defaultApiVersionsResponse(),
                    null);
            // Handshake request must be followed by the ApiVersions request
            setState(State.HANDSHAKE_REQUEST);
        }
    }

    private @NonNull String handleHandshakeRequest(ChannelHandlerContext ctx,
                                                   RequestHeader header,
                                                   SaslHandshakeRequest request)
            throws AuthenticationException {

        final String mechanism = request.mechanism();
        if (mechanism == null) {
            AuthenticationException e = new AuthenticationException("client's mechanism is null");
            sendKafkaResponse(ctx,
                    header,
                    request,
                    null,
                    e);
            throw e;
        }

        if (header.apiVersion() >= 1) {
            this.enableKafkaSaslAuthenticateHeaders = true;
        }

        if (allowedMechanisms.contains(mechanism)) {
            if (log.isDebugEnabled()) {
                log.debug("Using SASL mechanism '{}' provided by client", mechanism);
            }
            sendKafkaResponse(ctx,
                    header,
                    request,
                    new SaslHandshakeResponse(Errors.NONE, allowedMechanisms),
                    null);
            return mechanism;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("SASL mechanism '{}' requested by client is not supported", mechanism);
            }
            sendKafkaResponse(ctx,
                    header,
                    request,
                    new SaslHandshakeResponse(Errors.UNSUPPORTED_SASL_MECHANISM, allowedMechanisms),
                    null);
            throw new UnsupportedSaslMechanismException(mechanism);
        }
    }
}
