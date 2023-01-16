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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.security.oauth.KopOAuthBearerSaslServer;
import io.streamnative.pulsar.handlers.kop.security.oauth.KopOAuthBearerUnsecuredValidatorCallbackHandler;
import io.streamnative.pulsar.handlers.kop.utils.KafkaResponseUtils;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.KopResponseUtils;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;

/**
 * The SASL authenticator.
 */
@Slf4j
public class SaslAuthenticator {

    public static final String USER_NAME_PROP = "username";
    public static final String AUTH_DATA_SOURCE_PROP = "authDataSource";
    public static final String AUTHENTICATION_SERVER_OBJ = "authenticationServerObj";

    private static final byte[] EMPTY_BUFFER = new byte[0];

    @Getter
    private final AuthenticationService authenticationService;

    private final PulsarAdmin admin;
    private final Set<String> allowedMechanisms;
    private final Set<String> proxyRoles;
    private final AuthenticateCallbackHandler oauth2CallbackHandler;
    private State state = State.HANDSHAKE_OR_VERSIONS_REQUEST;
    private SaslServer saslServer;
    private Session session;
    private boolean enableKafkaSaslAuthenticateHeaders;
    private ByteBuf authenticationFailureResponse = null;
    private ChannelHandlerContext ctx = null;
    private String defaultKafkaMetadataTenant;

    private enum State {
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

    /**
     * The exception to indicate that the SaslServer doesn't have the expected property.
     */
    public static class NoExpectedPropertyException extends AuthenticationException {

        public NoExpectedPropertyException(String propertyName, String msg) {
            super("No expected property for " + propertyName + ": " + msg);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T safeGetProperty(final SaslServer saslServer, final String propertyName) {
        try {
            final Object property = saslServer.getNegotiatedProperty(propertyName);
            if (property == null) {
                throw new NoExpectedPropertyException(propertyName, "property not found");
            }
            return (T) property;
        } catch (ClassCastException e) {
            throw new NoExpectedPropertyException(propertyName, e.getMessage());
        }
    }

    /**
     * Build a {@link ByteBuf} response on authenticate failure. The actual response is sent out when
     * {@link #sendAuthenticationFailureResponse(Consumer)} is called.
     */
    private void buildResponseOnAuthenticateFailure(RequestHeader header,
                                                    AbstractRequest request,
                                                    AbstractResponse abstractResponse,
                                                    Exception e) {
        this.authenticationFailureResponse = buildKafkaResponse(header, request, abstractResponse, e);
    }

    /**
     * Send any authentication failure response that may have been previously built.
     */
    public void sendAuthenticationFailureResponse(Consumer<Future<? super Void>> listener) {
        if (authenticationFailureResponse == null) {
            listener.accept(null);
            return;
        }
        this.sendKafkaResponse(authenticationFailureResponse, listener::accept);
        authenticationFailureResponse = null;
    }

    public SaslAuthenticator(PulsarService pulsarService,
                             Set<String> allowedMechanisms,
                             KafkaServiceConfiguration config) throws PulsarServerException {
        this.authenticationService = pulsarService.getBrokerService().getAuthenticationService();
        this.admin = pulsarService.getAdminClient();
        this.allowedMechanisms = allowedMechanisms;
        this.proxyRoles = config.getProxyRoles();
        this.oauth2CallbackHandler = allowedMechanisms.contains(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
                ? createOAuth2CallbackHandler(config) : null;
        this.enableKafkaSaslAuthenticateHeaders = false;
        this.defaultKafkaMetadataTenant = config.getKafkaMetadataTenant();
    }

    /**
     * Used by external usages like KOP Proxy.
     * @param admin
     * @param authenticationService
     * @param allowedMechanisms
     * @param config
     * @throws PulsarServerException
     */
    public SaslAuthenticator(PulsarAdmin admin,
                             AuthenticationService authenticationService,
                             Set<String> allowedMechanisms,
                             KafkaServiceConfiguration config) throws PulsarServerException {
        this.authenticationService = authenticationService;
        this.proxyRoles = config.getProxyRoles();
        this.admin = admin;
        this.allowedMechanisms = allowedMechanisms;
        this.oauth2CallbackHandler = allowedMechanisms.contains(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
                ? createOAuth2CallbackHandler(config) : null;
        this.enableKafkaSaslAuthenticateHeaders = false;
    }

    public void authenticate(ChannelHandlerContext ctx,
                             ByteBuf requestBuf,
                             BiConsumer<Long, Throwable> registerRequestParseLatency,
                             BiConsumer<ApiKeys, Long> registerRequestLatency,
                             Function<Session, Boolean> tenantAccessValidationFunction)
            throws AuthenticationException {
        checkArgument(requestBuf.readableBytes() > 0);
        if (log.isDebugEnabled()) {
            log.debug("Authenticate {} {} {}", ctx, saslServer, state);
        }

        this.ctx = ctx;
        if (saslServer != null && saslServer.isComplete()) {
            setState(State.COMPLETE);
            return;
        }
        switch (state) {
            case HANDSHAKE_OR_VERSIONS_REQUEST:
            case HANDSHAKE_REQUEST:
                handleKafkaRequest(ctx, requestBuf, registerRequestParseLatency, registerRequestLatency);
                break;
            case AUTHENTICATE:
                handleSaslToken(ctx, requestBuf, registerRequestParseLatency, registerRequestLatency,
                                                                            tenantAccessValidationFunction);
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

    private @NonNull AuthenticateCallbackHandler createOAuth2CallbackHandler(
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
            handler = new KopOAuthBearerUnsecuredValidatorCallbackHandler();
        }

        final Properties props = config.getKopOauth2Properties();
        final Map<String, String> oauth2Configs = new HashMap<>();
        props.forEach((key, value) -> oauth2Configs.put(key.toString(), value.toString()));
        final AppConfigurationEntry appConfigurationEntry = new AppConfigurationEntry(
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                oauth2Configs);
        HashMap<String, Object> configs = new HashMap<>();
        configs.put(AUTHENTICATION_SERVER_OBJ, this.getAuthenticationService());
        handler.configure(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                Collections.singletonList(appConfigurationEntry));
        return handler;
    }

    private void createSaslServer(final String mechanism) throws AuthenticationException {
        // TODO: support more mechanisms, see https://github.com/streamnative/kop/issues/235
        if (mechanism.equals(PlainSaslServer.PLAIN_MECHANISM)) {
            saslServer = new PlainSaslServer(authenticationService, admin, proxyRoles);
        } else if (mechanism.equals(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)) {
            if (this.oauth2CallbackHandler == null) {
                throw new IllegalArgumentException("No OAuth2CallbackHandler found when mechanism is "
                        + OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);
            }
            saslServer = new KopOAuthBearerSaslServer(oauth2CallbackHandler, defaultKafkaMetadataTenant);
        } else {
            throw new AuthenticationException("KoP doesn't support '" + mechanism + "' mechanism");
        }
    }

    private static boolean isUnsupportedApiVersionsRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }

    private AbstractRequest parseRequest(RequestHeader header, ByteBuffer nioBuffer) {
        if (isUnsupportedApiVersionsRequest(header)) {
            ApiVersionsRequestData data = new ApiVersionsRequestData();
            return new ApiVersionsRequest(data, header.apiVersion());
        } else {
            ApiKeys apiKey = header.apiKey();
            short apiVersion = header.apiVersion();
            return AbstractRequest.parseRequest(apiKey, apiVersion, nioBuffer).request;
        }

    }

    // Parsing request, for here, only support ApiVersions and SaslHandshake request
    private void handleKafkaRequest(ChannelHandlerContext ctx,
                                    ByteBuf requestBuf,
                                    BiConsumer<Long, Throwable> registerRequestParseLatency,
                                    BiConsumer<ApiKeys, Long> registerRequestLatency)
            throws AuthenticationException {
        final long beforeParseTime = MathUtils.nowInNano();
        ByteBuffer nioBuffer = requestBuf.nioBuffer();
        RequestHeader header = RequestHeader.parse(nioBuffer);
        ApiKeys apiKey = header.apiKey();

        AbstractRequest body = parseRequest(header, nioBuffer);
        registerRequestParseLatency.accept(beforeParseTime, null);

        // Raise an error prior to parsing if the api cannot be handled at this layer. This avoids
        // unnecessary exposure to some of the more complex schema types.
        if (apiKey != ApiKeys.API_VERSIONS && apiKey != ApiKeys.SASL_HANDSHAKE) {
            throw new IllegalSaslStateException(
                    "Unexpected Kafka request of type " + apiKey + " during SASL handshake.");
        }

        if (log.isDebugEnabled()) {
            log.debug("Handling Kafka request header {}, body {}", header, body);
        }

        final long startProcessRequestTime = MathUtils.nowInNano();
        if (apiKey == ApiKeys.API_VERSIONS) {
            handleApiVersionsRequest(ctx,
                    header,
                    (ApiVersionsRequest) body,
                    startProcessRequestTime,
                    registerRequestLatency);
        } else {
            String clientMechanism = handleHandshakeRequest(ctx,
                    header,
                    (SaslHandshakeRequest) body,
                    startProcessRequestTime,
                    registerRequestLatency);
            try {
                createSaslServer(clientMechanism);
            } catch (AuthenticationException e) {
                this.authenticationFailureResponse = buildKafkaResponse(header, body, null, e);
                throw e;
            }

            setState(State.AUTHENTICATE);
        }
    }

    // Send a response that conforms to the Kafka protocol back to the client.
    private static void sendKafkaResponse(ChannelHandlerContext ctx,
                                          RequestHeader header,
                                          AbstractRequest request,
                                          AbstractResponse abstractResponse,
                                          Exception e) {
        ByteBuf response = buildKafkaResponse(header, request, abstractResponse, e);
        ctx.channel().eventLoop().execute(() -> {
            ctx.channel().writeAndFlush(response);
        });
    }

    private void sendKafkaResponse(ByteBuf response,
                                   GenericFutureListener<? extends Future<? super Void>> listener) {
        ctx.channel().eventLoop().execute(() -> {
            ctx.channel().writeAndFlush(response).addListener(listener);
        });
    }

    private static ByteBuf buildKafkaResponse(RequestHeader header,
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

        if (apiKey == ApiKeys.API_VERSIONS
                && !ApiKeys.API_VERSIONS.isVersionSupported(version)){
            version = ApiKeys.API_VERSIONS.oldestVersion();
        }
        return KopResponseUtils.serializeResponse(
                version,
                header.toResponseHeader(),
                backResponse
        );
    }

    private void handleSaslToken(ChannelHandlerContext ctx,
                                 ByteBuf requestBuf,
                                 BiConsumer<Long, Throwable> registerRequestParseLatency,
                                 BiConsumer<ApiKeys, Long> registerRequestLatency,
                                 Function<Session, Boolean> tenantAccessValidationFunction)
            throws AuthenticationException {
        final long timeBeforeParse = MathUtils.nowInNano();
        ByteBuffer nioBuffer = requestBuf.nioBuffer();
        // Handle the SaslAuthenticate token from the old client
        // relative to sasl handshake v0.
        if (!enableKafkaSaslAuthenticateHeaders) {
            try {
                byte[] clientToken = new byte[nioBuffer.remaining()];
                nioBuffer.get(clientToken, 0, clientToken.length);
                byte[] response = saslServer.evaluateResponse(clientToken);
                if (response != null) {
                    ByteBuf byteBuf = Unpooled.wrappedBuffer(response);
                    final Session newSession;
                    if (saslServer.isComplete()) {
                        newSession = new Session(
                                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID(),
                                        safeGetProperty(saslServer, USER_NAME_PROP),
                                        safeGetProperty(saslServer, AUTH_DATA_SOURCE_PROP)),
                                "old-clientId");
                        if (!tenantAccessValidationFunction.apply(newSession)) {
                            throw new AuthenticationException("User is not allowed to access this tenant");
                        }
                    } else {
                        newSession = null;
                    }
                    ctx.channel().writeAndFlush(byteBuf).addListener(future -> {
                        if (!future.isSuccess()) {
                            log.error("[{}] Failed to write {}", ctx.channel(), future.cause());
                        } else {
                            // This session is required for authorization.
                            session = newSession;
                            if (log.isDebugEnabled()) {
                                log.debug("Send sasl response to SASL_HANDSHAKE v0 old client {} successfully, "
                                        + "session {}", ctx.channel(), session);
                            }
                        }
                    });
                }
            } catch (SaslException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Authenticate failed for SASL_HANDSHAKE v0 old client, reason {}",
                            e.getMessage());
                }
            }
        } else {
            // Handle the SaslAuthenticateRequest from the client
            // relative to sasl handshake v1 and above.
            RequestHeader header = RequestHeader.parse(nioBuffer);
            ApiKeys apiKey = header.apiKey();
            short version = header.apiVersion();
            AbstractRequest request = AbstractRequest.parseRequest(apiKey, version, nioBuffer).request;
            registerRequestParseLatency.accept(timeBeforeParse, null);

            final long startProcessTime = MathUtils.nowInNano();
            if (apiKey != ApiKeys.SASL_AUTHENTICATE) {
                AuthenticationException e = new AuthenticationException(
                        "Unexpected Kafka request of type " + apiKey + " during SASL authentication");
                registerRequestLatency.accept(apiKey, startProcessTime);
                buildResponseOnAuthenticateFailure(header, request, null, e);
                throw e;
            }
            if (!apiKey.isVersionSupported(version)) {
                throw new AuthenticationException("Version " + version + " is not supported for apiKey " + apiKey);
            }

            SaslAuthenticateRequest saslAuthenticateRequest = (SaslAuthenticateRequest) request;

            try {
                byte[] responseToken =
                        saslServer.evaluateResponse(saslAuthenticateRequest.data().authBytes());
                byte[] responseBuf = (responseToken == null) ? EMPTY_BUFFER : responseToken;
                if (saslServer.isComplete()) {
                    String pulsarRole = saslServer.getAuthorizationID();
                    this.session = new Session(
                            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, pulsarRole,
                                    safeGetProperty(saslServer, USER_NAME_PROP),
                                    safeGetProperty(saslServer, AUTH_DATA_SOURCE_PROP)),
                            header.clientId());
                    if (log.isDebugEnabled()) {
                        log.debug("Authenticate successfully for client, header {}, request {}, session {} username {},"
                                        + " authDataSource {}",
                                header, saslAuthenticateRequest, session,
                                saslServer.getNegotiatedProperty(USER_NAME_PROP),
                                saslServer.getNegotiatedProperty(AUTH_DATA_SOURCE_PROP));
                    }
                    if (!tenantAccessValidationFunction.apply(session)) {
                        AuthenticationException e =
                                new AuthenticationException("User is not allowed to access this tenant");
                        registerRequestLatency.accept(apiKey, startProcessTime);
                        buildResponseOnAuthenticateFailure(header, request, null, e);
                        throw e;
                    }
                }
                registerRequestLatency.accept(apiKey, startProcessTime);
                sendKafkaResponse(ctx,
                        header,
                        request,
                        KafkaResponseUtils.newSaslAuthenticate(responseBuf),
                        null);
            } catch (SaslAuthenticationException e) {
                buildResponseOnAuthenticateFailure(header, request,
                        KafkaResponseUtils.newSaslAuthenticate(Errors.SASL_AUTHENTICATION_FAILED, e.getMessage()), e);
                throw e;
            } catch (SaslException e) {
                registerRequestLatency.accept(apiKey, startProcessTime);
                buildResponseOnAuthenticateFailure(header, request,
                        KafkaResponseUtils.newSaslAuthenticate(Errors.SASL_AUTHENTICATION_FAILED, e.getMessage()), e);
                sendAuthenticationFailureResponse((__ -> {}));
                if (log.isDebugEnabled()) {
                    log.debug("Authenticate failed for client, header {}, request {}, reason {}",
                            header, saslAuthenticateRequest, e.getMessage(), e);
                }
            }
        }
    }

    private void handleApiVersionsRequest(ChannelHandlerContext ctx,
                                          RequestHeader header,
                                          ApiVersionsRequest request,
                                          Long startProcessTime,
                                          BiConsumer<ApiKeys, Long> registerRequestLatency)
            throws AuthenticationException {
        if (state != State.HANDSHAKE_OR_VERSIONS_REQUEST) {
            throw new IllegalStateException(
                    "Receive ApiVersions request", state, State.HANDSHAKE_OR_VERSIONS_REQUEST);
        }
        if (request.hasUnsupportedRequestVersion()) {
            registerRequestLatency.accept(header.apiKey(), startProcessTime);
            sendKafkaResponse(ctx, header, request,
                    request.getErrorResponse(0, Errors.UNSUPPORTED_VERSION.exception()),
                    null);
        } else {
            ApiVersionsResponse versionsResponse =
                    ApiVersionsResponse.defaultApiVersionsResponse(ApiMessageType.ListenerType.BROKER);
            registerRequestLatency.accept(header.apiKey(), startProcessTime);
            sendKafkaResponse(ctx,
                    header,
                    request,
                    versionsResponse,
                    null);
            // Handshake request must be followed by the ApiVersions request
            setState(State.HANDSHAKE_REQUEST);
        }
    }

    private @NonNull String handleHandshakeRequest(ChannelHandlerContext ctx,
                                                   RequestHeader header,
                                                   SaslHandshakeRequest request,
                                                   Long startProcessTime,
                                                   BiConsumer<ApiKeys, Long> registerRequestLatency)
            throws AuthenticationException {

        final String mechanism = request.data().mechanism();
        if (mechanism == null) {
            AuthenticationException e = new AuthenticationException("client's mechanism is null");
            registerRequestLatency.accept(header.apiKey(), startProcessTime);
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
            registerRequestLatency.accept(header.apiKey(), startProcessTime);
            sendKafkaResponse(ctx,
                    header,
                    request,
                    KafkaResponseUtils.newSaslHandshake(Errors.NONE, allowedMechanisms),
                    null);
            return mechanism;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("SASL mechanism '{}' requested by client is not supported", mechanism);
            }
            registerRequestLatency.accept(header.apiKey(), startProcessTime);
            buildResponseOnAuthenticateFailure(header, request,
                    KafkaResponseUtils.newSaslHandshake(Errors.UNSUPPORTED_SASL_MECHANISM, allowedMechanisms),
                    null);
            throw new UnsupportedSaslMechanismException(mechanism);
        }
    }
}
