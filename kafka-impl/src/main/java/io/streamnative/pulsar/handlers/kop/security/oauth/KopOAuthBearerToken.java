package io.streamnative.pulsar.handlers.kop.security.oauth;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

import java.util.Set;

/**
 * The <code>b64token</code> value as defined in
 * <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section
 * 2.1</a> along with the token's specific scope and lifetime and principal
 * name.
 * <p>
 * A network request would be required to re-hydrate an opaque token, and that
 * could result in (for example) an {@code IOException}, but retrievers for
 * various attributes ({@link #scope()}, {@link #lifetimeMs()}, etc.) declare no
 * exceptions. Therefore, if a network request is required for any of these
 * retriever methods, that request could be performed at construction time so
 * that the various attributes can be reliably provided thereafter. For example,
 * a constructor might declare {@code throws IOException} in such a case.
 * Alternatively, the retrievers could throw unchecked exceptions.
 * <p>
 * This interface was introduced in 2.0.0 and, while it feels stable, it could
 * evolve. We will try to evolve the API in a compatible manner (easier now that
 * Java 7 and its lack of default methods doesn't have to be supported), but we
 * reserve the right to make breaking changes in minor releases, if necessary.
 * We will update the {@code InterfaceStability} annotation and this notice once
 * the API is considered stable.
 *
 * @see <a href="https://tools.ietf.org/html/rfc6749#section-1.4">RFC 6749
 *      Section 1.4</a> and
 *      <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750
 *      Section 2.1</a>
 */
@InterfaceStability.Evolving
public interface KopOAuthBearerToken extends OAuthBearerToken {
    AuthenticationDataSource authDataSource();
}

