package io.streamnative.pulsar.handlers.kop.security.oauth;

import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

public class KopOAuthBearerUnsecuredJws extends OAuthBearerUnsecuredJws implements KopOAuthBearerToken {

    /**
     * Constructor with the given principal and scope claim names
     *
     * @param compactSerialization the compact serialization to parse as an unsecured JWS
     * @param principalClaimName   the required principal claim name
     * @param scopeClaimName       the required scope claim name
     * @throws OAuthBearerIllegalTokenException if the compact serialization is not a valid unsecured JWS
     *                                          (meaning it did not have 3 dot-separated Base64URL sections
     *                                          without an empty digital signature; or the header or claims
     *                                          either are not valid Base 64 URL encoded values or are not JSON
     *                                          after decoding; or the mandatory '{@code alg}' header value is
     *                                          not "{@code none}")
     */
    public KopOAuthBearerUnsecuredJws(String compactSerialization, String principalClaimName, String scopeClaimName) throws OAuthBearerIllegalTokenException {
        super(compactSerialization, principalClaimName, scopeClaimName);
    }

    @Override
    public AuthenticationDataSource authDataSource() {
        return null;
    }
}
