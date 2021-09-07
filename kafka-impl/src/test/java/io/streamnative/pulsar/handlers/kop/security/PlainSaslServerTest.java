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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import javax.naming.AuthenticationException;
import javax.security.sasl.SaslException;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

/**
 * Test SaslAuthenticator.
 *
 * @see SaslAuthenticator
 */
public class PlainSaslServerTest {

    @Test
    public void testSetPrincipalRegularUser() throws Exception {
        testAssignPrincipal("I-DONT-CARE", "user", "user");
    }

    @Test
    public void testSetPrincipalUsingProxyRole() throws Exception {
        testAssignPrincipal("user", "proxy", "user");
    }

    @Test(expectedExceptions = SaslException.class)
    public void testCannotUserProxyRoleOverProxyRole() throws Exception {
        testAssignPrincipal("proxy", "secondproxy", null);
    }

    private void testAssignPrincipal(String username, String role, String expectedRole)
            throws AuthenticationException, SaslException {
        Set<String> proxyRoles = new HashSet<>();
        proxyRoles.add("proxy");
        proxyRoles.add("secondproxy");
        AuthenticationService authenticationService  = mock(AuthenticationService.class);
        AuthenticationProvider provider = mock(AuthenticationProvider.class);
        when(authenticationService.getAuthenticationProvider(eq("token"))).thenReturn(provider);
        AuthenticationState state = new AuthenticationState() {
            @Override
            public String getAuthRole() throws AuthenticationException {
                return role;
            }

            @Override
            public AuthData authenticate(AuthData authData) throws AuthenticationException {
                return authData;
            }

            @Override
            public AuthenticationDataSource getAuthDataSource() {
                return null;
            }

            @Override
            public boolean isComplete() {
                return true;
            }
        };
        when(provider.newAuthState(any(AuthData.class), any(), any())).thenReturn(state);
        PlainSaslServer server = new PlainSaslServer(authenticationService, null, proxyRoles);

        String challengeNoProxy = "XXXXX\000" + username + "\000token:xxxxx";
        server.evaluateResponse(challengeNoProxy.getBytes(StandardCharsets.US_ASCII));
        String detectedRole = server.getAuthorizationID();
        assertEquals(detectedRole, expectedRole);
    }

}
