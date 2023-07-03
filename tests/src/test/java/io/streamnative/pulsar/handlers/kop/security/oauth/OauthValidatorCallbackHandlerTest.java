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
package io.streamnative.pulsar.handlers.kop.security.oauth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.testng.annotations.Test;

/**
 * Unit test for {@link OauthValidatorCallbackHandler}.
 */
public class OauthValidatorCallbackHandlerTest {

    @Test
    public void testHandleCallback() throws AuthenticationException {
        AuthenticationService mockAuthService = mock(AuthenticationService.class);
        OauthValidatorCallbackHandler handler =
                spy(new OauthValidatorCallbackHandler(new ServerConfig(new HashMap<>()), mockAuthService));

        AuthenticationProvider mockAuthProvider = mock(AuthenticationProvider.class);

        doReturn(mockAuthProvider).when(mockAuthService).getAuthenticationProvider(anyString());

        AuthenticationState state = mock(AuthenticationState.class);

        doReturn(state).when(mockAuthProvider).newAuthState(any(), any(), any());

        doReturn(CompletableFuture.completedFuture(null)).when(state).authenticateAsync(any());

        KopOAuthBearerValidatorCallback callbackWithTenant =
                new KopOAuthBearerValidatorCallback("my-tenant" + OAuthTokenDecoder.DELIMITER + "my-token");

        handler.handleCallback(callbackWithTenant);

        KopOAuthBearerToken token = callbackWithTenant.token();
        assertEquals(token.tenant(), "my-tenant");
        assertEquals(token.value(), "my-token");

        KopOAuthBearerValidatorCallback callbackWithoutTenant =
                new KopOAuthBearerValidatorCallback("my-token");
        handler.handleCallback(callbackWithoutTenant);

        token = callbackWithoutTenant.token();
        assertNull(token.tenant());
        assertEquals(token.value(), "my-token");
    }

}
