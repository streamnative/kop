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
package io.streamnative.pulsar.handlers.kop;

import static com.google.common.base.Preconditions.checkState;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import org.apache.kafka.common.security.auth.SecurityProtocol;

/**
 * The endpoint that KoP binds.
 */
public class EndPoint {

    private static final String REGEX = "^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    @Getter
    private final SecurityProtocol securityProtocol;
    @Getter
    private final String hostname;
    @Getter
    private final int port;

    public EndPoint(final String listener) {
        final String errorMessage = "listener '" + listener + "' is invalid";
        final Matcher matcher = PATTERN.matcher(listener);
        checkState(matcher.find(), errorMessage);
        checkState(matcher.groupCount() == 3, errorMessage);

        this.securityProtocol = SecurityProtocol.forName(matcher.group(1));
        final String originalHostname = matcher.group(2);
        if (originalHostname.isEmpty()) {
            try {
                this.hostname = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                throw new IllegalStateException("hostname is empty and localhost is unknown: " + e.getMessage());
            }
        } else {
            this.hostname = originalHostname;
        }
        this.port = Integer.parseInt(matcher.group(3));
        checkState(port >= 0 && port <= 65535, errorMessage + ": port " + port + " is invalid");
    }

    public InetSocketAddress getInetAddress() {
        return new InetSocketAddress(hostname, port);
    }
}
