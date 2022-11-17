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
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import org.apache.commons.validator.routines.InetAddressValidator;

@Getter
public class AdvertisedListener {

    private static final String REGEX = "^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    // the original listener string, like "PLAINTEXT://localhost:9092"
    private final String originalListener;

    private final String listenerName;

    private final String hostname;

    private final int port;

    public static AdvertisedListener create(String listener) {
        return new AdvertisedListener(listener);
    }

    public AdvertisedListener(String listener) {
        this.originalListener = listener;
        final String errorMessage = "Listener '" + listener + "' is invalid";

        Matcher matcher = AdvertisedListener.matcherListener(listener, errorMessage);

        this.listenerName = matcher.group(1);
        this.port = Integer.parseInt(matcher.group(3), 10);

        checkState(port >= 0 && port <= 65535, errorMessage + ": port '" + port + "' is invalid.");

        String hostname = matcher.group(2);

        if (hostname.isEmpty()) {
            try {
                this.hostname = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                throw new IllegalStateException("Hostname is empty and localhost is unknown: " + e.getMessage());
            }
        } else if (InetAddressValidator.getInstance().isValidInet6Address(hostname)) {
            try {
                // Get full IPv6 address
                this.hostname = InetAddress.getByName(hostname).getHostAddress();
            } catch (UnknownHostException e) {
                throw new IllegalStateException("Hostname is invalid: " + e.getMessage());
            }
        } else {
            this.hostname = hostname;
        }
    }

    public static Matcher matcherListener(String listener, String errorMessage) {
        final Matcher matcher = PATTERN.matcher(listener);
        checkState(matcher.find(), errorMessage);
        checkState(matcher.groupCount() == 3, errorMessage);
        return matcher;
    }
}
