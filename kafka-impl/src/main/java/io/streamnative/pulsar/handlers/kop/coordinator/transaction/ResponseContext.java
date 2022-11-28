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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import java.net.SocketAddress;
import lombok.Getter;
import org.apache.kafka.common.requests.AbstractResponse;

@Getter
public class ResponseContext {

    private SocketAddress remoteAddress;
    private short apiVersion;
    private int correlationId;
    private AbstractResponse response;

    public ResponseContext set(final SocketAddress remoteAddress,
                               final short apiVersion,
                               final int correlationId,
                               final AbstractResponse response) {
        this.remoteAddress = remoteAddress;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.response = response;
        return this;
    }

    public String getResponseDescription() {
        return response.toString();
    }
}
