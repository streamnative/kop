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

import org.apache.kafka.common.Node;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KopEventManagerTest {

    @Test
    public void testGetNode() {
        final String host = "localhost";
        final int port = 9120;
        final String securityProtocol = "SASL_PLAINTEXT";
        final String brokerStr = securityProtocol + "://" + host + ":" + port;
        Node node = KopEventManager.getNode(brokerStr);
        Assert.assertEquals(node.host(), host);
        Assert.assertEquals(node.port(), port);
    }

}
