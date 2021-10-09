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

import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Node;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KopEventManagerTest {

    @Test
    public void testGetOneNode() {
        final String host = "localhost";
        final int port = 9120;
        final String securityProtocol = "SASL_PLAINTEXT";
        final String brokerStr = securityProtocol + "://" + host + ":" + port;
        Map<String, Set<Node>> nodes = KopEventManager.getNodes(brokerStr);
        Assert.assertEquals(1, nodes.size());
        Assert.assertTrue(nodes.containsKey(securityProtocol));
        Set<Node> nodeSet = nodes.get(securityProtocol);
        Assert.assertEquals(1, nodeSet.size());
        nodeSet.forEach(node -> {
            Assert.assertEquals(node.host(), host);
            Assert.assertEquals(node.port(), port);
        });
    }

    @Test
    public void testGetMultipleNodes() {
        final String listenerName1 = "kafka_internal";
        final String host1 = "localhost";
        final int port1 = 9120;
        final String listenerName2 = "kafka_external";
        final String host2 = "localhost";
        final int port2 = 9121;
        final String brokersStr = listenerName1 + "://" + host1 + ":" + port1 + ","
                + listenerName2 + "://" + host2 + ":" + port2;
        Map<String, Set<Node>> nodes = KopEventManager.getNodes(brokersStr);
        Assert.assertEquals(2, nodes.size());
        Assert.assertTrue(nodes.containsKey(listenerName1));
        Set<Node> nodesSet1 = nodes.get(listenerName1);
        Assert.assertEquals(1, nodesSet1.size());
        nodesSet1.forEach(node -> {
            Assert.assertEquals(node.host(), host1);
            Assert.assertEquals(node.port(), port1);
        });
        Assert.assertTrue(nodes.containsKey(listenerName2));
        Set<Node> nodesSet2 = nodes.get(listenerName2);
        Assert.assertEquals(1, nodesSet2.size());
        nodesSet2.forEach(node -> {
            Assert.assertEquals(node.host(), host2);
            Assert.assertEquals(node.port(), port2);
        });
    }

}
