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
package io.streamnative.pulsar.handlers.kop.utils;

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

/**
 * Validate TopicNameUtils.
 */
@Slf4j
public class TopicNameUtilsTest {

    @Test(timeOut = 20000)
    public void testTopicNameConvert() throws Exception {
        String topicName = "kopTopicNameConvert";
        int partitionNumber = 77;
        TopicPartition topicPartition = new TopicPartition(topicName, partitionNumber);

        String tenantName = "tenant_name";
        String nsName = "ns_name";
        NamespaceName ns = NamespaceName.get(tenantName, nsName);
        String expectedPulsarName = "persistent://" + tenantName + "/" + nsName + "/"
            + topicName + PARTITIONED_TOPIC_SUFFIX + partitionNumber;

        TopicName topicName1 = TopicNameUtils.pulsarTopicName(topicPartition, ns);
        TopicName topicName2 = TopicNameUtils.pulsarTopicName(topicName, partitionNumber, ns);

        assertTrue(topicName1.toString().equals(expectedPulsarName));
        assertTrue(topicName2.toString().equals(expectedPulsarName));

        TopicName topicName3 = TopicNameUtils.pulsarTopicName(topicName, ns);
        String expectedPulsarName3 = "persistent://" + tenantName + "/" + nsName + "/"
            + topicName;
        assertTrue(topicName3.toString().equals(expectedPulsarName3));
    }

    @Test(timeOut = 20000)
    public void testTopicNameUrlEncodeAndDecode() {
        String[] testTopicNames = new String[]{
                "topicName",
                "tenant/ns/topicName",
                "persistent://tenant/ns/topicName",
                "persistent://tenant/ns/topicName-1",
                "persistent://tenant/ns/topicName" + PARTITIONED_TOPIC_SUFFIX + "0"
        };

        for (String topicName : testTopicNames) {
            String encodedTopicName = TopicNameUtils.getTopicNameWithUrlEncoded(topicName);
            String decodedTopicName = TopicNameUtils.getTopicNameWithUrlDecoded(encodedTopicName);
            assertEquals(decodedTopicName, topicName);
        }

    }
}
