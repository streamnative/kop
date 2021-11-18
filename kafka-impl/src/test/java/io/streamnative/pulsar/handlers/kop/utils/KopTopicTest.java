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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

/**
 * Test for KopTopic.
 */
public class KopTopicTest {

    @Test
    public void testConstructor() {
        KopTopic topic;
        try {
            topic = new KopTopic("my-topic", null);
            fail();
        } catch (KopTopic.KoPTopicNotInitializedException e) {
            assertEquals(e.getMessage(), "KopTopic is not initialized");
        }

        String namespacePrefix = "my-tenant/my-ns";

        topic = new KopTopic("my-topic", namespacePrefix);
        assertEquals(topic.getOriginalName(), "my-topic");
        assertEquals(topic.getFullName(), "persistent://my-tenant/my-ns/my-topic");

        topic = new KopTopic("my-tenant-2/my-ns-2/my-topic", namespacePrefix);
        assertEquals(topic.getOriginalName(), "my-tenant-2/my-ns-2/my-topic");
        assertEquals(topic.getFullName(), "persistent://my-tenant-2/my-ns-2/my-topic");

        topic = new KopTopic("persistent://my-tenant-3/my-ns-3/my-topic", namespacePrefix);
        assertEquals(topic.getOriginalName(), "persistent://my-tenant-3/my-ns-3/my-topic");
        assertEquals(topic.getFullName(), topic.getOriginalName());

        try {
            topic = new KopTopic("my-ns/my-topic", namespacePrefix);
            fail();
        } catch (KopTopic.KoPTopicIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid short topic name"));
        }

        try {
            topic = new KopTopic("my-topic", null);
            fail();
        } catch (KopTopic.KoPTopicIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid short topic name"));
        }
        try {
            topic = new KopTopic("persistent://my-topic", namespacePrefix);
        } catch (KopTopic.KoPTopicIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid topic name"));
        }
    }

    @Test
    public void testGetPartitionName() {
        String namespacePrefix = "my-tenant/my-ns";

        KopTopic topic = new KopTopic("my-topic", namespacePrefix);
        assertEquals(topic.getPartitionName(0), "persistent://my-tenant/my-ns/my-topic-partition-0");
        assertEquals(topic.getPartitionName(12), "persistent://my-tenant/my-ns/my-topic-partition-12");
        try {
            topic.getPartitionName(-1);
            fail();
        } catch (KopTopic.KoPTopicIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid partition"));
        }
    }

    @Test
    public void testRemoveDefaultNamespacePrefix() {
        String namespacePrefix = "my-tenant/my-ns";

        final String topic1 = "persistent://my-tenant/my-ns/my-topic";
        final String topic2 = "persistent://my-tenant/another-ns/my-topic";
        assertEquals(KopTopic.removeDefaultNamespacePrefix(topic1, namespacePrefix), "my-topic");
        assertEquals(KopTopic.removeDefaultNamespacePrefix(topic2, namespacePrefix), topic2);
    }
}
