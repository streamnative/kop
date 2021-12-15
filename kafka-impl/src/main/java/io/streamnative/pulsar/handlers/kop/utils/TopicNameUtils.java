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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import lombok.NonNull;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Utils for Pulsar TopicName.
 */
public class TopicNameUtils {

    public static TopicName pulsarTopicName(TopicPartition topicPartition, NamespaceName namespace) {
        return pulsarTopicName(topicPartition.topic(), topicPartition.partition(), namespace);
    }

    public static TopicName pulsarTopicName(TopicPartition topicPartition) {
        return pulsarTopicName(topicPartition.topic(), topicPartition.partition());
    }

    private static TopicName pulsarTopicName(String topic, int partitionIndex) {
        return TopicName.get(topic + PARTITIONED_TOPIC_SUFFIX + partitionIndex);
    }

    public static TopicName pulsarTopicName(String topic, NamespaceName namespace) {
        return TopicName.get(TopicDomain.persistent.value(), namespace, topic);
    }

    public static TopicName pulsarTopicName(String topic) {
        return TopicName.get(topic);
    }

    public static TopicName pulsarTopicName(String topic, int partitionIndex, NamespaceName namespace) {
        if (topic.startsWith(TopicDomain.persistent.value())) {
            topic = topic.replace(TopicDomain.persistent.value() + "://", "");
        }

        if (topic.contains(namespace.getNamespaceObject().toString())) {
            topic = topic.replace(namespace.getNamespaceObject().toString() + "/", "");
        }
        return TopicName.get(TopicDomain.persistent.value(),
            namespace,
            topic + PARTITIONED_TOPIC_SUFFIX + partitionIndex);
    }

    public static String getPartitionedTopicNameWithoutPartitions(TopicName topicName) {
        String localName = topicName.getPartitionedTopicName();
        if (localName.contains(PARTITIONED_TOPIC_SUFFIX)) {
            return localName.substring(0, localName.lastIndexOf(PARTITIONED_TOPIC_SUFFIX));
        } else {
            return localName;
        }
    }

    /**
     * Get an url encoded topic name.
     */
    public static @NonNull String getTopicNameWithUrlEncoded(String topicName) {
        String encodedTopicName = "";
        try {
            encodedTopicName = URLEncoder.encode(topicName, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException ignore) {
            // The exception will never happen, because the charset always exists.
        }
        return encodedTopicName;
    }

    /**
     * Get an url decoded topic name.
     */
    public static @NonNull String getTopicNameWithUrlDecoded(String encodedTopicName) {
        String topicName = "";
        try {
            topicName = URLDecoder.decode(encodedTopicName, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException ignore) {
            // The exception will never happen, because the charset always exists.
        }
        return topicName;
    }

}
