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

import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import io.streamnative.pulsar.handlers.kop.exceptions.KoPTopicException;
import java.util.function.Function;
import lombok.Getter;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.common.naming.TopicName;

/**
 * KopTopic maintains two topic name, one is the original topic name, the other is the full topic name used in Pulsar.
 * We shouldn't use the original topic name directly in KoP source code. Instead, we should
 *   1. getOriginalName() when read a Kafka request from client or write a Kafka response to client.
 *   2. getFullName() when access Pulsar resources.
 */
public class KopTopic {

    private static final String persistentDomain = "persistent://";

    public static String removeDefaultNamespacePrefix(String fullTopicName, String namespacePrefix) {
        final String topicPrefix = persistentDomain + namespacePrefix + "/";
        if (fullTopicName.startsWith(topicPrefix)) {
            return fullTopicName.substring(topicPrefix.length());
        } else {
            return fullTopicName;
        }
    }

    public static String removePersistentDomain(String fullTopicName) {
        if (fullTopicName.startsWith(persistentDomain)) {
            return fullTopicName.substring(persistentDomain.length());
        } else {
            return fullTopicName;
        }
    }

    @Getter
    private final String originalName;
    @Getter
    private final String fullName;

    public static class KoPTopicIllegalArgumentException extends KoPTopicException {

        public KoPTopicIllegalArgumentException(String message) {
            super(message);
        }
    }

    public KopTopic(String topic, String namespacePrefix) {
        originalName = topic;
        fullName = expandToFullName(topic, namespacePrefix);
    }

    private String expandToFullName(String topic, String namespacePrefix) {
        if (topic.startsWith(persistentDomain)) {
            if (topic.substring(persistentDomain.length()).split("/").length != 3) {
                throw new KoPTopicIllegalArgumentException("Invalid topic name '" + topic + "', it should be "
                        + " persistent://<tenant>/<namespace>/<topic>");
            }
            return topic;
        }

        String[] parts = topic.split("/");
        if (parts.length == 3) {
            return persistentDomain + topic;
        } else if (parts.length == 1 && namespacePrefix != null) {
            return persistentDomain + namespacePrefix + "/" + topic;
        } else {
            throw new KoPTopicIllegalArgumentException(
                    "Invalid short topic name '" + topic + "', it should be in the format"
                    + " of <tenant>/<namespace>/<topic> or <topic>");
        }
    }

    public String getPartitionName(int partition) {
        if (partition < 0) {
            throw new KoPTopicIllegalArgumentException(
                    "Invalid partition " + partition + ", it should be non-negative number");
        }
        return fullName + PARTITIONED_TOPIC_SUFFIX + partition;
    }

    public static boolean isFullTopicName(String topic) {
        return topic.startsWith(persistentDomain);
    }

    public static String toString(TopicPartition topicPartition, String namespacePrefix) {
        return (new KopTopic(topicPartition.topic(), namespacePrefix)).getPartitionName(topicPartition.partition());
    }

    public static String toString(String topic, int partition, String namespacePrefix) {
        return (new KopTopic(topic, namespacePrefix)).getPartitionName(partition);
    }

    private static boolean validateTopic(final String fullTopicName,
                                         final String namespace,
                                         final Function<String, Boolean> topicValidation) {
        final TopicName topicName = TopicName.get(fullTopicName);
        if (!topicName.getNamespacePortion().equals(namespace)) {
            return false;
        }

        final String localName = topicName.getLocalName();
        return topicValidation.apply(topicName.isPartitioned()
                ? localName.substring(0, localName.lastIndexOf(PARTITIONED_TOPIC_SUFFIX))
                : localName);
    }

    public static boolean isInternalTopic(final String fullTopicName, final String metadataNamespace) {
        return validateTopic(fullTopicName, metadataNamespace,
                topic -> topic.equals(GROUP_METADATA_TOPIC_NAME) || topic.equals(TRANSACTION_STATE_TOPIC_NAME));
    }

    public static boolean isGroupMetadataTopicName(final String fullTopicName, final String metadataNamespace) {
        return validateTopic(fullTopicName, metadataNamespace, topic -> topic.equals(GROUP_METADATA_TOPIC_NAME));
    }

    public static boolean isTransactionMetadataTopicName(final String fullTopicName, final String metadataNamespace) {
        return validateTopic(fullTopicName, metadataNamespace, topic -> topic.equals(TRANSACTION_STATE_TOPIC_NAME));
    }

}
