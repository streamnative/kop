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
package io.streamnative.pulsar.handlers.kop.utils.delayed;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.kafka.common.TopicPartition;

/**
 * Delayed operation key.
 */
public interface DelayedOperationKey {

    /**
     * Key label.
     *
     * @return key label.
     */
    String keyLabel();

    /**
     * Member key.
     */
    @Data
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    class MemberKey implements DelayedOperationKey {

        private final String groupId;
        private final String consumerId;

        @Override
        public String keyLabel() {
            return String.format("%s-%s", groupId, consumerId);
        }
    }

    /**
     * Group key.
     */
    @Data
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    class GroupKey implements DelayedOperationKey {

        private final String groupId;

        @Override
        public String keyLabel() {
            return groupId;
        }
    }

    /**
     * Topic key.
     */
    @Data
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    class TopicKey implements DelayedOperationKey {

        private final String topic;

        @Override
        public String keyLabel() {
            return topic;
        }
    }

    /**
     * Topic partition key.
     */
    @Data
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    class TopicPartitionOperationKey implements DelayedOperationKey {

        private final TopicPartition topicPartition;

        @Override
        public String keyLabel() {
            return String.format("%s-%d", topicPartition.topic(),
                    topicPartition.partition());
        }
    }

}
