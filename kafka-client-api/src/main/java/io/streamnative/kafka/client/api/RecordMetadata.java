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
package io.streamnative.kafka.client.api;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A compatible class of Kafka's send result (org.apache.kafka.clients.producer.RecordMetadata).
 */
@Getter
@RequiredArgsConstructor
public class RecordMetadata {

    private final String topic;
    private final int partition;
    private final long offset;

    @Override
    public String toString() {
        return topic + "-" + partition + "@" + offset;
    }

    /**
     * Create the RecordMetadata instance from Kafka's send result.
     *
     * @param originalMetadata the original Kafka's RecordMetadata
     * @param <T> it should be org.apache.kafka.clients.producer.RecordMetadata
     * @return the RecordMetadata instance
     */
    public static <T> RecordMetadata create(final T originalMetadata) {
        final Class<?> clazz = originalMetadata.getClass();
        return new RecordMetadata(
                (String) ReflectionUtils.invoke(clazz, "topic", originalMetadata),
                (int) ReflectionUtils.invoke(clazz, "partition", originalMetadata),
                (long) ReflectionUtils.invoke(clazz, "offset", originalMetadata));
    }
}
