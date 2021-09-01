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

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.NonNull;
import org.apache.kafka.common.PartitionInfo;

/**
 * A common interface of Kafka consumer.
 */
public interface Consumer<K, V> extends Closeable {

    void subscribe(Collection<String> topics);

    default void subscribe(String topic) {
        subscribe(Collections.singleton(topic));
    }

    @NonNull
    List<ConsumerRecord<K, V>> receive(long timeoutMs);

    /**
     * Receive messages as much as possible until the number of received messages time exceeds limit or timed out.
     *
     * @param maxNumMessages the limited number of received messages
     * @param timeoutMs the total timeout in milliseconds for this method
     * @return the total received messages
     */
    default List<ConsumerRecord<K, V>> receiveUntil(int maxNumMessages, long timeoutMs) {
        final int pollTimeoutMs = 100;
        final List<ConsumerRecord<K, V>> records = new ArrayList<>();
        final AtomicInteger numReceived = new AtomicInteger(0);

        long elapsedTimeMs = 0;
        while (numReceived.get() < maxNumMessages && elapsedTimeMs < timeoutMs) {
            receive(pollTimeoutMs).forEach(record -> {
                records.add(record);
                numReceived.incrementAndGet();
            });
            elapsedTimeMs += pollTimeoutMs; // it may not be accurate, but it's not required to be accurate
        }
        return records;
    }

    Map<String, List<PartitionInfo>> listTopics(long timeoutMS);

    void commitOffsetSync(List<TopicOffsetAndMetadata> offsets, Duration timeout);
}
