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

import java.lang.reflect.InvocationTargetException;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A completable class of org.apache.kafka.clients.consumer.OffsetAndMetadata.
 */
@AllArgsConstructor
@Getter
public class TopicOffsetAndMetadata {

    private String topic;
    private int partition;
    private long offset;

    public <T> T createTopicPartition(final Class<T> clazz) {
        try {
            return clazz.getConstructor(
                    String.class, int.class
            ).newInstance(topic, partition);
        } catch (InvocationTargetException
                | InstantiationException
                | IllegalAccessException
                | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public <T> T createOffsetAndMetadata(final Class<T> clazz) {
        try {
            return clazz.getConstructor(
                    long.class
            ).newInstance(offset);
        } catch (InstantiationException
                | IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }

    }
}
