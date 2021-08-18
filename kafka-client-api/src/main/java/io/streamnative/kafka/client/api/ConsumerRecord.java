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

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A completable class of org.apache.kafka.clients.consumer.ConsumerRecord.
 */
@AllArgsConstructor
@Getter
public class ConsumerRecord<K, V> {

    private final K key;
    private final V value;
    private final String topic;
    private final int partition;
    private final long offset;
    private final List<Header> headers;

    @SuppressWarnings("unchecked")
    public static <K, V, T> ConsumerRecord<K, V> create(T originalRecord) {
        final Class<?> clazz = originalRecord.getClass();
        final Object originalHeaders = ReflectionUtils.invoke(clazz, "headers", originalRecord);
        final List<Header> headers = Header.fromHeaders(
                (Object[]) ReflectionUtils.invoke(originalHeaders.getClass(), "toArray", originalHeaders));

        return new ConsumerRecord<>((K) ReflectionUtils.invoke(clazz, "key", originalRecord),
                (V) ReflectionUtils.invoke(clazz, "value", originalRecord),
                (String) ReflectionUtils.invoke(clazz, "topic", originalRecord),
                (int) ReflectionUtils.invoke(clazz, "partition", originalRecord),
                (long) ReflectionUtils.invoke(clazz, "offset", originalRecord),
                headers);
    }

    //support kafka message before 0.11.x
    public static <K, V, T> ConsumerRecord<K, V> createOldRecord(T originalRecord) {
        final Class<?> clazz = originalRecord.getClass();

        final List<Header> headerList = new ArrayList<>();
        headerList.add(new Header(null, null));

        return new ConsumerRecord<>((K) ReflectionUtils.invoke(clazz, "key", originalRecord),
                (V) ReflectionUtils.invoke(clazz, "value", originalRecord),
                (String) ReflectionUtils.invoke(clazz, "topic", originalRecord),
                (int) ReflectionUtils.invoke(clazz, "partition", originalRecord),
                (long) ReflectionUtils.invoke(clazz, "offset", originalRecord),
                headerList);
    }
}
