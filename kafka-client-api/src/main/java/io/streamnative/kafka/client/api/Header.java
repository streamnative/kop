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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A compatible class of Kafka header (org.apache.kafka.common.header.Header).
 */
@AllArgsConstructor
@Getter
public class Header {

    private final String key;
    private final String value;

    private static Header fromHeader(final Object originalHeader) {
        if (originalHeader == null) {
            return null;
        }
        final Class<?> clazz = originalHeader.getClass();
        return new Header(
                (String) ReflectionUtils.invoke(clazz, "key", originalHeader),
                new String((byte[]) ReflectionUtils.invoke(clazz, "value", originalHeader), StandardCharsets.UTF_8));
    }

    /**
     * Create a list of Header from a list of Kafka header.
     *
     * @param originalHeaders the list of Kafka headers whose type is org.apache.kafka.common.header.Header
     * @return a list of converted Header
     */
    public static List<Header> fromHeaders(final Object[] originalHeaders) {
        if (originalHeaders == null || originalHeaders.length == 0) {
            return null;
        }
        final List<Header> headers = new ArrayList<>();
        for (Object header : originalHeaders) {
            headers.add(fromHeader(header));
        }
        return headers;
    }

    /**
     * Convert a list of Header to a list of Kafka Header.
     *
     * @param headers a list of Header
     * @param constructor the binary function of Kafka header's constructor
     * @param <T> the type of Kafka header that is usually org.apache.kafka.common.header.RecordHeader
     * @return the converted list of Kafka header
     */
    public static <T> List<T> toHeaders(final List<Header> headers, final BiFunction<String, byte[], T> constructor) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        return headers.stream()
                .map(header -> (header != null)
                        ? constructor.apply(header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8))
                        : null)
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Header header = (Header) o;
        return Objects.equals(key, header.key) && Objects.equals(value, header.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
