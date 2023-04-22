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
package io.streamnative.pulsar.handlers.kop;

import static org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import static org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;

import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.pulsar.common.naming.TopicName;

/**
 * The topic and its metadata (number of partitions or the error).
 */
@AllArgsConstructor
@Getter
public class TopicAndMetadata {

    private final String topic;
    private final int numPartitions;
    private final Errors error;

    public static TopicAndMetadata success(String topic, int numPartitions) {
        return new TopicAndMetadata(topic, numPartitions, Errors.NONE);
    }

    public static TopicAndMetadata failure(String topic, Errors error) {
        return new TopicAndMetadata(topic, -1, error);
    }

    public boolean hasNoError() {
        return error == Errors.NONE;
    }

    public CompletableFuture<TopicMetadata> lookupAsync(
            final Function<TopicName, CompletableFuture<PartitionMetadata>> lookupFunction,
            final Function<String, String> getOriginalTopic,
            final String metadataNamespace) {
        return CoreUtils.waitForAll(stream()
                .map(TopicName::get)
                .map(lookupFunction)
                .collect(Collectors.toList()), partitionMetadataList ->
                new TopicMetadata(
                        error,
                        getOriginalTopic.apply(topic),
                        KopTopic.isInternalTopic(topic, metadataNamespace),
                        partitionMetadataList
                ));
    }

    private Stream<String> stream() {
        if (numPartitions > 0) {
            return IntStream.range(0, numPartitions)
                    .mapToObj(i -> topic + "-partition-" + i);
        } else {
            return Stream.of(topic);
        }
    }

    public TopicMetadata toTopicMetadata(final Function<String, String> getOriginalTopic,
                                         final String metadataNamespace) {
        return new TopicMetadata(
                error,
                getOriginalTopic.apply(topic),
                KopTopic.isInternalTopic(topic, metadataNamespace),
                Collections.emptyList()
        );
    }
}
