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
package io.streamnative.pulsar.handlers.kop.coordinator.group;

import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.kafka.common.record.CompressionType;

/**
 * Offset configuration.
 */
@Builder
@Data
@Accessors(fluent = true)
public class OffsetConfig {

    public static final int DefaultMaxMetadataSize = 4096;
    public static final long DefaultOffsetsRetentionMs = 24 * 60 * 60 * 1000L;
    public static final long DefaultOffsetsRetentionCheckIntervalMs = 600000L;
    public static final int DefaultOffsetsNumPartitions = KafkaServiceConfiguration.DefaultOffsetsTopicNumPartitions;

    @Default
    @NonNull
    private String offsetsTopicName = null;
    @Default
    private int maxMetadataSize = DefaultMaxMetadataSize;
    @Default
    private CompressionType offsetsTopicCompressionType = CompressionType.NONE;
    @Default
    private long offsetsRetentionMs = DefaultOffsetsRetentionMs;
    @Default
    private long offsetsRetentionCheckIntervalMs = DefaultOffsetsRetentionCheckIntervalMs;
    @Default
    private int offsetsTopicNumPartitions = DefaultOffsetsNumPartitions;
}
