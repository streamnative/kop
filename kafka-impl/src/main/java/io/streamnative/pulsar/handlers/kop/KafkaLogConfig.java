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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.Records;

/**
 * KafkaLogConfig is ported from kafka.log.LogConfig.
 */
public class KafkaLogConfig {
    private static final Map<String, String> entries = defaultEntries();

    public static Map<String, String> getEntries() {
        return entries;
    }

    private static Map<String, String> defaultEntries() {
        return Collections.unmodifiableMap(new HashMap<String, String>(){{
            put(TopicConfig.SEGMENT_BYTES_CONFIG, Integer.toString(1024 * 1024 * 1024));
            put(TopicConfig.SEGMENT_MS_CONFIG, Long.toString(24 * 7 * 7 * 60 * 1000L));
            put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, "0");
            put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, Integer.toString(10 * 1024 * 1024));
            put(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, Long.toString(Long.MAX_VALUE));
            put(TopicConfig.FLUSH_MS_CONFIG, Long.toString(Long.MAX_VALUE));
            put(TopicConfig.RETENTION_BYTES_CONFIG, Long.toString(-1L));
            put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(24 * 7 * 60 * 60 * 60 * 1000L));
            put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(1000000 + Records.LOG_OVERHEAD));
            put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "4096");
            put(TopicConfig.DELETE_RETENTION_MS_CONFIG, Long.toString(24 * 60 * 60 * 1000L));
            put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, Long.toString(0L));
            put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "60000");
            put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.5");
            // Kafka's default value of cleanup.policy is "delete", but here we set it to "compact" because confluent
            // schema registry needs this config value.
            put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact");
            put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");
            put(TopicConfig.COMPRESSION_TYPE_CONFIG, "producer");
            put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
            put(TopicConfig.PREALLOCATE_CONFIG, "false");
            put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, "2.0");
            put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime");
            put(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, Long.toString(Long.MAX_VALUE));
            put(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, "true");
        }});
    }
}
