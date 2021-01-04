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

import org.apache.kafka.common.record.MemoryRecords;

/**
 * Utils for MemoryRecords.
 */
public class MemoryRecordsUtils {

    public static String dumpValues(final MemoryRecords records) {
        final StringBuilder stringBuilder = new StringBuilder();
        records.records().forEach(record -> {
            stringBuilder.append(" [").append(record.offset()).append("] ");
            final String value = CoreUtils.bufferToString(record.value());
            if (value != null) {
                stringBuilder.append("'").append(value).append("'");
            } else {
                stringBuilder.append("null");
            }
        });
        return stringBuilder.toString();
    }
}
