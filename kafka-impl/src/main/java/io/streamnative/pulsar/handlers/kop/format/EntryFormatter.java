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
package io.streamnative.pulsar.handlers.kop.format;

import io.netty.buffer.ByteBuf;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.record.MemoryRecords;


/**
 * The formatter for conversion between Kafka records and Bookie entries.
 */
public interface EntryFormatter {

    /**
     * Encode Kafka records to a ByteBuf.
     *
     * @param records messages with Kafka's format
     * @return the ByteBuf of an entry that is to be written to Bookie
     */
    ByteBuf encode(MemoryRecords records);

    /**
     * Decode a stream of entries to Kafka records.
     *
     * @param entries the list of entries
     * @param magic the Kafka record batch's magic value
     * @return the Kafka records
     */
    MemoryRecords decode(List<Entry> entries, byte magic);
}