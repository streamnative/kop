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

import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;

/**
 * The formatter for conversion between Kafka records and Bookie entries.
 */
public interface EntryFormatter {

    /**
     * Encode Kafka records to a ByteBuf.
     *
     * @param encodeRequest contains messages with Kafka's format
     * @return the EncodeResult contains the ByteBuf of an entry that is to be written to Bookie
     */
    EncodeResult encode(EncodeRequest encodeRequest);

    /**
     * Decode a stream of entries to Kafka records.
     * It should be noted that this method is responsible for releasing the entries.
     *
     * @param entries the list of entries
     * @param magic the Kafka record batch's magic value
     * @return the DecodeResult contains the Kafka records
     */
    DecodeResult decode(List<Entry> entries, byte magic);

    /**
     * Get the number of messages from MemoryRecords.
     * Since MemoryRecords doesn't provide a way to get the number of messages. We need to iterate over the whole
     * MemoryRecords object. So we use a helper method to get the number of messages that can be passed to
     * {@link EntryFormatter#encode(EncodeRequest)} and metrics related methods as well.
     *
     * @param records messages with Kafka's format
     * @return the number of messages
     */
    static int parseNumMessages(final MemoryRecords records) {
        int numMessages = 0;
        for (MutableRecordBatch batch : records.batches()) {
            numMessages += (batch.lastOffset() - batch.baseOffset() + 1);
        }
        return numMessages;
    }
}
