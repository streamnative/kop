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
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.common.policies.data.ConsumerStats;


/**
 * The formatter for conversion between Kafka records and Bookie entries.
 */
public interface EntryFormatter {

    /**
     * Encode Kafka records to a ByteBuf.
     *
     * @param records messages with Kafka's format
     * @param numMessages the number of messages
     * @return the ByteBuf of an entry that is to be written to Bookie
     */
    ByteBuf encode(final MemoryRecords records, final int numMessages);

    /**
     * Decode a stream of entries to Kafka records.
     * It should be noted that this method is responsible for releasing the entries.
     *
     * @param entries the list of entries
     * @param magic the Kafka record batch's magic value
     * @return the Kafka records
     */
    MemoryRecords decode(final List<Entry> entries, final byte magic);

    /**
     * Get the number of messages from MemoryRecords.
     * Since MemoryRecords doesn't provide a way to get the number of messages. We need to iterate over the whole
     * MemoryRecords object. So we use a helper method to get the number of messages that can be passed to
     * {@link EntryFormatter#encode(MemoryRecords, int)} and metrics related methods as well.
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

    /**
     * Update Consumer Stats.
     *
     * @param records messages with Kafka's format
     * @param consumerFuture pulsar internal consumer
     */
    static void updateConsumerStats(final MemoryRecords records, CompletableFuture<Consumer> consumerFuture) {
        ConsumerStats consumerStats = new ConsumerStats();
        consumerFuture.whenComplete((consumer, throwable) -> {
            if (consumer == null || throwable != null) {
                return;
            }
            consumerStats.bytesOutCounter = records.sizeInBytes();
            consumerStats.msgOutCounter = parseNumMessages(records);
            consumer.updateStats(consumerStats);
        });
    }
}
