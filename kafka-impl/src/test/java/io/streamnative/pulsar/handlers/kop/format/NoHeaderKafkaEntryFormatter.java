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

import io.netty.buffer.Unpooled;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.record.MemoryRecords;

/**
 * The entry formatter that uses Kafka's format but has no header.
 */
public class NoHeaderKafkaEntryFormatter implements EntryFormatter {

    @Override
    public EncodeResult encode(final EncodeRequest encodeRequest) {
        final MemoryRecords records = encodeRequest.getRecords();
        final int numMessages = EntryFormatter.parseNumMessages(records);
        // The difference from KafkaEntryFormatter is here we don't add the header
        return EncodeResult.get(records, Unpooled.wrappedBuffer(records.buffer()), numMessages, 0);
    }

    @Override
    public DecodeResult decode(List<Entry> entries, byte magic) {
        // Do nothing
        return null;
    }
}
