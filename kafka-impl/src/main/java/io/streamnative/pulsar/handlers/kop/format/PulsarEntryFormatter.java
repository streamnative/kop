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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.utils.PulsarMessageBuilder;
import java.util.List;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;


/**
 * The entry formatter that uses Pulsar's format.
 */
@Slf4j
public class PulsarEntryFormatter extends AbstractEntryFormatter {
    //// for Batch messages
    private static final int INITIAL_BATCH_BUFFER_SIZE = 1024;
    private static final int MAX_MESSAGE_BATCH_SIZE_BYTES = 128 * 1024;

    @Override
    public EncodeResult encode(final EncodeRequest encodeRequest) {
        final MemoryRecords records = encodeRequest.getRecords();
        final int numMessages = EntryFormatter.parseNumMessages(records);
        long currentBatchSizeBytes = 0;
        int numMessagesInBatch = 0;

        long sequenceId = -1;

        ByteBuf batchedMessageMetadataAndPayload = PulsarByteBufAllocator.DEFAULT
                .buffer(Math.min(INITIAL_BATCH_BUFFER_SIZE, MAX_MESSAGE_BATCH_SIZE_BYTES));
        List<MessageImpl<byte[]>> messages = Lists.newArrayListWithExpectedSize(numMessages);
        final MessageMetadata msgMetadata = new MessageMetadata();

        records.batches().forEach(recordBatch -> {
            boolean controlBatch = recordBatch.isControlBatch();
            StreamSupport.stream(recordBatch.spliterator(), true).forEachOrdered(record -> {
                MessageImpl<byte[]> message = recordToEntry(record);
                messages.add(message);
                if (recordBatch.isTransactional()) {
                    msgMetadata.setTxnidMostBits(recordBatch.producerId());
                    msgMetadata.setTxnidLeastBits(recordBatch.producerEpoch());
                }
                if (controlBatch) {
                    ControlRecordType controlRecordType = ControlRecordType.parse(record.key());
                    switch (controlRecordType) {
                        case ABORT:
                            msgMetadata.setMarkerType(MarkerType.TXN_ABORT_VALUE);
                            break;
                        case COMMIT:
                            msgMetadata.setMarkerType(MarkerType.TXN_COMMIT_VALUE);
                            break;
                        default:
                            msgMetadata.setMarkerType(MarkerType.UNKNOWN_MARKER_VALUE);
                            break;
                    }
                }
            });
        });

        for (MessageImpl<byte[]> message : messages) {
            if (++numMessagesInBatch == 1) {
                // msgMetadata will set publish time here
                sequenceId = Commands.initBatchMessageMetadata(msgMetadata, message.getMessageBuilder());
            }
            currentBatchSizeBytes += message.getDataBuffer().readableBytes();
            if (log.isDebugEnabled()) {
                log.debug("recordsToByteBuf , sequenceId: {}, numMessagesInBatch: {}, currentBatchSizeBytes: {} ",
                        sequenceId, numMessagesInBatch, currentBatchSizeBytes);
            }

            final MessageMetadata msgBuilder = message.getMessageBuilder();
            batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(msgBuilder,
                    message.getDataBuffer(), batchedMessageMetadataAndPayload);
        }

        msgMetadata.setNumMessagesInBatch(numMessagesInBatch);

        ByteBuf buf = Commands.serializeMetadataAndPayload(ChecksumType.Crc32c,
                msgMetadata,
                batchedMessageMetadataAndPayload);

        batchedMessageMetadataAndPayload.release();

        return EncodeResult.get(records, buf, numMessages, numMessagesInBatch);
    }

    @Override
    public DecodeResult decode(final List<Entry> entries, final byte magic) {
        return super.decode(entries, magic);
    }

    // convert kafka Record to Pulsar Message.
    // convert kafka Record to Pulsar Message.
    // called when publish received Kafka Record into Pulsar.
    private static MessageImpl<byte[]> recordToEntry(Record record) {

        PulsarMessageBuilder builder = PulsarMessageBuilder.newBuilder();

        // key
        if (record.hasKey()) {
            byte[] key = new byte[record.keySize()];
            record.key().get(key);
            builder.keyBytes(key);
            // reuse ordering key to avoid converting string < > bytes
            builder.orderingKey(key);
        }

        // value
        if (record.hasValue()) {
            byte[] value = new byte[record.valueSize()];
            record.value().get(value);
            builder.value(value);
        } else {
            builder.value(null);
        }

        // Following fields are required in `Commands.initBatchMessageMetadata`, but since we write to
        // bookie directly, broker won't make use of them. So here we just set trivial values.
        builder.getMetadataBuilder().setProducerName("");
        if (record.sequence() >= 0) {
            builder.sequenceId(record.sequence());
        } else {
            builder.sequenceId(0L);
        }

        // timestamp
        if (record.timestamp() >= 0) {
            builder.eventTime(record.timestamp());
            builder.getMetadataBuilder().setPublishTime(record.timestamp());
        } else {
            builder.getMetadataBuilder().setPublishTime(System.currentTimeMillis());
        }

        // header
        for (Header h : record.headers()) {
            builder.property(h.key(),
                    new String(h.value(), UTF_8));
        }

        return (MessageImpl<byte[]>) builder.getMessage();
    }

}
