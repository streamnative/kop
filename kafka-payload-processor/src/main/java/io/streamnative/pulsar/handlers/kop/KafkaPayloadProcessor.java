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

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessagePayloadImpl;
import org.apache.pulsar.client.impl.MessagePayloadUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;

/**
 * Process Kafka messages so that Pulsar consumer can recognize.
 */
public class KafkaPayloadProcessor implements MessagePayloadProcessor {

    private static final StringDeserializer deserializer = new StringDeserializer();
    private static final FastThreadLocal<SingleMessageMetadata> LOCAL_SINGLE_MESSAGE_METADATA =
            new FastThreadLocal<SingleMessageMetadata>() {
        @Override
        protected SingleMessageMetadata initialValue() throws Exception {
            return new SingleMessageMetadata();
        }
    };

    @Override
    public <T> void process(MessagePayload payload,
                            MessagePayloadContext context,
                            Schema<T> schema,
                            Consumer<Message<T>> messageConsumer) throws Exception {
        if (!isKafkaFormat(context)) {
            DEFAULT.process(payload, context, schema, messageConsumer);
            return;
        }

        final ByteBuf buf = MessagePayloadUtils.convertToByteBuf(payload);
        try {
            final MemoryRecords records = MemoryRecords.readableRecords(buf.nioBuffer());
            int numMessages = 0;
            for (Record ignored : records.records()) {
                numMessages++;
            }
            int index = 0;
            for (RecordBatch batch : records.batches()) {
                if (batch.isControlBatch()) {
                    continue;
                }
                // TODO: Currently KoP doesn't support multi batches in an entry so it works well at this moment. After
                //  we supported multi batches in future, the following code should be changed. See
                //  https://github.com/streamnative/kop/issues/537 for details.
                for (Record record : records.records()) {
                    final MessagePayload singlePayload = newByteBufFromRecord(record);
                    try {
                        messageConsumer.accept(context.getMessageAt(index, numMessages, singlePayload, true, schema));
                    } finally {
                        index++;
                        singlePayload.release();
                    }
                }
            }
        } finally {
            buf.release();
        }
    }

    private static boolean isKafkaFormat(final MessagePayloadContext context) {
        final String value = context.getProperty("entry.format");
        return value != null && value.equalsIgnoreCase("kafka");
    }

    private static byte[] bufferToBytes(final ByteBuffer buffer) {
        final byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return data;
    }

    private MessagePayload newByteBufFromRecord(final Record record) {
        final SingleMessageMetadata singleMessageMetadata = LOCAL_SINGLE_MESSAGE_METADATA.get();
        singleMessageMetadata.clear();
        if (record.hasKey()) {
            final byte[] data = bufferToBytes(record.key());
            // It's okay to pass a null topic because it's not used in StringDeserializer
            singleMessageMetadata.setPartitionKey(deserializer.deserialize(null, data));
            singleMessageMetadata.setOrderingKey(data);
        }

        final ByteBuffer valueBuffer;
        if (record.hasValue()) {
            valueBuffer = record.value();
            singleMessageMetadata.setPayloadSize(record.valueSize());
        } else {
            valueBuffer = null;
            singleMessageMetadata.setNullValue(true);
            singleMessageMetadata.setPayloadSize(0);
        }

        for (Header header : record.headers()) {
            singleMessageMetadata.addProperty()
                    .setKey(header.key())
                    .setValue(new String(header.value(), StandardCharsets.UTF_8));
        }

        final ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(
                4 + singleMessageMetadata.getSerializedSize() + record.valueSize());
        buf.writeInt(singleMessageMetadata.getSerializedSize());
        singleMessageMetadata.writeTo(buf);
        if (valueBuffer != null) {
            buf.writeBytes(valueBuffer);
        }
        return MessagePayloadImpl.create(buf);
    }
}
