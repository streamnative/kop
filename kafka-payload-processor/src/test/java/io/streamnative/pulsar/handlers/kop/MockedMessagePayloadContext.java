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
import io.netty.buffer.Unpooled;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

public class MockedMessagePayloadContext implements MessagePayloadContext {

    private final int numMessages;
    private final MessageIdImpl messageId;

    public MockedMessagePayloadContext(final int numMessages,
                                       final MessageIdImpl messageId) {
        this.numMessages = numMessages;
        this.messageId = messageId;
    }

    @Override
    public String getProperty(String key) {
        return "kafka";
    }

    @Override
    public int getNumMessages() {
        return numMessages;
    }

    @Override
    public boolean isBatch() {
        return true;
    }

    @Override
    public <T> Message<T> getMessageAt(int index,
                                       int numMessages,
                                       MessagePayload payload,
                                       boolean containMetadata,
                                       Schema<T> schema) {
        final ByteBuf payloadBuffer = Unpooled.wrappedBuffer(payload.copiedBuffer());

        final MessageMetadata messageMetadata = new MessageMetadata();
        messageMetadata.setNumMessagesInBatch(numMessages);

        Commands.skipMessageMetadata(payloadBuffer);

        final BatchMessageIdImpl batchMessageId = new BatchMessageIdImpl(
                messageId.getLedgerId(), messageId.getEntryId(), messageId.getPartitionIndex(), index);
        try {
            return MessageImpl.create("",
                    batchMessageId,
                    messageMetadata,
                    payloadBuffer,
                    Optional.empty(),
                    null,
                    schema,
                    0,
                    false);
        } finally {
            payloadBuffer.release();
        }
    }

    @Override
    public <T> Message<T> asSingleMessage(MessagePayload payload, Schema<T> schema) {
        // Not used
        return null;
    }
}
