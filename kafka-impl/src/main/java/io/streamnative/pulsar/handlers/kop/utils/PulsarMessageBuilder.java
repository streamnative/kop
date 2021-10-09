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

import static com.google.common.base.Preconditions.checkArgument;

import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;

/**
 * Manually build {@link MessageImpl}.
 */
public class PulsarMessageBuilder {
    private static final ByteBuffer EMPTY_CONTENT = ByteBuffer.allocate(0);
    private static final Schema<byte[]> SCHEMA = Schema.BYTES;

    private final transient MessageMetadata metadata;
    private transient ByteBuffer content;

    private static final FastThreadLocal<MessageMetadata> LOCAL_MESSAGE_METADATA =
            new FastThreadLocal<MessageMetadata>() {
                @Override
                protected MessageMetadata initialValue() {
                    return new MessageMetadata();
                }
            };

    private PulsarMessageBuilder() {
        metadata = LOCAL_MESSAGE_METADATA.get();
        metadata.clear();
        this.content = EMPTY_CONTENT;
    }

    public static PulsarMessageBuilder newBuilder() {
        return new PulsarMessageBuilder();
    }

    public PulsarMessageBuilder keyBytes(byte[] key) {
        metadata.setPartitionKey(Base64.getEncoder().encodeToString(key));
        metadata.setPartitionKeyB64Encoded(true);
        return this;
    }

    public PulsarMessageBuilder orderingKey(byte[] orderingKey) {
        metadata.setOrderingKey(orderingKey);
        return this;
    }

    public PulsarMessageBuilder value(byte[] value) {
        if (value == null) {
            metadata.setNullValue(true);
            return this;
        }
        this.content = ByteBuffer.wrap(SCHEMA.encode(value));
        return this;
    }

    public PulsarMessageBuilder property(String name, String value) {
        checkArgument(name != null, "Need Non-Null name");
        checkArgument(value != null, "Need Non-Null value for name: " + name);
        metadata.addProperty()
                .setKey(name)
                .setValue(value);
        return this;
    }

    public PulsarMessageBuilder properties(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            checkArgument(entry.getKey() != null, "Need Non-Null key");
            checkArgument(entry.getValue() != null, "Need Non-Null value for key: " + entry.getKey());
            metadata.addProperty()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue());
        }

        return this;
    }

    public PulsarMessageBuilder eventTime(long timestamp) {
        checkArgument(timestamp > 0, "Invalid timestamp : '%s'", timestamp);
        metadata.setEventTime(timestamp);
        return this;
    }

    public MessageMetadata getMetadataBuilder() {
        return metadata;
    }

    public PulsarMessageBuilder sequenceId(long sequenceId) {
        checkArgument(sequenceId >= 0);
        metadata.setSequenceId(sequenceId);
        return this;
    }

    public Message<byte[]> getMessage() {
        return MessageImpl.create(metadata, content, SCHEMA);
    }

}