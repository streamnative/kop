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
package io.streamnative.pulsar.handlers.kop.systopic;

import static io.streamnative.pulsar.handlers.kop.systopic.SystemTopicProducerStateClient.TOPIC_NAME_PROP;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
@AllArgsConstructor
public class SystemTopicProducerStateWriter implements SystemTopicClient.Writer<ByteBuffer> {

    private final SystemTopicClient<ByteBuffer> systemTopicClient;
    private final Producer<ByteBuffer> producer;

    @Override
    public MessageId write(ByteBuffer bytes) throws PulsarClientException {
        return producer.newMessage()
                .property(TOPIC_NAME_PROP, systemTopicClient.getTopicName().toString())
                .value(bytes)
                .send();
    }

    @Override
    public CompletableFuture<MessageId> writeAsync(ByteBuffer buffer) {
        return producer.newMessage().value(buffer).sendAsync().whenComplete(((messageId, throwable) -> {
            if (throwable != null) {
                log.error("Failed to write msg for system topic {}", systemTopicClient.getTopicName(), throwable);
                return;
            }
            log.info("Success to write msg for system topic {}", systemTopicClient.getTopicName());
        }));
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return producer.closeAsync();
    }

    @Override
    public SystemTopicClient<ByteBuffer> getSystemTopicClient() {
        return systemTopicClient;
    }
}

