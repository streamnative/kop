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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClientBase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;


@Slf4j
public class SystemTopicProducerStateClient extends SystemTopicClientBase<ByteBuffer> {

    private static final String TOPIC_NAME_PROP = "topic_name";

    private final io.streamnative.pulsar.handlers.kop.SystemTopicClient systemTopicClient;

    @AllArgsConstructor
    public static class SystemTopicProducerStateReader implements Reader<ByteBuffer> {

        private final SystemTopicClient<ByteBuffer> systemTopicClient;
        private final org.apache.pulsar.client.api.Reader<ByteBuffer> reader;

        private void readLoop(CompletableFuture<Message<ByteBuffer>> result,
                              Queue<Message<ByteBuffer>> internalMsgQueue) {
            hasMoreEventsAsync()
                    .thenComposeAsync(hasMore -> {
                        if (hasMore) {
                            return reader.readNextAsync();
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    }).thenComposeAsync(message -> {
                        if (message == null) {
                            if (internalMsgQueue.size() > 0) {
                                result.complete(internalMsgQueue.poll());
                            } else {
                                result.complete(null);
                            }
                            return null;
                        }
                        if (belongToThisTopic(message)) {
                            internalMsgQueue.add(message);
                            if (internalMsgQueue.size() > 1) {
                                internalMsgQueue.poll();
                            }
                        }
                        readLoop(result, internalMsgQueue);
                        return null;
                    }).exceptionally(throwable -> {
                        log.error("Failed to read message form system topic {}.",
                                systemTopicClient.getTopicName(), throwable);
                        result.completeExceptionally(throwable.getCause());
                        return null;
                    });
        }

        private boolean belongToThisTopic(Message<ByteBuffer> message) {
            return message.getProperty(TOPIC_NAME_PROP).equals(systemTopicClient.getTopicName().toString());
        }

        @Override
        public Message<ByteBuffer> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<ByteBuffer>> readNextAsync() {
            CompletableFuture<Message<ByteBuffer>> lastMsg = new CompletableFuture<>();
            readLoop(lastMsg, new LinkedBlockingQueue<>());
            return lastMsg;
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            return reader.hasMessageAvailable();
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            return reader.hasMessageAvailableAsync();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return reader.closeAsync();
        }

        @Override
        public SystemTopicClient<ByteBuffer> getSystemTopic() {
            return systemTopicClient;
        }
    }

    @AllArgsConstructor
    public static class SystemTopicProducerStateWriter implements Writer<ByteBuffer> {

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

    public SystemTopicProducerStateClient(io.streamnative.pulsar.handlers.kop.SystemTopicClient systemTopicClient,
                                          TopicName topicName) {
        super(null, topicName);
        this.systemTopicClient = systemTopicClient;
    }

    @Override
    protected CompletableFuture<Writer<ByteBuffer>> newWriterAsyncInternal() {
        return systemTopicClient.newProducerBuilder()
                .topic(topicName.toString()).createAsync()
                .thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("New system topic writer for topic {}", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new SystemTopicProducerStateWriter(this, producer));
                });
    }

    @Override
    protected CompletableFuture<Reader<ByteBuffer>> newReaderAsyncInternal() {
        return systemTopicClient.newReaderBuilder()
                .topic(topicName.toString())
                .readCompacted(true)
                .startMessageId(MessageId.earliest).createAsync()
                .thenCompose(reader -> {
                    if (log.isDebugEnabled()) {
                        log.debug("New system topic reader for topic {}", topicName);
                    }
                    return CompletableFuture.completedFuture(
                            new SystemTopicProducerStateReader(this, reader));
                });
    }

}
