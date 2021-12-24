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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

@Slf4j
@AllArgsConstructor
public class SystemTopicProducerStateReader implements SystemTopicClient.Reader<ByteBuffer> {

    private final SystemTopicClient<ByteBuffer> systemTopicClient;
    private final Reader<ByteBuffer> reader;

    private void readLoop(CompletableFuture<Message<ByteBuffer>> result,
                          Deque<Message<ByteBuffer>> internalMsgQueue) {
        hasMoreEventsAsync()
                .thenAccept(hasMore -> {
                    log.info("HasMore: {}", hasMore);
                    if (hasMore) {
                        reader.readNextAsync().thenAccept(message -> {
                            log.info("HasMore2: {}", hasMore);
                            if (message == null) {
                                if (internalMsgQueue.size() > 0) {
                                    result.complete(internalMsgQueue.pollLast());
                                } else {
                                    result.complete(null);
                                }
                                return;
                            }
                            if (belongToThisTopic(message)) {
                                internalMsgQueue.add(message);
                                if (internalMsgQueue.size() > 1) {
                                    internalMsgQueue.pollFirst();
                                }
                            }
                            readLoop(result, internalMsgQueue);
                        });
                    } else {
                        if (internalMsgQueue.size() > 0) {
                            result.complete(internalMsgQueue.pollLast());
                        } else {
                            result.complete(null);
                        }
                    }
                }).exceptionally(throwable -> {
                    log.error("Failed to read message form system topic {}.",
                            systemTopicClient.getTopicName(), throwable);
                    result.completeExceptionally(throwable.getCause());
                    return null;
                });
    }

    private boolean belongToThisTopic(Message<ByteBuffer> message) {
        log.info("WK {} {}", message, message.getProperties());
        return message.getProperty(TOPIC_NAME_PROP).equals(systemTopicClient.getTopicName().toString());
    }

    @Override
    public Message<ByteBuffer> readNext() throws PulsarClientException {
        return reader.readNext();
    }

    @Override
    public CompletableFuture<Message<ByteBuffer>> readNextAsync() {
        CompletableFuture<Message<ByteBuffer>> lastMsg = new CompletableFuture<>();
        readLoop(lastMsg, new ArrayDeque<>());
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

