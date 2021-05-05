package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClientBase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class SystemTopicProducerStateClient extends SystemTopicClientBase<byte[]> {

    private final static String TOPIC_NAME_PROP = "topic_name";

    @AllArgsConstructor
    public static class SystemTopicProducerStateReader implements Reader<byte[]> {

        private final SystemTopicClient<byte[]> systemTopicClient;
        private final org.apache.pulsar.client.api.Reader<byte[]> reader;

        public CompletableFuture<Message<byte[]>> readLastValidMessage() {
            CompletableFuture<Message<byte[]>> lastMsg = new CompletableFuture<>();
            readLoop(lastMsg, new LinkedBlockingQueue<>());
            return lastMsg;
        }

        private void readLoop(CompletableFuture<Message<byte[]>> result,
                                                            Queue<Message<byte[]>> internalMsgQueue) {
            hasMoreEventsAsync()
                    .thenComposeAsync(hasMore -> {
                        if (hasMore) {
                            return readNextAsync();
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
//                            return CompletableFuture.completedFuture(null);
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

        private boolean belongToThisTopic(Message<byte[]> message) {
            return message.getProperty(TOPIC_NAME_PROP).equals(systemTopicClient.getTopicName().toString());
        }

        @Override
        public Message<byte[]> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<byte[]>> readNextAsync() {
            return reader.readNextAsync();
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
        public SystemTopicClient<byte[]> getSystemTopic() {
            return systemTopicClient;
        }
    }

    @AllArgsConstructor
    public static class SystemTopicProducerStateWriter implements Writer<byte[]> {

        private final SystemTopicClient<byte[]> systemTopicClient;
        private final Producer<byte[]> producer;

        @Override
        public MessageId write(byte[] bytes) throws PulsarClientException {
            return producer.newMessage()
                    .property(TOPIC_NAME_PROP, systemTopicClient.getTopicName().toString())
                    .value(bytes)
                    .send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(byte[] bytes) {
            return producer.newMessage().value(bytes).sendAsync().whenComplete(((messageId, throwable) -> {
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
        public SystemTopicClient<byte[]> getSystemTopicClient() {
            return systemTopicClient;
        }
    }

    public SystemTopicProducerStateClient(PulsarClient client, TopicName topicName) {
        super(client, topicName);
    }

    @Override
    protected CompletableFuture<Writer<byte[]>> newWriterAsyncInternal() {
        return client.newProducer()
                .topic(topicName.toString()).createAsync()
                .thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("New system topic writer for topic {}", topicName);
                    }
                    return CompletableFuture.completedFuture(new SystemTopicProducerStateWriter(this, producer));
                });
    }

    @Override
    protected CompletableFuture<Reader<byte[]>> newReaderAsyncInternal() {
        return client.newReader()
                .topic(topicName.toString())
                .readCompacted(true)
                .startMessageId(MessageId.earliest).createAsync()
                .thenCompose(reader -> {
                    if (log.isDebugEnabled()) {
                        log.debug("New system topic reader for topic {}", topicName);
                    }
                    return CompletableFuture.completedFuture(new SystemTopicProducerStateReader(this, reader));
                });
    }

}
