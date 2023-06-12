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
package io.streamnative.pulsar.handlers.kop.coordinator;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;

/**
 * The abstraction to read or write a compacted partitioned topic.
 */
@Slf4j
public class CompactedPartitionedTopic<T> implements Closeable {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<Integer, Future<Producer<T>>> producers = new ConcurrentHashMap<>();
    private final Map<Integer, Future<Reader<T>>> readers = new ConcurrentHashMap<>();
    private final ProducerBuilder<T> producerBuilder;
    private final ReaderBuilder<T> readerBuilder;
    private final String topic;
    private final Executor executor;
    // Before this class is introduced, KoP writes an "empty" value to indicate it's the end of the topic, then read
    // until the message's position. The extra write is unnecessary. To be compatible with the older versions of KoP,
    // we need to recognize if the message is "empty" and then skip it.
    private final Function<T, Boolean> valueIsEmpty;

    // Use a separated executor for the creation of producers and readers to avoid deadlock
    private final ExecutorService createAsyncExecutor;

    public CompactedPartitionedTopic(final PulsarClient client,
                                     final Schema<T> schema,
                                     final int maxPendingMessages,
                                     final String topic,
                                     final Executor executor,
                                     final Function<T, Boolean> valueIsEmpty) {
        this.producerBuilder = client.newProducer(schema)
                .maxPendingMessages(maxPendingMessages)
                .blockIfQueueFull(true);
        this.readerBuilder = client.newReader(schema)
                .startMessageId(MessageId.earliest)
                .readCompacted(true);
        this.topic = topic;
        this.executor = executor;
        this.valueIsEmpty = valueIsEmpty;
        this.createAsyncExecutor = Executors.newSingleThreadExecutor();
    }

    /**
     * Send the message asynchronously.
     */
    public CompletableFuture<MessageId> sendAsync(int partition, byte[] key, T value, long timestamp) {
        final Producer<T> producer;
        try {
            producer = getProducer(partition);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e.getCause());
        }

        final var future = new CompletableFuture<MessageId>();
        producer.newMessage().keyBytes(key).value(value).eventTime(timestamp).sendAsync().whenComplete((msgId, e) -> {
            if (e == null) {
                future.complete(msgId);
            } else {
                if (e instanceof PulsarClientException.AlreadyClosedException) {
                    // The producer is already closed, we don't need to close it again.
                    producers.remove(partition);
                }
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    /**
     * Read to the latest message of the partition.
     *
     * @param partition the partition of `topic` to read
     * @param messageConsumer the message callback that is guaranteed to be called in the same thread
     * @return the future of the read result
     */
    public CompletableFuture<ReadResult> readToLatest(int partition, Consumer<Message<T>> messageConsumer) {
        final CompletableFuture<ReadResult> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                final Reader<T> reader = getReader(partition);
                readToLatest(reader, partition, messageConsumer, future, System.currentTimeMillis(), 0);
            } catch (IOException e) {
                future.completeExceptionally(e.getCause());
            }
        });
        return future;
    }

    private void readToLatest(Reader<T> reader, int partition, Consumer<Message<T>> messageConsumer,
                              CompletableFuture<ReadResult> future, long startTimeMs, long numMessages) {
        if (closed.get()) {
            future.complete(new ReadResult(System.currentTimeMillis() - startTimeMs, numMessages));
            return;
        }
        reader.hasMessageAvailableAsync().thenComposeAsync(available -> {
            if (available && !closed.get()) {
                return reader.readNextAsync();
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }, executor).thenAcceptAsync(msg -> {
            if (msg == null) {
                future.complete(new ReadResult(System.currentTimeMillis() - startTimeMs, numMessages));
                return;
            }
            long numMessagesProcessed = numMessages;
            if (!valueIsEmpty.apply(msg.getValue())) {
                numMessagesProcessed++;
                messageConsumer.accept(msg);
            }
            readToLatest(reader, partition, messageConsumer, future, startTimeMs, numMessagesProcessed);
        }, executor).exceptionallyAsync(e -> {
            while (e.getCause() != null) {
                e = e.getCause();
            }
            if (e instanceof PulsarClientException.AlreadyClosedException) {
                // The producer is already closed, we don't need to close it again.
                removeAndClose("reader", readers, partition, producer -> CompletableFuture.completedFuture(null));
                log.warn("Failed to read {}-{} to latest since the reader is closed", topic, partition);
                future.complete(new ReadResult(System.currentTimeMillis() - startTimeMs, numMessages));
            } else {
                removeAndClose("reader", readers, partition, Reader::closeAsync);
                log.error("Failed to read {}-{} to latest", topic, partition, e);
                future.completeExceptionally(e);
            }
            return null;
        }, executor);
    }

    /**
     * Remove the cached producer and reader of the target partition.
     */
    public CompletableFuture<Void> remove(int partition) {
        return CompletableFuture.allOf(
                removeAndClose("producer", producers, partition, Producer::closeAsync),
                removeAndClose("readers", readers, partition, Reader::closeAsync)
        );
    }

    @Override
    public void close() {
        try {
            CoreUtils.waitForAll(
                    Sets.union(producers.keySet(), readers.keySet()).stream().map(this::remove).toList()
            ).get(3, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("Failed to close CompactedPartitionedTopic ({}) in 3 seconds", topic);
        }
        createAsyncExecutor.shutdown();
    }

    private static <T> CompletableFuture<Void> removeAndClose(String name, Map<Integer, Future<T>> cache, int index,
                                                              Function<T, CompletableFuture<Void>> closeFunc) {
        final var elem = cache.remove(index);
        if (elem == null) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            return closeFunc.apply(elem.get(1, TimeUnit.SECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("Failed to create {}: {}", name, e.getCause());
            return CompletableFuture.completedFuture(null);
        }
    }

    private Producer<T> getProducer(int partition) throws IOException {
        try {
            return producers.computeIfAbsent(partition, __ ->
                createAsyncExecutor.submit(() -> producerBuilder.clone().topic(getPartition(partition)).create())
            ).get();
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    private Reader<T> getReader(int partition) throws IOException {
        try {
            return readers.computeIfAbsent(partition, __ ->
                    createAsyncExecutor.submit(() -> readerBuilder.clone().topic(getPartition(partition)).create())
            ).get();
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    private String getPartition(int partition) {
        return topic + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
    }

    public record ReadResult(long timeMs, long numMessages) {
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ReadResult that)) {
                return false;
            }
            return this.timeMs == that.timeMs && this.numMessages == that.numMessages;
        }
    }
}
