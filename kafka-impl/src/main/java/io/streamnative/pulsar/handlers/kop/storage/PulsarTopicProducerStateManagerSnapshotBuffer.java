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
package io.streamnative.pulsar.handlers.kop.storage;

import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;

@Slf4j
public class PulsarTopicProducerStateManagerSnapshotBuffer implements ProducerStateManagerSnapshotBuffer {

    private Map<String, ProducerStateManagerSnapshot> latestSnapshots = new ConcurrentHashMap<>();
    private final String topic;
    private final SystemTopicClient pulsarClient;
    private CompletableFuture<Reader<ByteBuffer>> reader;
    private CompletableFuture<Void> currentReadHandle;

    private synchronized CompletableFuture<Reader<ByteBuffer>> ensureReaderHandle() {
        if (reader == null) {
            reader = pulsarClient.newReaderBuilder()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .readCompacted(true)
                    .createAsync();
        }
        return reader;
    }

    private CompletableFuture<Void> readNextMessageIfAvailable(Reader<ByteBuffer> reader) {
        return reader
                .hasMessageAvailableAsync()
                .thenCompose(hasMessageAvailable -> {
                    if (hasMessageAvailable == null
                            || !hasMessageAvailable) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        CompletableFuture<Message<ByteBuffer>> opMessage = reader.readNextAsync();
                        return opMessage.thenCompose(msg -> {
                            processMessage(msg);
                            return readNextMessageIfAvailable(reader);
                        });
                    }
                });
    }


    private synchronized CompletableFuture<Void> ensureLatestData(boolean beforeWrite) {
        if (currentReadHandle != null) {
            if (beforeWrite) {
                // we are inside a write loop, so
                // we must ensure that we start to read now
                // otherwise the write would use non up-to-date data
                // so let's finish the current loop
                if (log.isDebugEnabled()) {
                    log.debug("A read was already pending, starting a new one in order to ensure consistency");
                }
                return currentReadHandle
                        .thenCompose(___ -> ensureLatestData(false));
            }
            // if there is an ongoing read operation then complete it
            return currentReadHandle;
        }
        // please note that the read operation is async,
        // and it is not execute inside this synchronized block
        CompletableFuture<Reader<ByteBuffer>> readerHandle = ensureReaderHandle();
        final CompletableFuture<Void> newReadHandle =
                readerHandle.thenCompose(this::readNextMessageIfAvailable);
        currentReadHandle = newReadHandle;
        return newReadHandle.thenApply((__) -> {
            endReadLoop(newReadHandle);
            return null;
        });
    }

    private synchronized void endReadLoop(CompletableFuture<?> handle) {
        if (handle == currentReadHandle) {
            currentReadHandle = null;
        }
    }

    @Override
    public CompletableFuture<Void> write(ProducerStateManagerSnapshot snapshot) {
        ByteBuffer serialized = serialize(snapshot);
        CompletableFuture<Producer<ByteBuffer>> producerHandle = pulsarClient.newProducerBuilder()
                .enableBatching(false)
                .topic(topic)
                .blockIfQueueFull(true)
                .createAsync();
        return producerHandle.thenCompose(opProducer -> {
            // nobody can write now to the topic
            // wait for local cache to be up-to-date
            CompletableFuture<Void> dummy = ensureLatestData(true)
                    .thenCompose((___) -> {
                        return opProducer
                                .newMessage()
                                .key(snapshot.getTopicPartition()) // leverage compaction
                                .value(serialized)
                                .sendAsync()
                                .thenApply((msgId) -> {
                                    if (log.isDebugEnabled()) {
                                        log.debug("{} written {} as {}", this, snapshot, msgId);
                                    }
                                    latestSnapshots.put(snapshot.getTopicPartition(), snapshot);
                                    return null;
                                });
                    });
            // ensure that we release the exclusive producer in any case
            return dummy.whenComplete((___, err) -> {
                opProducer.closeAsync().whenComplete((____, errorClose) -> {
                    if (errorClose != null) {
                        log.error("Error closing producer for {}", topic, errorClose);
                    }
                });
            });
        });
    }

    private static ByteBuffer serialize(ProducerStateManagerSnapshot snapshot) {
        return ByteBuffer.allocate(0);
    }

    private static ProducerStateManagerSnapshot deserialize(ByteBuffer buffer) {
        return null;
    }

    private void processMessage(Message<ByteBuffer> msg) {
        ProducerStateManagerSnapshot deserialize = deserialize(msg.getValue());
        if (deserialize != null) {
            latestSnapshots.put(deserialize.getTopicPartition(), deserialize);
        }
    }

    @Override
    public CompletableFuture<ProducerStateManagerSnapshot> readLatestSnapshot(String topicPartition) {
        return ensureLatestData(false).thenApply(__ -> {
            return latestSnapshots.get(topicPartition);
        });
    }

    public PulsarTopicProducerStateManagerSnapshotBuffer(String topicName, SystemTopicClient pulsarClient) {
        this.topic = topicName;
        this.pulsarClient = pulsarClient;
    }


    @Override
    public synchronized void shutdown() {
        if (reader != null) {
            reader.whenComplete((r, e) -> {
                if (r != null) {
                    r.closeAsync().whenComplete((___, err) -> {
                        if (err != null) {
                            log.error("Error closing reader for {}", topic, err);
                        }
                    });
                }
            });
        }
    }
}
