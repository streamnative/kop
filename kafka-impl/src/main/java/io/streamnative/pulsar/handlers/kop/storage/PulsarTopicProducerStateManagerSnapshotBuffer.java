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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.util.FutureUtil;

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
        if (serialized == null) {
            // cannot serialise, skip
            return CompletableFuture.completedFuture(null);
        }
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
                        ProducerStateManagerSnapshot latest = latestSnapshots.get(snapshot.getTopicPartition());
                        if (latest != null && latest.getOffset() > snapshot.getOffset()) {
                            log.error("Topic ownership changed for {}. Found a snapshot at {} "
                                    + "while trying to write the snapshot at {}", snapshot.getTopicPartition(),
                                    latest.getOffset(), snapshot.getOffset());
                            return FutureUtil.failedFuture(new NotLeaderOrFollowerException("No more owner of "
                                    + "ProducerState for topic " + topic));
                        }
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

        ByteBuf byteBuf = Unpooled.buffer();
        try (DataOutputStream dataOutputStream =
                     new DataOutputStream(new ByteBufOutputStream(byteBuf));) {

            dataOutputStream.writeUTF(snapshot.getTopicPartition());
            dataOutputStream.writeLong(snapshot.getOffset());

            dataOutputStream.writeInt(snapshot.getProducers().size());
            for (Map.Entry<Long, ProducerStateEntry> entry : snapshot.getProducers().entrySet()) {
                ProducerStateEntry producer = entry.getValue();
                dataOutputStream.writeLong(producer.producerId());
                if (producer.producerEpoch() != null) {
                    dataOutputStream.writeInt(producer.producerEpoch());
                } else {
                    dataOutputStream.writeInt(-1);
                }
                if (producer.coordinatorEpoch() != null) {
                    dataOutputStream.writeInt(producer.coordinatorEpoch());
                } else {
                    dataOutputStream.writeInt(-1);
                }
                if (producer.lastTimestamp() != null) {
                    dataOutputStream.writeLong(producer.lastTimestamp());
                } else {
                    dataOutputStream.writeLong(-1L);
                }
                if (producer.currentTxnFirstOffset().isPresent()) {
                    dataOutputStream.writeLong(producer.currentTxnFirstOffset().get());
                } else {
                    dataOutputStream.writeLong(-1);
                }
            }

            dataOutputStream.writeInt(snapshot.getOngoingTxns().size());
            for (Map.Entry<Long, TxnMetadata> entry : snapshot.getOngoingTxns().entrySet()) {
                TxnMetadata tx = entry.getValue();
                dataOutputStream.writeLong(tx.producerId());
                dataOutputStream.writeLong(tx.firstOffset());
                dataOutputStream.writeLong(tx.lastOffset());
            }

            dataOutputStream.writeInt(snapshot.getAbortedIndexList().size());
            for (AbortedTxn tx : snapshot.getAbortedIndexList()) {
                dataOutputStream.writeLong(tx.producerId());
                dataOutputStream.writeLong(tx.firstOffset());
                dataOutputStream.writeLong(tx.lastOffset());
                dataOutputStream.writeLong(tx.lastStableOffset());
            }

            dataOutputStream.flush();

            return byteBuf.nioBuffer();

        } catch (IOException err) {
            log.error("Cannot serialise snapshot {}", snapshot, err);
            return null;
        }
    }

    private static ProducerStateManagerSnapshot deserialize(ByteBuffer buffer) {
        try (DataInputStream dataInputStream =
                     new DataInputStream(new ByteBufInputStream(Unpooled.wrappedBuffer(buffer)));) {
            String topicPartition = dataInputStream.readUTF();
            long offset = dataInputStream.readLong();

            int numProducers = dataInputStream.readInt();
            Map<Long, ProducerStateEntry> producers = new HashMap<>();
            for (int i = 0; i < numProducers; i++) {
                long producerId = dataInputStream.readLong();
                Integer producerEpoch = dataInputStream.readInt();
                if (producerEpoch == -1) {
                    producerEpoch = null;
                }
                Integer coordinatorEpoch = dataInputStream.readInt();
                if (coordinatorEpoch == -1) {
                    coordinatorEpoch = null;
                }
                Long lastTimestamp = dataInputStream.readLong();
                if (lastTimestamp == -1) {
                    lastTimestamp = null;
                }
                Long currentTxFirstOffset = dataInputStream.readLong();
                if (currentTxFirstOffset == -1) {
                    currentTxFirstOffset = null;
                }
                ProducerStateEntry entry = ProducerStateEntry.empty(producerId)
                        .producerEpoch(producerEpoch != null ? producerEpoch.shortValue() : null)
                        .coordinatorEpoch(coordinatorEpoch)
                        .lastTimestamp(lastTimestamp)
                        .currentTxnFirstOffset(Optional.ofNullable(currentTxFirstOffset));
                producers.put(producerId, entry);
            }

            int numOngoingTxns = dataInputStream.readInt();
            TreeMap<Long, TxnMetadata> ongoingTxns = new TreeMap<>();
            for (int i = 0; i < numOngoingTxns; i++) {
                long producerId = dataInputStream.readLong();
                long firstOffset = dataInputStream.readLong();
                long lastOffset = dataInputStream.readLong();
                ongoingTxns.put(firstOffset, new TxnMetadata(producerId, firstOffset)
                        .lastOffset(lastOffset));
            }

            int numAbortedIndexList = dataInputStream.readInt();
            List<AbortedTxn> abortedTxnList = new ArrayList<>();
            for (int i = 0; i < numAbortedIndexList; i++) {
                long producerId = dataInputStream.readLong();
                long firstOffset = dataInputStream.readLong();
                long lastOffset = dataInputStream.readLong();
                long lastStableOffset = dataInputStream.readLong();
                abortedTxnList.add(new AbortedTxn(producerId, firstOffset, lastOffset, lastStableOffset));
            }

            return new ProducerStateManagerSnapshot(topicPartition, offset,
                    producers, ongoingTxns, abortedTxnList);

        } catch (Throwable err) {
            log.error("Cannot deserialize snapshot", err);
            return null;
        }
    }

    private void processMessage(Message<ByteBuffer> msg) {
        ProducerStateManagerSnapshot deserialize = deserialize(msg.getValue());
        if (deserialize != null) {
            String key = msg.hasKey() ? msg.getKey() : null;
            if (Objects.equals(key, deserialize.getTopicPartition())) {
                if (log.isDebugEnabled()) {
                    log.info("found snapshot for {} : {}", deserialize.getTopicPartition(), deserialize);
                }
                latestSnapshots.put(deserialize.getTopicPartition(), deserialize);
            }
        }
    }

    @Override
    public CompletableFuture<ProducerStateManagerSnapshot> readLatestSnapshot(String topicPartition) {
        log.info("Reading latest snapshot for {}", topicPartition);
        return ensureLatestData(false).thenApply(__ -> {
            ProducerStateManagerSnapshot result =  latestSnapshots.get(topicPartition);
            log.info("Latest snapshot for {} is {}", topicPartition, result);
            return result;
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
