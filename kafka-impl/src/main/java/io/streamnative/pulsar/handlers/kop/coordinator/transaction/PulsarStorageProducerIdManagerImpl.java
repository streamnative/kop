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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;

/**
 * ProducerIdManager is the part of the transaction coordinator that provides ProducerIds in a unique way
 * such that the same producerId will not be assigned twice across multiple transaction coordinators.
 *
 * ProducerIds are managed via a Pulsar non-partitioned topic.
 */
@Slf4j
public class PulsarStorageProducerIdManagerImpl implements ProducerIdManager {

    private final AtomicLong nextId = new AtomicLong(0);
    private final String topic;
    private final PulsarClient pulsarClient;
    private CompletableFuture<Reader<byte[]>> reader;
    private CompletableFuture<Void> currentReadHandle;

    private synchronized CompletableFuture<Reader<byte[]>> ensureReaderHandle() {
        if (reader == null) {
            reader = pulsarClient.newReader()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .startMessageIdInclusive()
                    .readCompacted(true)
                    .createAsync();
        }
        return reader;
    }

    private CompletableFuture<Void> readNextMessageIfAvailable(Reader<byte[]> reader) {
        return reader
                .hasMessageAvailableAsync()
                .thenCompose(hasMessageAvailable -> {
                    if (hasMessageAvailable == null
                            || !hasMessageAvailable) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        CompletableFuture<Message<byte[]>> opMessage = reader.readNextAsync();
                        return opMessage.thenCompose(msg -> {
                            byte[] value = msg.getValue();
                            long newId = new BigInteger(value).longValue();
                            log.debug("{} Read {} from {}", this, newId, msg.getMessageId());
                            nextId.set(newId);
                            return readNextMessageIfAvailable(reader);
                        });
                    }
                });
    }

    synchronized CompletableFuture<Void> ensureLatestData(boolean beforeWrite) {
        if (currentReadHandle != null) {
            if (beforeWrite) {
                // we are inside a write loop, so
                // we must ensure that we start to read now
                // otherwise the write would use non up-to-date data
                // so let's finish the current loop
                log.debug("A read was already pending, starting a new one in order to ensure consistency");
                return currentReadHandle
                        .thenCompose(___ -> ensureLatestData(false));
            }
            // if there is an ongoing read operation then complete it
            return currentReadHandle;
        }
        // please note that the read operation is async,
        // and it is not execute inside this synchronized block
        CompletableFuture<Reader<byte[]>> readerHandle = ensureReaderHandle();
        final CompletableFuture<Void> newReadHandle =
                readerHandle.thenCompose(this::readNextMessageIfAvailable);
        currentReadHandle = newReadHandle;
        return newReadHandle.whenComplete((a, b) -> {
            endReadLoop(newReadHandle);
            if (b != null) {
                throw new CompletionException(b);
            }
        });
    }

    private synchronized void endReadLoop(CompletableFuture<?> handle) {
        if (handle == currentReadHandle) {
            currentReadHandle = null;
        }
    }

    @Override
    public synchronized CompletableFuture<Long> generateProducerId() {
        // we could get rid of the Exclusive Producer if we had Message.getIndex()
        // introduced in 2.9.0 https://github.com/apache/pulsar/pull/11553
        CompletableFuture<Producer<byte[]>> producerHandle = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(topic)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .blockIfQueueFull(true)
                .createAsync();
        return producerHandle.thenCompose(opProducer -> {
            // nobody can write now to the topic
            // wait for local cache to be up-to-date
            CompletableFuture<Long> dummy = ensureLatestData(true)
                    .thenCompose((___) -> {
                        final long result = nextId.get() + 1;
                        // write to Pulsar
                        byte[] serialized = BigInteger.valueOf(result).toByteArray();
                        CompletableFuture<Long>  res =  opProducer
                                .newMessage()
                                .key("") // always the same key, this way we can rely on compaction
                                .value(serialized)
                                .sendAsync()
                                .thenApply((msgId) -> {
                                    log.debug("{} written {} as {}", this, result, msgId);
                                    nextId.set(result);
                                    return result;
                                });
                        return res;
                    });
            // ensure that we release the exclusive producer in any case
            dummy.whenComplete((___, err) -> {
                opProducer.closeAsync();
            });
            return dummy;
        });
    }

    public PulsarStorageProducerIdManagerImpl(String topicName, PulsarClient pulsarClient) {
        this.topic = topicName;
        this.pulsarClient = pulsarClient;
    }

    @Override
    public CompletableFuture<Void> initialize() {
       return ensureLatestData(false);
    }

    @Override
    public void shutdown() {
    }

}
