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

import com.google.common.base.Predicate;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.exceptions.MetadataCorruptedException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.intercept.ManagedLedgerInterceptorImpl;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Utils for Pulsar MessageId.
 */
@Slf4j
public class MessageMetadataUtils {

    public static long getCurrentOffset(ManagedLedger managedLedger) {
        return ((ManagedLedgerInterceptorImpl) managedLedger.getManagedLedgerInterceptor()).getIndex();
    }

    public static long getHighWatermark(ManagedLedger managedLedger) {
        return getCurrentOffset(managedLedger) + 1;
    }

    public static long getLogEndOffset(ManagedLedger managedLedger) {
        return getCurrentOffset(managedLedger) + 1;
    }

    public static long getPublishTime(final ByteBuf byteBuf) throws MetadataCorruptedException {
        final int readerIndex = byteBuf.readerIndex();
        final MessageMetadata metadata = parseMessageMetadata(byteBuf);
        byteBuf.readerIndex(readerIndex);
        if (metadata.hasPublishTime()) {
            return metadata.getPublishTime();
        } else {
            throw new MetadataCorruptedException("Field 'publish_time' is not set");
        }
    }

    public static CompletableFuture<Long> getOffsetOfPosition(ManagedLedgerImpl managedLedger,
                                                              PositionImpl position,
                                                              boolean needCheckMore,
                                                              long timestamp,
                                                              boolean skipMessagesWithoutIndex) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        managedLedger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                if (exception instanceof ManagedLedgerException.NonRecoverableLedgerException) {
                    // The position doesn't exist, it usually happens when the rollover of managed ledger leads to
                    // the deletion of all expired ledgers. In this case, there's only one empty ledger in the managed
                    // ledger. So here we complete it with the latest offset.
                    future.complete(getLogEndOffset(managedLedger));
                } else {
                    future.completeExceptionally(exception);
                }
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    if (needCheckMore) {
                        long offset = peekOffsetFromEntry(entry);
                        final long publishTime = getPublishTime(entry.getDataBuffer());
                        if (publishTime >= timestamp) {
                            future.complete(offset);
                        } else {
                            future.complete(offset + 1);
                        }
                    } else {
                        future.complete(peekBaseOffsetFromEntry(entry));
                    }
                } catch (MetadataCorruptedException.NoBrokerEntryMetadata e) {
                    if (skipMessagesWithoutIndex) {
                        log.warn("The entry {} doesn't have BrokerEntryMetadata, return 0 as the offset", position);
                        future.complete(0L);
                    } else {
                        future.completeExceptionally(e);
                    }
                } catch (MetadataCorruptedException e) {
                    future.completeExceptionally(e);
                } finally {
                    if (entry != null) {
                        entry.release();
                    }
                }

            }
        }, null);
        return future;
    }

    public static long peekOffsetFromEntry(Entry entry) throws MetadataCorruptedException {
        return peekOffset(entry.getDataBuffer(), entry.getPosition());
    }

    private static long peekOffset(ByteBuf buf, @Nullable Position position)
            throws MetadataCorruptedException {
        try {
            final BrokerEntryMetadata brokerEntryMetadata =
                    Commands.peekBrokerEntryMetadataIfExist(buf); // might throw IllegalArgumentException
            if (brokerEntryMetadata == null) {
                throw new MetadataCorruptedException.NoBrokerEntryMetadata();
            }
            return brokerEntryMetadata.getIndex(); // might throw IllegalStateException
        } catch (IllegalArgumentException | IllegalStateException e) {
            // This exception could be thrown by both peekBrokerEntryMetadataIfExist or null check
            throw new MetadataCorruptedException(
                    "Failed to peekOffsetFromEntry for " + position + ": " + e.getMessage());
        }
    }

    public static long peekBaseOffsetFromEntry(Entry entry) throws MetadataCorruptedException {
        return peekBaseOffset(entry.getDataBuffer(), entry.getPosition());
    }

    private static long peekBaseOffset(ByteBuf buf, @Nullable Position position)
            throws MetadataCorruptedException {
        MessageMetadata metadata = Commands.peekMessageMetadata(buf, null, 0);

        if (metadata == null) {
            throw new MetadataCorruptedException("Failed to peekMessageMetadata for " + position);
        }

        return peekBaseOffset(buf, position, metadata.getNumMessagesInBatch());
    }

    private static long peekBaseOffset(ByteBuf buf, @Nullable Position position, int numMessages)
            throws MetadataCorruptedException {
        return peekOffset(buf, position) - (numMessages - 1);
    }

    public static long peekBaseOffset(ByteBuf buf, int numMessages) throws MetadataCorruptedException {
        return peekBaseOffset(buf, null, numMessages);
    }

    public static MessageMetadata parseMessageMetadata(ByteBuf buf) throws MetadataCorruptedException {
        try {
            return Commands.parseMessageMetadata(buf);
        } catch (IllegalArgumentException e) {
            throw new MetadataCorruptedException(e.getMessage());
        }
    }

    public static long getMockOffset(long ledgerId, long entryId) {
        return ledgerId + entryId;
    }

    public static CompletableFuture<Position> asyncFindPosition(final ManagedLedger managedLedger,
                                                                final long offset,
                                                                final boolean skipMessagesWithoutIndex) {
        return managedLedger.asyncFindPosition(new FindEntryByOffset(managedLedger,
                offset, skipMessagesWithoutIndex));
    }

    @AllArgsConstructor
    private static class FindEntryByOffset implements Predicate<Entry> {
        private final ManagedLedger managedLedger;
        private final long offset;
        private final boolean skipMessagesWithoutIndex;

        @Override
        public boolean apply(Entry entry) {
            if (entry == null) {
                // `entry` should not be null, add the null check here to fix the spotbugs check
                return false;
            }
            try {
                return peekOffsetFromEntry(entry) < offset;
            } catch (MetadataCorruptedException.NoBrokerEntryMetadata ignored) {
                // When skipMessagesWithoutIndex is false, just return false to stop finding the position. Otherwise,
                // we assume the messages without BrokerEntryMetadata are produced by KoP < 2.8.0 that doesn't
                // support BrokerEntryMetadata. In this case, these messages should be older than any message produced
                // by KoP with BrokerEntryMetadata enabled.
                return skipMessagesWithoutIndex;
            } catch (MetadataCorruptedException e) {
                log.error("[{}] Entry {} is corrupted: {}",
                        managedLedger.getName(), entry.getPosition(), e.getMessage());
                return false;
            } finally {
                entry.release();
            }
        }

        @Override
        public String toString() {
            return "FindEntryByOffset{ " + offset + "}";
        }
    }

}
