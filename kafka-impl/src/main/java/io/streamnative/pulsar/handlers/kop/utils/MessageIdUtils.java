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

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.intercept.ManagedLedgerInterceptorImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils for Pulsar MessageId.
 */
public class MessageIdUtils {
    private static final Logger log = LoggerFactory.getLogger(MessageIdUtils.class);

    public static long getCurrentOffset(ManagedLedger managedLedger) {
        return ((ManagedLedgerInterceptorImpl) managedLedger.getManagedLedgerInterceptor()).getIndex();
    }

    public static long getHighWatermark(ManagedLedger managedLedger) {
        return getCurrentOffset(managedLedger) + 1;
    }

    public static long getLogEndOffset(ManagedLedger managedLedger) {
        return getCurrentOffset(managedLedger) + 1;
    }

    public static long getPublishTime(final ByteBuf byteBuf) {
        final int readerIndex = byteBuf.readerIndex();
        Commands.skipBrokerEntryMetadataIfExist(byteBuf);
        final MessageMetadata metadata = Commands.parseMessageMetadata(byteBuf);
        byteBuf.readerIndex(readerIndex);
        return metadata.getPublishTime();
    }

    public static CompletableFuture<Long> getOffsetOfPosition(ManagedLedgerImpl managedLedger,
                                                              PositionImpl position,
                                                              boolean needCheckMore,
                                                              long timestamp) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        managedLedger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
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

                } catch (Exception e) {
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

    public static PositionImpl getPositionForOffset(ManagedLedger managedLedger, Long offset) {
        try {
            return (PositionImpl) managedLedger.asyncFindPosition(new OffsetSearchPredicate(offset)).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("[{}] Failed to find position for offset {}", managedLedger.getName(), offset);
            throw new RuntimeException(managedLedger.getName() + " failed to find position for offset " + offset);
        }
    }

    public static long peekOffsetFromEntry(Entry entry) {
        return Commands.peekBrokerEntryMetadataIfExist(entry.getDataBuffer()).getIndex();
    }

    public static long peekBaseOffsetFromEntry(Entry entry) {

        return peekOffsetFromEntry(entry)
                - Commands.peekMessageMetadata(entry.getDataBuffer(), null, 0)
                    .getNumMessagesInBatch() + 1;
    }

    public static long getMockOffset(long ledgerId, long entryId) {
        return ledgerId + entryId;
    }
}
