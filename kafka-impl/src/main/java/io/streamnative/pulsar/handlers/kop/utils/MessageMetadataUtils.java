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
import io.streamnative.pulsar.handlers.kop.exceptions.KoPMessageMetadataNotFoundException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
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

    public static long getPublishTime(final ByteBuf byteBuf) {
        final int readerIndex = byteBuf.readerIndex();
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

    private static void checkNullEntry(Entry entry) throws KoPMessageMetadataNotFoundException {
        if (entry == null) {
            throw new KoPMessageMetadataNotFoundException("Unexpected null entry");
        }
        if (entry.getDataBuffer() == null) {
            throw new KoPMessageMetadataNotFoundException("Unexpected null buffer in entry " + entry.getPosition());
        }
    }

    public static long peekOffsetFromEntry(Entry entry) throws KoPMessageMetadataNotFoundException {
        checkNullEntry(entry);
        return unsafePeekOffsetFromEntry(entry);
    }

    private static long unsafePeekOffsetFromEntry(Entry entry) throws KoPMessageMetadataNotFoundException {
        return unsafePeekOffset(entry.getDataBuffer(), entry.getPosition());
    }

    private static long unsafePeekOffset(ByteBuf buf, @Nullable Position position)
            throws KoPMessageMetadataNotFoundException {
        try {
            final BrokerEntryMetadata brokerEntryMetadata =
                    Commands.peekBrokerEntryMetadataIfExist(buf); // might throw IllegalArgumentException
            if (brokerEntryMetadata == null) {
                throw new IllegalArgumentException("No BrokerEntryMetadata found");
            }
            return brokerEntryMetadata.getIndex(); // might throw IllegalStateException
        } catch (IllegalArgumentException | IllegalStateException e) {
            // This exception could be thrown by both peekBrokerEntryMetadataIfExist or null check
            throw new KoPMessageMetadataNotFoundException(
                    "Failed to peekOffsetFromEntry for " + position + ": " + e.getMessage());
        }
    }

    public static long peekBaseOffsetFromEntry(Entry entry) throws KoPMessageMetadataNotFoundException {
        checkNullEntry(entry);
        return peekBaseOffset(entry.getDataBuffer(), entry.getPosition());
    }

    private static long peekBaseOffset(ByteBuf buf, @Nullable Position position)
            throws KoPMessageMetadataNotFoundException {
        MessageMetadata metadata = Commands.peekMessageMetadata(buf, null, 0);

        if (metadata == null) {
            throw new KoPMessageMetadataNotFoundException("Failed to peekMessageMetadata for " + position);
        }

        return peekBaseOffset(buf, position, metadata.getNumMessagesInBatch());
    }

    private static long peekBaseOffset(ByteBuf buf, @Nullable Position position, int numMessages)
            throws KoPMessageMetadataNotFoundException {
        return unsafePeekOffset(buf, position) - (numMessages - 1);
    }

    public static long peekBaseOffset(ByteBuf buf, int numMessages) throws KoPMessageMetadataNotFoundException {
        return peekBaseOffset(buf, null, numMessages);
    }

    public static MessageMetadata parseMessageMetadata(ByteBuf buf) throws KoPMessageMetadataNotFoundException {
        try {
            return Commands.parseMessageMetadata(buf);
        } catch (IllegalArgumentException e) {
            throw new KoPMessageMetadataNotFoundException(e.getMessage());
        }
    }

    public static long getMockOffset(long ledgerId, long entryId) {
        return ledgerId + entryId;
    }
}
