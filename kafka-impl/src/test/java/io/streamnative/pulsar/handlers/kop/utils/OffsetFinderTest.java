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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.service.persistent.PersistentMessageExpiryMonitor;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.testng.annotations.Test;

/**
 * Test for OffsetFinder.
 */
public class OffsetFinderTest extends MockedBookKeeperTestCase {

    public static byte[] createMessageWrittenToLedger(String msg) throws Exception {
        final MessageMetadata messageMetadata = new MessageMetadata();
        messageMetadata.setPublishTime(System.currentTimeMillis());
        messageMetadata.setProducerName("createMessageWrittenToLedger");
        messageMetadata.setSequenceId(1);
        ByteBuf data = UnpooledByteBufAllocator.DEFAULT.heapBuffer().writeBytes(msg.getBytes());

        int msgMetadataSize = messageMetadata.getSerializedSize();
        int payloadSize = data.readableBytes();
        int totalSize = 4 + msgMetadataSize + payloadSize;

        ByteBuf headers = PulsarByteBufAllocator.DEFAULT.heapBuffer(totalSize, totalSize);
        headers.writeInt(msgMetadataSize);
        messageMetadata.writeTo(headers);
        ByteBuf headersAndPayload = ByteBufPair.coalesce(ByteBufPair.get(headers, data));
        byte[] byteMessage = headersAndPayload.nioBuffer().array();
        headersAndPayload.release();
        return byteMessage;
    }

    class Result {
        ManagedLedgerException exception = null;
        Position position = null;

        void reset() {
            this.exception = null;
            this.position = null;
        }
    }

    CompletableFuture<Void> findMessage(final Result result, final ManagedLedger c1, final long timestamp) {
        OffsetFinder messageFinder = new OffsetFinder((ManagedLedgerImpl) c1);

        final CompletableFuture<Void> future = new CompletableFuture<>();
        messageFinder.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                result.position = position;
                future.complete(null);
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception, Optional<Position> var2, Object ctx) {
                result.exception = exception;
                future.completeExceptionally(exception);
            }
        });
        return future;
    }

    @Test
    void testOffsetFinder() throws Exception {
        final String ledgerAndCursorName = "testOffsetFinder";
        int entriesPerLedger = 2;
        long beginTimestamp = System.currentTimeMillis();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        ledger.addEntry(createMessageWrittenToLedger("retained1"));
        // space apart message publish times
        Thread.sleep(100);
        ledger.addEntry(createMessageWrittenToLedger("retained2"));
        Thread.sleep(100);
        Position newPosition = ledger.addEntry(createMessageWrittenToLedger("retained3"));
        Thread.sleep(100);
        long timestamp = System.currentTimeMillis();
        Thread.sleep(10);

        ledger.addEntry(createMessageWrittenToLedger("afterresetposition"));

        Position lastPosition = ledger.addEntry(createMessageWrittenToLedger("not-read"));
        List<Entry> entries = c1.readEntries(3);
        c1.markDelete(entries.get(2).getPosition());
        c1.close();
        ledger.close();
        entries.forEach(e -> e.release());
        // give timed ledger trimming a chance to run
        Thread.sleep(1000);

        ledger = factory.open(ledgerAndCursorName, config);
        c1 = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);
        long endTimestamp = System.currentTimeMillis();

        Result result = new Result();

        CompletableFuture<Void> future = findMessage(result, ledger, timestamp);
        future.get();
        assertEquals(result.exception, null);
        assertTrue(result.position != null);
        assertEquals(result.position, newPosition);

        result.reset();
        future = findMessage(result, ledger, beginTimestamp);
        future.get();
        assertEquals(result.exception, null);
        assertEquals(result.position, c1.getFirstPosition());

        result.reset();
        future = findMessage(result, ledger, endTimestamp);
        future.get();
        assertEquals(result.exception, null);
        assertNotEquals(result.position, null);
        assertEquals(result.position, lastPosition);

        OffsetFinder messageFinder = new OffsetFinder((ManagedLedgerImpl) ledger);
        final AtomicBoolean ex = new AtomicBoolean(false);
        messageFinder.findEntryFailed(
            new ManagedLedgerException("failed"),
            Optional.empty(),
            new AsyncCallbacks.FindEntryCallback() {
                @Override
                public void findEntryComplete(Position position, Object ctx) {
                }

                @Override
                public void findEntryFailed(ManagedLedgerException exception, Optional<Position> pos, Object ctx) {
                    ex.set(true);
                }
            });
        assertTrue(ex.get());

        PersistentTopic persistentTopic = mock(PersistentTopic.class);
        when(persistentTopic.getName()).thenReturn("topicname");
        PersistentMessageExpiryMonitor monitor =
            new PersistentMessageExpiryMonitor(persistentTopic, c1.getName(), c1, null);
        monitor.findEntryFailed(new ManagedLedgerException
            .ConcurrentFindCursorPositionException("failed"), Optional.empty(), null);
        Field field = monitor.getClass().getDeclaredField("expirationCheckInProgress");
        field.setAccessible(true);
        assertEquals(0, field.get(monitor));

        result.reset();
        c1.close();
        ledger.close();
        factory.shutdown();
    }
}
