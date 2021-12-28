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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;


/**
 * ProducerStateLogRecovery is used to recover producer state from logs.
 */
@Slf4j
public class ProducerStateLogRecovery {

    private final PersistentTopic topic;
    private final Position startReadCursorPosition = PositionImpl.earliest;
    public static final String SUBSCRIPTION_NAME = "producer-state-log-recovery-sub";
    private final ProducerStateRecoverCallBack callBack;
    private final SpscArrayQueue<Entry> entryQueue;
    private final AtomicInteger exceptionNumber = new AtomicInteger();

    public ProducerStateLogRecovery(PersistentTopic topic,
                                     ProducerStateRecoverCallBack callBack,
                                     int cacheQueueSize) {
        this.topic = topic;
        this.callBack = callBack;
        this.entryQueue = new SpscArrayQueue<>(cacheQueueSize);
    }

    protected void recover() {
        ManagedCursor managedCursor;
        try {
            managedCursor = topic.getManagedLedger()
                    .newNonDurableCursor(this.startReadCursorPosition, SUBSCRIPTION_NAME);
        } catch (ManagedLedgerException e) {
            callBack.recoverExceptionally(e);
            log.error("[{}] Producer state recover fail when open cursor!", topic.getName(), e);
            return;
        }
        PositionImpl lastConfirmedEntry = (PositionImpl) topic.getManagedLedger().getLastConfirmedEntry();
        PositionImpl currentLoadPosition = (PositionImpl) this.startReadCursorPosition;
        FillEntryQueueCallback fillEntryQueueCallback = new FillEntryQueueCallback(entryQueue, managedCursor,
                this);
        if (lastConfirmedEntry.getEntryId() != -1) {
            while (lastConfirmedEntry.compareTo(currentLoadPosition) > 0
                    && fillEntryQueueCallback.fillQueue()) {
                Entry entry = entryQueue.poll();
                if (entry != null) {
                    try {
                        currentLoadPosition = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
                        callBack.handleTxnEntry(entry);
                    } finally {
                        entry.release();
                    }
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        //no-op
                    }
                }
            }
        }
        closeCursor(managedCursor);
        callBack.recoverComplete();
        log.info("Finish to recover from logs.");
    }

    private void callBackException(ManagedLedgerException e) {
        log.error("Transaction buffer recover fail when recover transaction entry!", e);
        this.exceptionNumber.getAndIncrement();
    }

    private void closeCursor(ManagedCursor cursor) {
        cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                log.info("[{}] Producer state log recover cursor close complete.", topic);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}] Producer state log recover cursor close fail.", topic);
            }
        }, null);
    }

    static class FillEntryQueueCallback implements AsyncCallbacks.ReadEntriesCallback {

        private final AtomicLong outstandingReadsRequests = new AtomicLong(0);

        private final SpscArrayQueue<Entry> entryQueue;

        private final ManagedCursor cursor;

        private final ProducerStateLogRecovery recover;

        private volatile boolean isReadable = true;

        private FillEntryQueueCallback(SpscArrayQueue<Entry> entryQueue, ManagedCursor cursor,
                                       ProducerStateLogRecovery recover) {
            this.entryQueue = entryQueue;
            this.cursor = cursor;
            this.recover = recover;
        }
        boolean fillQueue() {
            if (entryQueue.size() < entryQueue.capacity()
                    && outstandingReadsRequests.get() == 0
                    && cursor.hasMoreEntries()) {
                outstandingReadsRequests.incrementAndGet();
                cursor.asyncReadEntries(100, this, System.nanoTime(), PositionImpl.latest);
            }
            return isReadable;
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            entryQueue.fill(new MessagePassingQueue.Supplier<Entry>() {
                private int i = 0;
                @Override
                public Entry get() {
                    Entry entry = entries.get(i);
                    i++;
                    return entry;
                }
            }, entries.size());

            outstandingReadsRequests.decrementAndGet();
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            if (recover.topic.getManagedLedger().getConfig().isAutoSkipNonRecoverableData()
                    && exception instanceof ManagedLedgerException.NonRecoverableLedgerException
                    || exception instanceof ManagedLedgerException.ManagedLedgerFencedException) {
                isReadable = false;
            }
            recover.callBackException(exception);
            outstandingReadsRequests.decrementAndGet();
        }
    }

}
