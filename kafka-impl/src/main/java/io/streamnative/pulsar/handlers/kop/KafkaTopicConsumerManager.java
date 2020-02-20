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
package io.streamnative.pulsar.handlers.kop;

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils.offsetAfterBatchIndex;

import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;

/**
 * KafkaTopicConsumerManager manages a topic and its related offset cursor.
 * Each cursor is trying to track the read from a consumer client.
 */
@Slf4j
public class KafkaTopicConsumerManager implements Closeable {
    private final PersistentTopic topic;
    private final KafkaRequestHandler requestHandler;

    // the lock for closed status change.
    private final ReentrantReadWriteLock rwLock;
    private boolean closed;

    // keep fetch offset and related cursor. keep cursor and its last offset in Pair. <offset, pair>
    @Getter
    private final ConcurrentLongHashMap<Pair<ManagedCursor, Long>> consumers;

    // track last access time(millis) for offsets <offset, time>
    @Getter
    private final ConcurrentLongHashMap<Long> lastAccessTimes;

    KafkaTopicConsumerManager(KafkaRequestHandler requestHandler, PersistentTopic topic) {
        this.topic = topic;
        this.consumers = new ConcurrentLongHashMap<>();
        this.lastAccessTimes = new ConcurrentLongHashMap<>();
        this.requestHandler = requestHandler;
        this.rwLock = new ReentrantReadWriteLock();
        this.closed = false;
    }

    // delete expired cursors, so backlog can be cleared.
    void deleteExpiredCursor(long current, long expirePeriodMillis) {
        lastAccessTimes.forEach((offset, record) -> {
            if (current - record - expirePeriodMillis > 0) {
                deleteOneExpiredCursor(offset);
            }
        });
    }

    void deleteOneExpiredCursor(long offset) {
        Pair<ManagedCursor, Long> pair = consumers.remove(offset);
        lastAccessTimes.remove(offset);
        if (pair != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cursor timed out for offset: {} - {}, cursors cache size: {}",
                    requestHandler.ctx.channel(), offset, MessageIdUtils.getPosition(offset), consumers.size());
            }

            ManagedCursor managedCursor = pair.getKey();
            closeOneCursorAsync(managedCursor, "cursor expired");
        }
    }

    // get one cursor offset pair.
    // remove from cache, so another same offset read could happen.
    // each success remove should have a following add.
    public Pair<ManagedCursor, Long> remove(long offset) {
        // This is for read a new entry, first check if offset is from a batched message request.
        offset = offsetAfterBatchIndex(offset);

        Pair<ManagedCursor, Long> cursor = consumers.remove(offset);
        lastAccessTimes.remove(offset);
        if (cursor != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Get cursor for offset: {} - {} in cache. cache size: {}",
                    requestHandler.ctx.channel(), offset, MessageIdUtils.getPosition(offset), consumers.size());
            }
            return cursor;
        }

        // handle offset not exist in consumers, need create cursor.
        cursor = consumers.computeIfAbsent(
            offset,
            off -> {
                PositionImpl position = MessageIdUtils.getPosition(off);

                String cursorName = "kop-consumer-cursor-" + topic.getName()
                    + "-" + position.getLedgerId() + "-" + position.getEntryId()
                    + "-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);

                // get previous position, because NonDurableCursor is read from next position.
                ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
                PositionImpl previous = ledger.getPreviousPosition(position);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Create cursor {} for offset: {}. position: {}, previousPosition: {}",
                        requestHandler.ctx.channel(), cursorName, off, position, previous);
                }
                ManagedCursor newCursor;
                try {
                    newCursor = ledger.newNonDurableCursor(previous, cursorName);
                } catch (ManagedLedgerException e) {
                    log.error("[{}] Error create cursor for topic {} at offset {} - {}. will cause fetch data error.",
                        requestHandler.ctx.channel(), topic.getName(), off, previous, e);
                    return null;
                }

                lastAccessTimes.put(off, System.currentTimeMillis());
                return Pair.of(newCursor, off);
            });

        return consumers.remove(offset);
    }

    // once entry read complete, add new offset back.
    public void add(long offset, Pair<ManagedCursor, Long> pair) {
        checkArgument(offset == pair.getRight(),
            "offset not equal. key: " + offset + " value: " + pair.getRight());

        rwLock.readLock().lock();
        // should delete the cursor since this tcm already in closing state.
        try {
            if (closed) {
                ManagedCursor managedCursor = pair.getLeft();
                closeOneCursorAsync(managedCursor, "A race - add cursor back but tcm already closed");
                return;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        Pair<ManagedCursor, Long> oldPair = consumers.putIfAbsent(offset, pair);
        if (oldPair != null) {
            closeOneCursorAsync(pair.getLeft(), "reason: A race - same cursor already cached");
        }
        lastAccessTimes.put(offset, System.currentTimeMillis());

        if (log.isDebugEnabled()) {
            log.debug("[{}] requestHandler.ctx.channel(), Add cursor back {} for offset: {} - {}",
                pair.getLeft().getName(), offset, MessageIdUtils.getPosition(offset));
        }
    }

    @Override
    public void close() {
        rwLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Close TCM for topic {}.",
                    requestHandler.ctx.channel(), topic.getName());
            }
        } finally {
            rwLock.writeLock().unlock();
        }

        consumers.values()
            .forEach(pair -> {
                try {
                    ManagedCursor cursor = pair.getLeft();
                    closeOneCursorAsync(cursor, "TopicConsumerManager close");
                } catch (Exception e) {
                    log.error("[{}] Failed to close cursor for topic {}. exception:",
                        requestHandler.ctx.channel(), topic.getName(), e);
                }
            });
        consumers.clear();
        lastAccessTimes.clear();
    }


    // close passed in cursor.
    private void closeOneCursorAsync(ManagedCursor cursor, String reason) {
        if (cursor != null) {
            topic.getManagedLedger().asyncDeleteCursor(cursor.getName(), new DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Cursor {} for topic {} deleted successfully for reason: {}.",
                            requestHandler.ctx.channel(), cursor.getName(), topic.getName(), reason);
                    }
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.warn("[{}] Error deleting cursor for topic {} for reason: {}.",
                        requestHandler.ctx.channel(), cursor.getName(), topic.getName(), reason, exception);
                }
            }, null);
        }
    }
}
