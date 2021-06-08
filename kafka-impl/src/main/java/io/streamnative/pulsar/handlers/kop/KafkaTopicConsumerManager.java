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

import io.streamnative.pulsar.handlers.kop.utils.OffsetSearchPredicate;
import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
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
    // once closed, should not add new cursor back, since consumers are cleared.
    private final ReentrantReadWriteLock rwLock;
    private boolean closed;

    // key is the offset, value is the future of (cursor, offset), whose offset is the last offset in pair.
    @Getter
    private final ConcurrentLongHashMap<CompletableFuture<Pair<ManagedCursor, Long>>> cursors;

    // used to track all created cursor, since above consumers may be remove and in fly,
    // use this map will not leak cursor when close.
    private final ConcurrentMap<String, ManagedCursor> createdCursors;

    // track last access time(millis) for offsets <offset, time>
    @Getter
    private final ConcurrentLongHashMap<Long> lastAccessTimes;

    KafkaTopicConsumerManager(KafkaRequestHandler requestHandler, PersistentTopic topic) {
        this.topic = topic;
        this.cursors = new ConcurrentLongHashMap<>();
        this.createdCursors = new ConcurrentHashMap<>();
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
        CompletableFuture<Pair<ManagedCursor, Long>> cursorFuture;

        // need not do anything, since this tcm already in closing state. and close() will delete every thing.
        rwLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            cursorFuture = cursors.remove(offset);
            lastAccessTimes.remove(offset);
        } finally {
            rwLock.readLock().unlock();
        }

        if (cursorFuture != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cursor timed out for offset: {}, cursors cache size: {}",
                        requestHandler.ctx.channel(), offset, cursors.size());
            }

            // TODO: Should we just cancel this future?
            cursorFuture.whenComplete((pair, e) -> {
                if (e != null || pair == null) {
                    return;
                }
                ManagedCursor managedCursor = pair.getKey();
                deleteOneCursorAsync(managedCursor, "cursor expired");
            });
        }
    }

    // delete passed in cursor.
    void deleteOneCursorAsync(ManagedCursor cursor, String reason) {
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
                    log.warn("[{}] Error deleting cursor {} for topic {} for reason: {}.",
                        requestHandler.ctx.channel(), cursor.getName(), topic.getName(), reason, exception);
                }
            }, null);
            createdCursors.remove(cursor.getName());
        }
    }

    // get one cursor offset pair.
    // remove from cache, so another same offset read could happen.
    // each success remove should have a following add.
    public CompletableFuture<Pair<ManagedCursor, Long>> removeCursorFuture(long offset) {
        rwLock.readLock().lock();
        try {
            if (closed) {
                return null;
            }
            lastAccessTimes.remove(offset);
            final CompletableFuture<Pair<ManagedCursor, Long>> cursorFuture = cursors.remove(offset);
            if (cursorFuture == null) {
                return asyncCreateCursorIfNotExists(offset);
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Get cursor for offset: {} in cache. cache size: {}",
                        requestHandler.ctx.channel(), offset, cursors.size());
            }
            return cursorFuture;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private CompletableFuture<Pair<ManagedCursor, Long>> asyncCreateCursorIfNotExists(long offset) {
        rwLock.readLock().lock();
        try {
            if (closed) {
                return null;
            }
            cursors.putIfAbsent(offset, asyncGetCursorByOffset(offset));

            // notice:  above would add a <offset, null-Pair>
            lastAccessTimes.remove(offset);
            return cursors.remove(offset);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void add(long offset, Pair<ManagedCursor, Long> pair) {
        checkArgument(offset == pair.getRight(),
                "offset not equal. key: " + offset + " value: " + pair.getRight());

        rwLock.readLock().lock();
        try {
            if (closed) {
                ManagedCursor managedCursor = pair.getLeft();
                deleteOneCursorAsync(managedCursor, "A race - add cursor back but tcm already closed");
                return;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        final CompletableFuture<Pair<ManagedCursor, Long>> cursorFuture = CompletableFuture.completedFuture(pair);
        if (cursors.putIfAbsent(offset, cursorFuture) != null) {
            deleteOneCursorAsync(pair.getLeft(), "reason: A race - same cursor already cached");
        }
        lastAccessTimes.put(offset, System.currentTimeMillis());

        if (log.isDebugEnabled()) {
            log.debug("[{}] Add cursor back {} for offset: {}",
                    requestHandler.ctx.channel(), pair.getLeft().getName(), offset);
        }
    }

    // called when channel closed.
    @Override
    public void close() {
        final ConcurrentLongHashMap<CompletableFuture<Pair<ManagedCursor, Long>>> cursorFuturesToClose =
                new ConcurrentLongHashMap<>();
        ConcurrentMap<String, ManagedCursor> cursorsToClose;
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
            cursors.forEach(cursorFuturesToClose::put);
            cursors.clear();
            lastAccessTimes.clear();
            cursorsToClose = new ConcurrentHashMap<>();
            createdCursors.forEach(cursorsToClose::put);
            createdCursors.clear();
        } finally {
            rwLock.writeLock().unlock();
        }

        cursorFuturesToClose.values().forEach(cursorFuture -> {
            cursorFuture.whenComplete((pair, e) -> {
                if (e != null || pair == null) {
                    return;
                }
                ManagedCursor cursor = pair.getLeft();
                deleteOneCursorAsync(cursor, "TopicConsumerManager close");
                if (cursor != null) {
                    cursorsToClose.remove(cursor.getName());
                }
            });
        });

        // delete dangling createdCursors
        cursorsToClose.values().forEach(cursor ->
            deleteOneCursorAsync(cursor, "TopicConsumerManager close but cursor is still outstanding"));
        cursorsToClose.clear();
    }

    private CompletableFuture<Pair<ManagedCursor, Long>> asyncGetCursorByOffset(long offset) {
        final ManagedLedger ledger = topic.getManagedLedger();
        return ledger.asyncFindPosition(new OffsetSearchPredicate(offset)).thenApply(position -> {
            final String cursorName = "kop-consumer-cursor-" + topic.getName()
                    + "-" + position.getLedgerId() + "-" + position.getEntryId()
                    + "-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);

            // get previous position, because NonDurableCursor is read from next position.
            final PositionImpl previous = ((ManagedLedgerImpl) ledger).getPreviousPosition((PositionImpl) position);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Create cursor {} for offset: {}. position: {}, previousPosition: {}",
                        requestHandler.ctx.channel(), cursorName, offset, position, previous);
            }
            try {
                final ManagedCursor newCursor = ledger.newNonDurableCursor(previous, cursorName);
                createdCursors.putIfAbsent(newCursor.getName(), newCursor);
                lastAccessTimes.put(offset, System.currentTimeMillis());
                return Pair.of(newCursor, offset);
            } catch (ManagedLedgerException e) {
                log.error("[{}] Error new cursor for topic {} at offset {} - {}. will cause fetch data error.",
                        requestHandler.ctx.channel(), topic.getName(), offset, previous, e);
                return null;
            }
        });
    }
}
