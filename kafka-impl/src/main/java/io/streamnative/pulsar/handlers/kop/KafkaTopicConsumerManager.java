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

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.pulsar.handlers.kop.utils.MessageMetadataUtils;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * KafkaTopicConsumerManager manages a topic and its related offset cursor.
 * Each cursor is trying to track the read from a consumer client.
 */
@Slf4j
public class KafkaTopicConsumerManager implements Closeable {

    private final PersistentTopic topic;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    // key is the offset, value is the future of (cursor, offset), whose offset is the last offset in pair.
    @Getter
    private final Map<Long, CompletableFuture<Pair<ManagedCursor, Long>>> cursors;

    // used to track all created cursor, since above consumers may be remove and in fly,
    // use this map will not leak cursor when close.
    @Getter
    private final Map<String, ManagedCursor> createdCursors;

    // track last access time(millis) for offsets <offset, time>
    @Getter
    private final Map<Long, Long> lastAccessTimes;

    private final boolean skipMessagesWithoutIndex;

    private final String description;

    KafkaTopicConsumerManager(KafkaRequestHandler requestHandler, PersistentTopic topic) {
        this(requestHandler.ctx.channel() + "",
                requestHandler.isSkipMessagesWithoutIndex(),
                topic);
    }

    public KafkaTopicConsumerManager(String description, boolean skipMessagesWithoutIndex, PersistentTopic topic) {
        this.topic = topic;
        this.cursors = new ConcurrentHashMap<>();
        this.createdCursors = new ConcurrentHashMap<>();
        this.lastAccessTimes = new ConcurrentHashMap<>();
        this.description =  description;
        this.skipMessagesWithoutIndex = skipMessagesWithoutIndex;
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
        if (closed.get()) {
            return;
        }

        final CompletableFuture<Pair<ManagedCursor, Long>> cursorFuture = cursors.remove(offset);
        lastAccessTimes.remove(offset);

        if (cursorFuture != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cursor timed out for offset: {}, cursors cache size: {}",
                        description, offset, cursors.size());
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
    public void deleteOneCursorAsync(ManagedCursor cursor, String reason) {
        if (cursor != null) {
            topic.getManagedLedger().asyncDeleteCursor(cursor.getName(), new DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Cursor {} for topic {} deleted successfully for reason: {}.",
                                description, cursor.getName(), topic.getName(), reason);
                    }
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    if (exception instanceof ManagedLedgerException.CursorNotFoundException) {
                        log.debug("[{}] Cursor already deleted {} for topic {} for reason: {} - {}.",
                                description, cursor.getName(), topic.getName(), reason, exception.toString());
                    } else {
                        log.warn("[{}] Error deleting cursor {} for topic {} for reason: {}.",
                                description, cursor.getName(), topic.getName(), reason, exception);
                    }
                }
            }, null);
            createdCursors.remove(cursor.getName());
        }
    }

    // get one cursor offset pair.
    // remove from cache, so another same offset read could happen.
    // each success remove should have a following add.
    public CompletableFuture<Pair<ManagedCursor, Long>> removeCursorFuture(long offset) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new Exception("Current managedLedger for "
                    + topic.getName() + " has been closed."));
        }

        lastAccessTimes.remove(offset);
        final CompletableFuture<Pair<ManagedCursor, Long>> cursorFuture = cursors.remove(offset);
        if (cursorFuture == null) {
            return asyncCreateCursorIfNotExists(offset);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Get cursor for offset: {} in cache. cache size: {}",
                    description, offset, cursors.size());
        }
        return cursorFuture;
    }

    private CompletableFuture<Pair<ManagedCursor, Long>> asyncCreateCursorIfNotExists(long offset) {
        if (closed.get()) {
            return null;
        }
        cursors.putIfAbsent(offset, asyncGetCursorByOffset(offset));

        // notice:  above would add a <offset, null-Pair>
        lastAccessTimes.remove(offset);
        return cursors.remove(offset);
    }

    public void add(long offset, Pair<ManagedCursor, Long> pair) {
        checkArgument(offset == pair.getRight(),
                "offset not equal. key: " + offset + " value: " + pair.getRight());

        if (closed.get()) {
            ManagedCursor managedCursor = pair.getLeft();
            deleteOneCursorAsync(managedCursor, "A race - add cursor back but tcm already closed");
            return;
        }

        final CompletableFuture<Pair<ManagedCursor, Long>> cursorFuture = CompletableFuture.completedFuture(pair);
        if (cursors.putIfAbsent(offset, cursorFuture) != null) {
            deleteOneCursorAsync(pair.getLeft(), "reason: A race - same cursor already cached");
        }
        lastAccessTimes.put(offset, System.currentTimeMillis());

        if (log.isDebugEnabled()) {
            log.debug("[{}] Add cursor back {} for offset: {}",
                    description, pair.getLeft().getName(), offset);
        }
    }

    // called when channel closed.
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] Close TCM for topic {}.",
                    description, topic.getName());
        }
        final List<CompletableFuture<Pair<ManagedCursor, Long>>> cursorFuturesToClose = new ArrayList<>();
        cursors.forEach((ignored, cursorFuture) -> cursorFuturesToClose.add(cursorFuture));
        cursors.clear();
        lastAccessTimes.clear();
        createdCursors.clear();

        cursorFuturesToClose.forEach(cursorFuture -> {
            cursorFuture.whenComplete((pair, e) -> {
                if (e != null || pair == null) {
                    return;
                }
                ManagedCursor cursor = pair.getLeft();
                deleteOneCursorAsync(cursor, "TopicConsumerManager close");
            });
        });
        cursorFuturesToClose.clear();
    }

    private CompletableFuture<Pair<ManagedCursor, Long>> asyncGetCursorByOffset(long offset) {
        if (closed.get()) {
            // return a null completed future instead of null because the returned value will be put into a Map
            return CompletableFuture.completedFuture(null);
        }
        final ManagedLedger ledger = topic.getManagedLedger();

        if (((ManagedLedgerImpl) ledger).getState() == ManagedLedgerImpl.State.Closed) {
            log.error("[{}] Async get cursor for offset {} for topic {} failed, "
                            + "because current managedLedger has been closed",
                    description, offset, topic.getName());
            CompletableFuture<Pair<ManagedCursor, Long>> future = new CompletableFuture<>();
            future.completeExceptionally(new Exception("Current managedLedger for "
                    + topic.getName() + " has been closed."));
            return future;
        }

        return MessageMetadataUtils.asyncFindPosition(ledger, offset, skipMessagesWithoutIndex).thenApply(position -> {
            if (position == null) {
                return null;
            }
            final String cursorName = "kop-consumer-cursor-" + topic.getName()
                    + "-" + position.getLedgerId() + "-" + position.getEntryId()
                    + "-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);

            // get previous position, because NonDurableCursor is read from next position.
            final PositionImpl previous = ((ManagedLedgerImpl) ledger).getPreviousPosition((PositionImpl) position);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Create cursor {} for offset: {}. position: {}, previousPosition: {}",
                        description, cursorName, offset, position, previous);
            }
            try {
                final ManagedCursor newCursor = ledger.newNonDurableCursor(previous, cursorName);
                createdCursors.putIfAbsent(newCursor.getName(), newCursor);
                lastAccessTimes.put(offset, System.currentTimeMillis());
                return Pair.of(newCursor, offset);
            } catch (ManagedLedgerException e) {
                log.error("[{}] Error new cursor for topic {} at offset {} - {}. will cause fetch data error.",
                        description, topic.getName(), offset, previous, e);
                return null;
            }
        });
    }

    public ManagedLedger getManagedLedger() {
        return topic.getManagedLedger();
    }

    @VisibleForTesting
    public int getNumCreatedCursors() {
        int numCreatedCursors = 0;
        for (ManagedCursor ignored : topic.getManagedLedger().getCursors()) {
            numCreatedCursors++;
        }
        return numCreatedCursors;
    }

    @VisibleForTesting
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Returns the position in the ManagedLedger for the given offset.
     * @param offset
     * @return null if not found, PositionImpl.latest if the offset matches the end of the topic
     */
    public CompletableFuture<Position> findPositionForIndex(Long offset) {
        if (closed.get()) {
            return FutureUtil.failedFuture(
                    new IllegalStateException("This manager for " + topic.getName() + " is closed"));
        }
        final ManagedLedger ledger = topic.getManagedLedger();

        return MessageMetadataUtils.asyncFindPosition(ledger, offset, skipMessagesWithoutIndex).thenApply(position -> {
            PositionImpl lastConfirmedEntry = (PositionImpl) ledger.getLastConfirmedEntry();
            log.info("Found position {} for offset {}, lastConfirmedEntry {}", position, offset, lastConfirmedEntry);
            if (position == null) {
                return null;
            }
            if (lastConfirmedEntry != null
                    && Objects.equals(lastConfirmedEntry.getNext(), position)) {
                log.debug("Found position {} for offset {}, LAC {} -> RETURN LATEST",
                        position, offset, lastConfirmedEntry);
                return PositionImpl.LATEST;
            } else {
                return position;
            }
        });
    }
}
