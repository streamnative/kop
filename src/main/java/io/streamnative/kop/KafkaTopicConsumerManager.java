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
package io.streamnative.kop;

import static com.google.common.base.Preconditions.checkArgument;

import io.streamnative.kop.utils.MessageIdUtils;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImplWrapper;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * KafkaTopicConsumerManager manages a topic and its related offset cursor.
 * Each cursor is trying to track the read from a consumer client.
 */
@Slf4j
public class KafkaTopicConsumerManager {

    private final PersistentTopic topic;

    // keep fetch offset and related cursor. keep cursor and its last offset in Pair
    @Getter
    private final ConcurrentLongHashMap<CompletableFuture<Pair<ManagedCursor, Long>>> consumers;

    KafkaTopicConsumerManager(PersistentTopic topic) {
        this.topic = topic;
        this.consumers = new ConcurrentLongHashMap<>();
    }

    public CompletableFuture<Pair<ManagedCursor, Long>> remove(long offset) {
        CompletableFuture<Pair<ManagedCursor, Long>> cursor = consumers.remove(offset);
        if (cursor != null) {
            if (log.isDebugEnabled()) {
                log.debug("Get cursor for offset: {} in cache", offset);
            }
            return cursor;
        }

        // handle null remove.
        cursor = new CompletableFuture<>();
        CompletableFuture<Pair<ManagedCursor, Long>> oldCursor = consumers.putIfAbsent(offset, cursor);
        if (oldCursor != null) {
            // added by other thread while creating.
            return remove(offset);
        }

        String cursorName = "kop-consumer-cursor-" + topic.getName() + "-" + offset + "-"
            + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);

        PositionImpl position = MessageIdUtils.getPosition(offset);

        try {
            // get previous position, because NonDurableCursor is read from next position.
            ManagedLedgerImplWrapper ledger =
                new ManagedLedgerImplWrapper((ManagedLedgerImpl) topic.getManagedLedger());
            PositionImpl previous = ledger.getPreviousPosition(position);
            if (log.isDebugEnabled()) {
                log.debug("Create cursor {} for offset: {}. position: {}, previousPosition: {}",
                    cursorName, offset, position, previous);
            }

            cursor.complete(Pair
                .of(topic.getManagedLedger().newNonDurableCursor(previous, cursorName),
                    offset));
        } catch (Exception e) {
            log.error("Failed create nonDurable cursor for topic {} position: {}.", topic, position, e);
            cursor.completeExceptionally(e);
        }

        return remove(offset);
    }

    // once entry read complete, add new offset back.
    public void add(long offset, Pair<ManagedCursor, Long> cursor) {
        checkArgument(offset == cursor.getRight(),
            "offset not equal. key: " + offset + " value: " + cursor.getRight());

        CompletableFuture<Pair<ManagedCursor, Long>> cursorOffsetPair = new CompletableFuture<>();

        cursorOffsetPair.complete(cursor);
        consumers.putIfAbsent(offset, cursorOffsetPair);

        if (log.isDebugEnabled()) {
            log.debug("Add cursor back {} for offset: {}", cursor.getLeft().getName(), offset);
        }
    }

}
