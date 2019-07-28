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

import io.streamnative.kop.utils.MessageIdUtils;
import io.streamnative.kop.utils.ReflectionUtils;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * KafkaTopicConsumerManager manages a topic and its related offset cursor.
 * Each cursor is trying to track the read from a consumer client.
 */
@Slf4j
public class KafkaTopicConsumerManager {

    private final PersistentTopic topic;
    private final ConcurrentLongHashMap<CompletableFuture<ManagedCursor>> consumers;

    KafkaTopicConsumerManager(PersistentTopic topic) {
        this.topic = topic;
        this.consumers = new ConcurrentLongHashMap<>();
    }

    public CompletableFuture<ManagedCursor> remove(long offset) {
        CompletableFuture<ManagedCursor> cursor = consumers.remove(offset);
        if (cursor != null) {
            if (log.isDebugEnabled()) {
                log.debug("Get cursor for offset: {} in cache", offset);
            }
            return cursor;
        }

        // handle null remove.
        cursor = new CompletableFuture<>();
        CompletableFuture<ManagedCursor> oldCursor = consumers.putIfAbsent(offset, cursor);
        if (oldCursor != null) {
            return remove(offset);
        }

        String cursorName = "kop-consumer-cursor-" + topic.getName() + "-" + offset + "-"
            + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);

        PositionImpl position = MessageIdUtils.getPosition(offset);

        try {
            // get previous position, because NonDurableCursor is read from next position.
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
            Method getPreviousPosition = ReflectionUtils
                .setMethodAccessible(ledger, "getPreviousPosition", PositionImpl.class);
            PositionImpl previous = (PositionImpl) getPreviousPosition.invoke(ledger, position);

            if (log.isDebugEnabled()) {
                log.debug("Create cursor {} for offset: {}. position: {}, previousPosition: {}",
                    cursorName, offset, position, previous);
            }

            cursor.complete(topic.getManagedLedger().newNonDurableCursor(previous, cursorName));
        } catch (Exception e) {
            log.error("Failed create nonDurable cursor for topic {} position: {}.", topic, position, e);
            cursor.completeExceptionally(e);
        }

        return remove(offset);
    }


    // once entry read complete, add new offset back.
    public void add(long offset, CompletableFuture<ManagedCursor> cursor) {
        CompletableFuture<ManagedCursor> oldCursor = consumers.putIfAbsent(offset, cursor);

        if (log.isDebugEnabled()) {
            log.debug("Add cursor {} for offset: {}. oldCursor: {}", cursor.join().getName(), offset, oldCursor);
        }
    }

}
