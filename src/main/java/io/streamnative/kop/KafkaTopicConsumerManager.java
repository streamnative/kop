package io.streamnative.kop;

import io.streamnative.kop.utils.MessageIdUtils;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * KafkaTopicConsumerManager manages a topic and its related offset cursor.
 *
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

        if (log.isDebugEnabled()) {
            log.debug("Create cursor {} for offset: {}.", cursorName, offset);
        }

        try {
            cursor.complete(topic.getManagedLedger().newNonDurableCursor(position, cursorName));
        } catch (ManagedLedgerException e) {
            log.error("Created nonDurable cursor for topic {} position: {}", topic, position);
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
