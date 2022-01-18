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
package io.streamnative.pulsar.handlers.kop.format;

import static io.streamnative.pulsar.handlers.kop.KopServerStats.BYTES_OUT;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.CONSUME_MESSAGE_CONVERSIONS;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.CONSUME_MESSAGE_CONVERSIONS_TIME_NANOS;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.ENTRIES_OUT;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.GROUP_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_OUT;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PARTITION_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.TOPIC_SCOPE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;

/**
 * Result of decode in entry formatter.
 */
public class DecodeResult {

    @Getter
    private MemoryRecords records;
    private ByteBuf releasedByteBuf;
    @Getter
    private int conversionCount;
    @Getter
    private long conversionTimeNanos;

    private final Recycler.Handle<DecodeResult> recyclerHandle;

    public static DecodeResult get(MemoryRecords records) {
        return get(records, null, 0, 0L);
    }

    public static DecodeResult get(MemoryRecords records,
                                   ByteBuf releasedByteBuf,
                                   int conversionCount,
                                   long conversionTimeNanos) {
        DecodeResult decodeResult = RECYCLER.get();
        decodeResult.records = records;
        decodeResult.releasedByteBuf = releasedByteBuf;
        decodeResult.conversionCount = conversionCount;
        decodeResult.conversionTimeNanos = conversionTimeNanos;
        return decodeResult;
    }

    private DecodeResult(Recycler.Handle<DecodeResult> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<DecodeResult> RECYCLER = new Recycler<DecodeResult>() {
        @Override
        protected DecodeResult newObject(Recycler.Handle<DecodeResult> handle) {
            return new DecodeResult(handle);
        }
    };

    public void recycle() {
        records = null;
        if (releasedByteBuf != null) {
            releasedByteBuf.release();
            releasedByteBuf = null;
        }
        conversionCount = -1;
        conversionTimeNanos = -1L;
        recyclerHandle.recycle(this);
    }

    public @NonNull ByteBuf getOrCreateByteBuf() {
        if (releasedByteBuf != null) {
            return releasedByteBuf;
        } else {
            return Unpooled.wrappedBuffer(records.buffer());
        }
    }

    public void updateConsumerStats(final TopicPartition topicPartition,
                                    int entrySize,
                                    final String groupId,
                                    RequestStats statsLogger) {
        final int numMessages = EntryFormatter.parseNumMessages(records);

        final StatsLogger statsLoggerForThisPartition = statsLogger.getStatsLogger()
                .scopeLabel(TOPIC_SCOPE, topicPartition.topic())
                .scopeLabel(PARTITION_SCOPE, String.valueOf(topicPartition.partition()));

        statsLoggerForThisPartition.getCounter(CONSUME_MESSAGE_CONVERSIONS).add(conversionCount);
        statsLoggerForThisPartition.getOpStatsLogger(CONSUME_MESSAGE_CONVERSIONS_TIME_NANOS)
                .registerSuccessfulEvent(conversionTimeNanos, TimeUnit.NANOSECONDS);

        final StatsLogger statsLoggerForThisGroup = statsLoggerForThisPartition.scopeLabel(GROUP_SCOPE, groupId);

        statsLoggerForThisGroup.getCounter(BYTES_OUT).add(records.sizeInBytes());
        statsLoggerForThisGroup.getCounter(MESSAGE_OUT).add(numMessages);
        statsLoggerForThisGroup.getCounter(ENTRIES_OUT).add(entrySize);

    }

}
