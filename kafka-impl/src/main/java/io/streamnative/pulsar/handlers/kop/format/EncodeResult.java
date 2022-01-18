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

import static io.streamnative.pulsar.handlers.kop.KopServerStats.BYTES_IN;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_IN;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PARTITION_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PRODUCE_MESSAGE_CONVERSIONS;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PRODUCE_MESSAGE_CONVERSIONS_TIME_NANOS;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.TOPIC_SCOPE;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.streamnative.pulsar.handlers.kop.KafkaTopicManager;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.pulsar.broker.service.Producer;

/**
 * Result of encode in entry formatter.
 */
@Getter
public class EncodeResult {

    private MemoryRecords records;
    private ByteBuf encodedByteBuf;
    private int numMessages;
    private int conversionCount;
    private long conversionTimeNanos;

    private final Recycler.Handle<EncodeResult> recyclerHandle;

    public static EncodeResult get(MemoryRecords records,
                                   ByteBuf encodedByteBuf,
                                   int numMessages,
                                   int conversionCount,
                                   long conversionTimeNanos) {
        EncodeResult encodeResult = RECYCLER.get();
        encodeResult.records = records;
        encodeResult.encodedByteBuf = encodedByteBuf;
        encodeResult.numMessages = numMessages;
        encodeResult.conversionCount = conversionCount;
        encodeResult.conversionTimeNanos = conversionTimeNanos;
        return encodeResult;
    }

    private EncodeResult(Recycler.Handle<EncodeResult> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<EncodeResult> RECYCLER = new Recycler<EncodeResult>() {
        @Override
        protected EncodeResult newObject(Recycler.Handle<EncodeResult> handle) {
            return new EncodeResult(handle);
        }
    };

    public void recycle() {
        records = null;
        if (encodedByteBuf != null) {
            encodedByteBuf.release();
            encodedByteBuf = null;
        }
        numMessages = -1;
        conversionCount = -1;
        conversionTimeNanos = -1L;
        recyclerHandle.recycle(this);
    }

    public void updateProducerStats(final TopicPartition topicPartition,
                                    final RequestStats requestStats,
                                    final String namespacePrefix) {
        final int numBytes = encodedByteBuf.readableBytes();

        final Producer producer = KafkaTopicManager
                .getReferenceProducer(KopTopic.toString(topicPartition, namespacePrefix));
        producer.updateRates(numMessages, numBytes);
        producer.getTopic().incrementPublishCount(numMessages, numBytes);

        final StatsLogger statsLoggerForThisPartition = requestStats.getStatsLogger()
                .scopeLabel(TOPIC_SCOPE, topicPartition.topic())
                .scopeLabel(PARTITION_SCOPE, String.valueOf(topicPartition.partition()));

        statsLoggerForThisPartition.getCounter(BYTES_IN).add(numBytes);
        statsLoggerForThisPartition.getCounter(MESSAGE_IN).add(numMessages);
        statsLoggerForThisPartition.getCounter(PRODUCE_MESSAGE_CONVERSIONS).add(conversionCount);
        statsLoggerForThisPartition.getOpStatsLogger(PRODUCE_MESSAGE_CONVERSIONS_TIME_NANOS)
                .registerSuccessfulEvent(conversionTimeNanos, TimeUnit.NANOSECONDS);

        RequestStats.BATCH_COUNT_PER_MEMORY_RECORDS_INSTANCE.set(numMessages);
    }

}
