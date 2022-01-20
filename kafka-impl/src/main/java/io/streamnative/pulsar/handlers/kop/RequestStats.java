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

import static io.streamnative.pulsar.handlers.kop.KopServerStats.ACTIVE_CHANNEL_COUNT;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.ALIVE_CHANNEL_COUNT;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.BATCH_COUNT_PER_MEMORYRECORDS;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.CATEGORY_SERVER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.FETCH_DECODE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_PUBLISH;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_QUEUED_LATENCY;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_READ;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PREPARE_METADATA;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PRODUCE_ENCODE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.REQUEST_PARSE_LATENCY;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.REQUEST_QUEUE_SIZE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.RESPONSE_BLOCKED_LATENCY;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.RESPONSE_BLOCKED_TIMES;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.SERVER_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.WAITING_FETCHES_TRIGGERED;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.pulsar.handlers.kop.stats.NullStatsLogger;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.kafka.common.protocol.ApiKeys;

/**
 * Kop request stats metric for prometheus metrics.
 */
@StatsDoc(
    name = SERVER_SCOPE,
    category = CATEGORY_SERVER,
    help = "KOP request stats"
)

@Getter
@Slf4j
public class RequestStats {

    public static final AtomicInteger REQUEST_QUEUE_SIZE_INSTANCE = new AtomicInteger(0);
    public static final AtomicInteger BATCH_COUNT_PER_MEMORY_RECORDS_INSTANCE = new AtomicInteger(0);
    public static final AtomicInteger ALIVE_CHANNEL_COUNT_INSTANCE = new AtomicInteger(0);
    public static final AtomicInteger ACTIVE_CHANNEL_COUNT_INSTANCE = new AtomicInteger(0);

    public static final RequestStats NULL_INSTANCE = new RequestStats(NullStatsLogger.INSTANCE);

    private final StatsLogger statsLogger;

    @StatsDoc(
            name = REQUEST_PARSE_LATENCY,
            help = "parse ByteBuf to request latency"
    )
    private final OpStatsLogger requestParseLatencyStats;

    @StatsDoc(
            name = RESPONSE_BLOCKED_TIMES,
            help = "response blocked times"
    )
    private final Counter responseBlockedTimes;

    @StatsDoc(
            name = RESPONSE_BLOCKED_LATENCY,
            help = "response blocked latency"
    )
    private final OpStatsLogger responseBlockedLatency;

    @StatsDoc(
        name = PRODUCE_ENCODE,
        help = "produce encode stats of Kop"
    )
    private final OpStatsLogger produceEncodeStats;

    @StatsDoc(
        name = MESSAGE_PUBLISH,
        help = "message publish stats from kop to pulsar broker"
    )
    private final OpStatsLogger messagePublishStats;

    @StatsDoc(
        name = MESSAGE_QUEUED_LATENCY,
        help = "message queued stats from kop to pulsar broker"
    )
    private final OpStatsLogger messageQueuedLatencyStats;

    @StatsDoc(
            name = PREPARE_METADATA,
            help = "stats of preparing metadata in fetch request"
    )
    private final OpStatsLogger prepareMetadataStats;

    @StatsDoc(
            name = MESSAGE_READ,
            help = "stats of performing a single cursor's async-read within fetch request"
    )
    private final OpStatsLogger messageReadStats;

    @StatsDoc(
            name = FETCH_DECODE,
            help = "stats of decoding entries in fetch request"
    )
    private final OpStatsLogger fetchDecodeStats;

    @StatsDoc(
            name = WAITING_FETCHES_TRIGGERED,
            help = "number of pending fetches that woke up due to some data produced"
    )
    private final Counter waitingFetchesTriggered;

    private final Map<ApiKeys, StatsLogger> apiKeysToStatsLogger = new ConcurrentHashMap<>();

    public RequestStats(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;

        this.requestParseLatencyStats = statsLogger.getOpStatsLogger(REQUEST_PARSE_LATENCY);

        this.responseBlockedLatency = statsLogger.getOpStatsLogger(RESPONSE_BLOCKED_LATENCY);
        this.responseBlockedTimes = statsLogger.getCounter(RESPONSE_BLOCKED_TIMES);

        this.produceEncodeStats = statsLogger.getOpStatsLogger(PRODUCE_ENCODE);
        this.messagePublishStats = statsLogger.getOpStatsLogger(MESSAGE_PUBLISH);
        this.messageQueuedLatencyStats = statsLogger.getOpStatsLogger(MESSAGE_QUEUED_LATENCY);

        this.prepareMetadataStats = statsLogger.getOpStatsLogger(PREPARE_METADATA);
        this.messageReadStats = statsLogger.getOpStatsLogger(MESSAGE_READ);
        this.fetchDecodeStats  = statsLogger.getOpStatsLogger(FETCH_DECODE);
        this.waitingFetchesTriggered = statsLogger.getCounter(WAITING_FETCHES_TRIGGERED);

        statsLogger.registerGauge(REQUEST_QUEUE_SIZE, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return REQUEST_QUEUE_SIZE_INSTANCE;
            }
        });

        statsLogger.registerGauge(BATCH_COUNT_PER_MEMORYRECORDS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return BATCH_COUNT_PER_MEMORY_RECORDS_INSTANCE;
            }
        });

        statsLogger.registerGauge(ALIVE_CHANNEL_COUNT, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return ALIVE_CHANNEL_COUNT_INSTANCE;
            }
        });

        statsLogger.registerGauge(ACTIVE_CHANNEL_COUNT, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return ACTIVE_CHANNEL_COUNT_INSTANCE;
            }
        });
    }

    /**
     * Get the stats logger for Kafka requests.
     *
     * @param apiKey the {@link ApiKeys} object that represents the Kafka request's type
     * @param statsName the stats name
     * @return
     */
    public OpStatsLogger getRequestStatsLogger(final ApiKeys apiKey, final String statsName) {
        return apiKeysToStatsLogger.computeIfAbsent(apiKey,
                __ -> statsLogger.scopeLabel(KopServerStats.REQUEST_SCOPE, apiKey.name)
        ).getOpStatsLogger(statsName);
    }

    @VisibleForTesting
    public Set<ApiKeys> getApiKeysSet() {
        return new TreeSet<>(apiKeysToStatsLogger.keySet());
    }
}
