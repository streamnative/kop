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

import static io.streamnative.pulsar.handlers.kop.KopServerStats.CATEGORY_SERVER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.FETCH_DECODE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_PUBLISH;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_QUEUED_LATENCY;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_READ;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PREPARE_METADATA;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PRODUCE_ENCODE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.REQUEST_PARSE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.REQUEST_QUEUED_LATENCY;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.REQUEST_QUEUE_SIZE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.RESPONSE_BLOCKED_LATENCY;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.RESPONSE_BLOCKED_TIMES;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.RESPONSE_QUEUE_SIZE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.SERVER_SCOPE;

import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

/**
 * Kop request stats metric for prometheus metrics.
 */
@StatsDoc(
    name = SERVER_SCOPE,
    category = CATEGORY_SERVER,
    help = "KOP request stats"
)

@Getter
public class RequestStats {

    private final AtomicInteger requestQueueSize = new AtomicInteger(0);
    private final AtomicInteger responseQueueSize = new AtomicInteger(0);

    private final StatsLogger statsLogger;

    @StatsDoc(
            name = REQUEST_QUEUED_LATENCY,
            help = "latency from request enqueued to dequeued"
    )
    private final OpStatsLogger requestQueuedLatencyStats;

    @StatsDoc(
            name = REQUEST_PARSE,
            help = "parse request to ByteBuf"
    )
    private final OpStatsLogger requestParseStats;

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

    public RequestStats(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;

        this.requestQueuedLatencyStats = statsLogger.getOpStatsLogger(REQUEST_QUEUED_LATENCY);
        this.requestParseStats = statsLogger.getOpStatsLogger(REQUEST_PARSE);

        this.responseBlockedLatency = statsLogger.getOpStatsLogger(RESPONSE_BLOCKED_LATENCY);
        this.responseBlockedTimes = statsLogger.getCounter(RESPONSE_BLOCKED_TIMES);

        this.produceEncodeStats = statsLogger.getOpStatsLogger(PRODUCE_ENCODE);
        this.messagePublishStats = statsLogger.getOpStatsLogger(MESSAGE_PUBLISH);
        this.messageQueuedLatencyStats = statsLogger.getOpStatsLogger(MESSAGE_QUEUED_LATENCY);

        this.prepareMetadataStats = statsLogger.getOpStatsLogger(PREPARE_METADATA);
        this.messageReadStats = statsLogger.getOpStatsLogger(MESSAGE_READ);
        this.fetchDecodeStats  = statsLogger.getOpStatsLogger(FETCH_DECODE);


        statsLogger.registerGauge(RESPONSE_QUEUE_SIZE, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return responseQueueSize;
            }
        });

        statsLogger.registerGauge(REQUEST_QUEUE_SIZE, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return requestQueueSize;
            }
        });
    }
}
