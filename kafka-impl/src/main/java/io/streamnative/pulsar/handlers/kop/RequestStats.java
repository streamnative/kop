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
import static io.streamnative.pulsar.handlers.kop.KopServerStats.HANDLE_FETCH_REQUEST;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.HANDLE_PRODUCE_REQUEST;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_PUBLISH;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_QUEUED_LATENCY;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_READ;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PREPARE_METADATA;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PRODUCE_ENCODE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.REQUEST_PARSE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.REQUEST_QUEUED_LATENCY;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.REQUEST_QUEUE_SIZE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.SERVER_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.TOTAL_MESSAGE_READ;

import lombok.Getter;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
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
    @StatsDoc(
            name = REQUEST_QUEUE_SIZE,
            help = "request queue's size"
    )
    private final Counter requestQueueSize;

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
        name = HANDLE_PRODUCE_REQUEST,
        help = "handle produce request stats of Kop"
    )
    private final OpStatsLogger handleProduceRequestStats;

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
            name = HANDLE_FETCH_REQUEST,
            help = "stats of fetch request"
    )
    private final OpStatsLogger handleFetchRequestStats;

    @StatsDoc(
            name = PREPARE_METADATA,
            help = "stats of preparing metadata in fetch request"
    )
    private final OpStatsLogger prepareMetadataStats;

    @StatsDoc(
            name = TOTAL_MESSAGE_READ,
            help = "stats of reading total entries in a single fetch request"
    )
    private final OpStatsLogger totalMessageReadStats;

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
        this.requestQueueSize = statsLogger.getCounter(REQUEST_QUEUE_SIZE);
        this.requestQueuedLatencyStats = statsLogger.getOpStatsLogger(REQUEST_QUEUED_LATENCY);
        this.requestParseStats = statsLogger.getOpStatsLogger(REQUEST_PARSE);

        this.handleProduceRequestStats = statsLogger.getOpStatsLogger(HANDLE_PRODUCE_REQUEST);
        this.produceEncodeStats = statsLogger.getOpStatsLogger(PRODUCE_ENCODE);
        this.messagePublishStats = statsLogger.getOpStatsLogger(MESSAGE_PUBLISH);
        this.messageQueuedLatencyStats = statsLogger.getOpStatsLogger(MESSAGE_QUEUED_LATENCY);

        this.handleFetchRequestStats = statsLogger.getOpStatsLogger(HANDLE_FETCH_REQUEST);
        this.prepareMetadataStats = statsLogger.getOpStatsLogger(PREPARE_METADATA);
        this.totalMessageReadStats = statsLogger.getOpStatsLogger(TOTAL_MESSAGE_READ);
        this.messageReadStats = statsLogger.getOpStatsLogger(MESSAGE_READ);
        this.fetchDecodeStats  = statsLogger.getOpStatsLogger(FETCH_DECODE);
    }
}
