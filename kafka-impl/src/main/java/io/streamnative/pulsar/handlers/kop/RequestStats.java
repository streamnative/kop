package io.streamnative.pulsar.handlers.kop;

import lombok.Getter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

import static io.streamnative.pulsar.handlers.kop.KopServerStats.CATEGORY_SERVER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.HANDLE_PRODUCE_REQUEST;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_PUBLISH;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_QUEUED_LATENCY;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PRODUCE_ENCODE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.SERVER_SCOPE;

@StatsDoc(
    name = SERVER_SCOPE,
    category = CATEGORY_SERVER,
    help = "KOP request stats"
)

@Getter
public class RequestStats {
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

    public RequestStats(StatsLogger statsLogger) {
        this.handleProduceRequestStats = statsLogger.getOpStatsLogger(HANDLE_PRODUCE_REQUEST);
        this.produceEncodeStats = statsLogger.getOpStatsLogger(PRODUCE_ENCODE);
        this.messagePublishStats = statsLogger.getOpStatsLogger(MESSAGE_PUBLISH);
        this.messageQueuedLatencyStats = statsLogger.getOpStatsLogger(MESSAGE_QUEUED_LATENCY);
    }
}
