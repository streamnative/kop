package io.streamnative.pulsar.handlers.kop.coordinator.group;

import lombok.Getter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

import javax.print.attribute.standard.MediaSize;

import static io.streamnative.pulsar.handlers.kop.KopServerStats.CATEGORY_SERVER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.COORDINATOR_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.FIND_POSITION;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.GET_OR_CREATE_CONSUMER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.GET_TOPIC;

/**
 * Kop coordinator stats metric for prometheus metrics.
 */
@StatsDoc(
        name = COORDINATOR_SCOPE,
        category = CATEGORY_SERVER,
        help = "KOP Coordinator stats"
)
@Getter
public class CoordinatorStats {
    private final StatsLogger statsLogger;

    @StatsDoc(
            name = GET_OR_CREATE_CONSUMER,
            category = CATEGORY_SERVER,
            help = "Kop offset acker getOrCreateConsumer stats"
    )
    private final OpStatsLogger getOrCreateConsumerStats;

    @StatsDoc(
            name = GET_TOPIC,
            category = CATEGORY_SERVER,
            help = "Kop offset acker get topic stats"
    )
    private final OpStatsLogger getTopicStats;

    @StatsDoc(
            name = FIND_POSITION,
            category = CATEGORY_SERVER,
            help = "Kop offset acker find position stats"
    )
    private final OpStatsLogger findPositionStats;

    public CoordinatorStats(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;

        this.getOrCreateConsumerStats = statsLogger.getOpStatsLogger(GET_OR_CREATE_CONSUMER);
        this.getTopicStats = statsLogger.getOpStatsLogger(GET_TOPIC);
        this.findPositionStats = statsLogger.getOpStatsLogger(FIND_POSITION);
    }
}
