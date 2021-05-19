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
package io.streamnative.pulsar.handlers.kop.coordinator.group;

import static io.streamnative.pulsar.handlers.kop.KopServerStats.CATEGORY_SERVER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.COORDINATOR_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.FIND_POSITION;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.GET_OR_CREATE_CONSUMER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.GET_TOPIC;

import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import lombok.Getter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;

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
